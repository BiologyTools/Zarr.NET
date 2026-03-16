using System.Collections.Concurrent;
using System.Text.Json;
using ZarrNET.Core;
using ZarrNET;
using ZarrNET.Core;
using ZarrNET.Core.Zarr;
using ZarrNET.Core.Zarr.Store;

namespace ZarrNET.Core;

// =============================================================================
// Image descriptor — caller fills this in before handing off to the writer
// =============================================================================

/// <summary>
/// Everything the writer needs to know about a 5D image being saved.
/// Shape is always interpreted as (T, C, Z, Y, X) in this descriptor.
/// All dimension sizes must be at least 1.
/// </summary>
public class BioImageDescriptor
{
    public string Name     { get; init; } = "image";
    public string DataType { get; init; } = "uint16";

    public int SizeT { get; }
    public int SizeC { get; }
    public int SizeZ { get; }
    public int SizeY { get; }
    public int SizeX { get; }

    public double PhysicalSizeZ { get; init; } = 1.0;
    public double PhysicalSizeY { get; init; } = 1.0;
    public double PhysicalSizeX { get; init; } = 1.0;

    public int ChunkT { get; init; } = 1;
    public int ChunkC { get; init; } = 1;
    public int ChunkZ { get; init; } = 1;
    public int ChunkY { get; init; } = 512;
    public int ChunkX { get; init; } = 512;

    public long[] Shape  => [SizeT, SizeC, SizeZ, SizeY, SizeX];
    public int[]  Chunks => [ChunkT, ChunkC, ChunkZ, ChunkY, ChunkX];

    public BioImageDescriptor(int sizeX, int sizeY, ZCT coord)
    {
        SizeX = sizeX; SizeY = sizeY;
        SizeZ = coord.Z; SizeC = coord.C; SizeT = coord.T;
    }
}

// =============================================================================
// Per-level descriptor
// =============================================================================

/// <summary>
/// Describes one resolution level in a multi-scale pyramid.
/// </summary>
public class ResolutionLevelDescriptor
{
    public int    SizeX      { get; }
    public int    SizeY      { get; }
    /// <summary>Downsample factor relative to level 0. Level 0 = 1.0.</summary>
    public double Downsample { get; }

    public ResolutionLevelDescriptor(int sizeX, int sizeY, double downsample)
    {
        SizeX = sizeX; SizeY = sizeY; Downsample = downsample;
    }
}

// =============================================================================
// Writer
// =============================================================================

/// <summary>
/// Creates a new OME-Zarr v3 dataset on disk and writes pixel data into it.
///
/// Performance notes
/// -----------------
/// • <see cref="ZarrArray"/> handles are opened once per level at construction
///   time and cached for the lifetime of the writer.  Re-opening on every tile
///   (the previous behaviour) paid the cost of reading and parsing zarr.json
///   for every single write call.
///
/// • <see cref="WriteRegionAsync"/> is guarded by a <see cref="SemaphoreSlim"/>
///   whose width is <see cref="MaxConcurrentWrites"/> (default: logical CPU
///   count, capped at 16).  Callers can fire many writes concurrently and the
///   semaphore prevents the store from being overwhelmed while still keeping
///   all cores busy.
///
/// • Metadata for all levels is written in parallel during bootstrap.
/// </summary>
public sealed class OmeZarrWriter : IAsyncDisposable
{
    // Maximum simultaneous in-flight store writes.
    // One write per logical core is a good default for local NVMe/SSD;
    // lower it (e.g. 4) for spinning-disk or network stores.
    public static int MaxConcurrentWrites { get; set; } =
        Math.Min(Environment.ProcessorCount, 16);

    private readonly IZarrStore                              _store;
    private readonly BioImageDescriptor                      _descriptor;
    private readonly IReadOnlyList<ResolutionLevelDescriptor> _levels;

    // One cached ZarrArray per level — opened once, reused for all tiles.
    private readonly ZarrArray[]  _arrays;
    private readonly SemaphoreSlim _writeSem;
    private bool _disposed;

    private OmeZarrWriter(
        IZarrStore                               store,
        BioImageDescriptor                       descriptor,
        IReadOnlyList<ResolutionLevelDescriptor> levels,
        ZarrArray[]                              arrays)
    {
        _store      = store;
        _descriptor = descriptor;
        _levels     = levels;
        _arrays     = arrays;
        _writeSem   = new SemaphoreSlim(MaxConcurrentWrites, MaxConcurrentWrites);
    }

    // -------------------------------------------------------------------------
    // Public factories
    // -------------------------------------------------------------------------

    /// <summary>Single-resolution convenience overload (backward-compatible).</summary>
    public static Task<OmeZarrWriter> CreateAsync(
        string             outputPath,
        BioImageDescriptor descriptor,
        CancellationToken  ct = default)
    {
        var singleLevel = new[]
        {
            new ResolutionLevelDescriptor(descriptor.SizeX, descriptor.SizeY, 1.0)
        };
        return CreateAsync(outputPath, descriptor, singleLevel, ct);
    }

    /// <summary>
    /// Multi-scale factory. Creates one Zarr array per entry in
    /// <paramref name="levels"/> (paths "0", "1", "2", …).
    /// All array zarr.json files are written in parallel.
    /// The returned writer has pre-opened handles to every array.
    /// </summary>
    public static async Task<OmeZarrWriter> CreateAsync(
        string                                   outputPath,
        BioImageDescriptor                       descriptor,
        IReadOnlyList<ResolutionLevelDescriptor> levels,
        CancellationToken                        ct = default)
    {
        if (levels == null || levels.Count == 0)
            throw new ArgumentException("At least one resolution level is required.", nameof(levels));

        Directory.CreateDirectory(outputPath);
        var store = new LocalFileSystemStore(outputPath);

        // --- Bootstrap metadata (root group + all array zarr.json files) ---
        // Write root zarr.json first (sequential), then all array metadata in parallel.
        await WriteRootGroupMetadataAsync(store, descriptor, levels, ct).ConfigureAwait(false);

        await Task.WhenAll(
            Enumerable.Range(0, levels.Count)
                      .Select(i => WriteArrayMetadataAsync(store, descriptor, levels[i], i, ct))
        ).ConfigureAwait(false);

        // --- Pre-open one ZarrArray handle per level ---
        var rootGroup = await ZarrGroup.OpenRootAsync(store, ct).ConfigureAwait(false);
        var arrays    = await Task.WhenAll(
            Enumerable.Range(0, levels.Count)
                      .Select(i => rootGroup.OpenArrayAsync(i.ToString(), ct))
        ).ConfigureAwait(false);

        return new OmeZarrWriter(store, descriptor, levels, arrays);
    }

    // -------------------------------------------------------------------------
    // Public write API
    // -------------------------------------------------------------------------

    /// <summary>
    /// Writes the full pixel buffer for the entire image at level 0 in one call.
    /// </summary>
    public async Task WritePixelDataAsync(byte[] pixelData, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var arr         = _arrays[0];
        var regionStart = new long[arr.Metadata.Rank];
        var regionEnd   = arr.Metadata.Shape;

        await _writeSem.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await arr.WriteRegionAsync(regionStart, regionEnd, pixelData, ct)
                     .ConfigureAwait(false);
        }
        finally { _writeSem.Release(); }
    }

    /// <summary>
    /// Writes a single Z-plane into the array at level 0.
    /// </summary>
    public async Task WritePlaneAsync(int zIndex, byte[] planeData, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var d           = _descriptor;
        var regionStart = new long[] { 0,       0,       zIndex,     0,       0       };
        var regionEnd   = new long[] { d.SizeT, d.SizeC, zIndex + 1, d.SizeY, d.SizeX };

        await _writeSem.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await _arrays[0].WriteRegionAsync(regionStart, regionEnd, planeData, ct)
                            .ConfigureAwait(false);
        }
        finally { _writeSem.Release(); }
    }

    /// <summary>
    /// Writes an arbitrary sub-region into the 5D array at
    /// <paramref name="levelIndex"/> (0 = full resolution).
    ///
    /// Thread-safe: many callers may call this concurrently; the internal
    /// semaphore limits simultaneous in-flight store writes to
    /// <see cref="MaxConcurrentWrites"/>.
    /// </summary>
    public async Task WriteRegionAsync(
        int    t,       int c,      int z,
        int    yOffset, int xOffset,
        int    height,  int width,
        byte[] data,
        int    levelIndex    = 0,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var regionStart = new long[] { t,     c,     z,     yOffset,          xOffset         };
        var regionEnd   = new long[] { t + 1, c + 1, z + 1, yOffset + height, xOffset + width };

        await _writeSem.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await _arrays[levelIndex]
                    .WriteRegionAsync(regionStart, regionEnd, data, ct)
                    .ConfigureAwait(false);
        }
        finally { _writeSem.Release(); }
    }

    // -------------------------------------------------------------------------
    // Metadata helpers (static so they can be called before the writer exists)
    // -------------------------------------------------------------------------

    private static async Task WriteRootGroupMetadataAsync(
        IZarrStore                               store,
        BioImageDescriptor                       d,
        IReadOnlyList<ResolutionLevelDescriptor> levels,
        CancellationToken                        ct)
    {
        var datasets = levels.Select((lvl, i) => new
        {
            path = i.ToString(),
            coordinateTransformations = new object[]
            {
                new
                {
                    type  = "scale",
                    scale = new[]
                    {
                        1.0, 1.0,
                        d.PhysicalSizeZ,
                        d.PhysicalSizeY * lvl.Downsample,
                        d.PhysicalSizeX * lvl.Downsample
                    }
                }
            }
        }).ToArray();

        var multiscale = new
        {
            version = "0.5",
            name    = d.Name,
            axes    = new object[]
            {
                new { name = "t", type = "time"    },
                new { name = "c", type = "channel" },
                new { name = "z", type = "space", unit = "micrometer" },
                new { name = "y", type = "space", unit = "micrometer" },
                new { name = "x", type = "space", unit = "micrometer" }
            },
            datasets                  = datasets,
            coordinateTransformations = new object[]
            {
                new { type = "scale", scale = new[] { 1.0, 1.0, d.PhysicalSizeZ, d.PhysicalSizeY, d.PhysicalSizeX } }
            }
        };

        var rootDoc = new
        {
            zarr_format = 3,
            node_type   = "group",
            attributes  = new
            {
                ome = new { version = "0.5", multiscales = new[] { multiscale } }
            }
        };

        await WriteJsonAsync(store, "zarr.json", rootDoc, ct).ConfigureAwait(false);
    }

    private static async Task WriteArrayMetadataAsync(
        IZarrStore                 store,
        BioImageDescriptor         d,
        ResolutionLevelDescriptor  lvl,
        int                        levelIndex,
        CancellationToken          ct)
    {
        var elementSize = ZarrDataType.Parse(d.DataType).ElementSize;
        int chunkY      = Math.Min(d.ChunkY, lvl.SizeY);
        int chunkX      = Math.Min(d.ChunkX, lvl.SizeX);

        var arrayDoc = new
        {
            zarr_format = 3,
            node_type   = "array",
            shape       = new long[] { d.SizeT, d.SizeC, d.SizeZ, lvl.SizeY, lvl.SizeX },
            data_type   = d.DataType,
            chunk_grid  = new
            {
                name          = "regular",
                configuration = new { chunk_shape = new[] { d.ChunkT, d.ChunkC, d.ChunkZ, chunkY, chunkX } }
            },
            chunk_key_encoding = new
            {
                name          = "default",
                configuration = new { separator = "/" }
            },
            fill_value      = 0,
            dimension_names = new[] { "t", "c", "z", "y", "x" },
            codecs          = new object[]
            {
                new
                {
                    name          = "blosc",
                    configuration = new
                    {
                        cname    = "lz4",
                        clevel   = 5,
                        shuffle  = "byteshuffle",
                        typesize = elementSize,
                        blocksize= 0
                    }
                }
            }
        };

        await WriteJsonAsync(store, $"{levelIndex}/zarr.json", arrayDoc, ct).ConfigureAwait(false);
    }

    private static async Task WriteJsonAsync(IZarrStore store, string key, object document, CancellationToken ct)
    {
        var options = new JsonSerializerOptions { WriteIndented = true };
        var json    = JsonSerializer.SerializeToUtf8Bytes(document, options);
        await store.WriteAsync(key, json, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Disposal
    // -------------------------------------------------------------------------

    private void ThrowIfDisposed()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(OmeZarrWriter));
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        _writeSem.Dispose();
        await _store.DisposeAsync().ConfigureAwait(false);
    }
}
