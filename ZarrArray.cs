using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using ZarrNET;
using ZarrNET.Core.Zarr.Store;

namespace ZarrNET.Core.Zarr;

/// <summary>
/// Represents a single Zarr v3 array. Knows how to read and write chunks
/// via the store using the array's codec pipeline and chunk key encoding.
///
/// When sharding is active (Metadata.Sharding is non-null), the outer
/// ChunkShape represents the shard shape and the actual data granularity
/// is the inner chunk shape. ReadRegionAsync transparently handles both
/// cases — callers do not need to know whether sharding is in use.
///
/// Does not interpret array contents — that is the responsibility of callers
/// who know the OME axis semantics.
/// </summary>
public sealed class ZarrArray
{
    private static int s_writeDebugCount = 0;
    private static int s_readDebugCount = 0;
    private static readonly ConcurrentDictionary<string, byte> s_writePlaneProbeLogged = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, byte> s_readPlaneProbeLogged = new(StringComparer.Ordinal);

    private readonly IZarrStore _store;
    private readonly string _arrayPath;   // store-relative path to the array root
    private readonly CodecPipeline _pipeline;

    // Effective chunk shape — inner chunk shape when sharding, outer chunk shape otherwise.
    // This is the granularity at which ReadRegionAsync iterates and assembles data.
    private readonly long[] _chunkShapeLong;
    private readonly long   _chunkElementCount;

    public ZarrArrayMetadata Metadata { get; }

    internal ZarrArray(IZarrStore store, string arrayPath, ZarrArrayMetadata metadata)
    {
        _store = store;
        _arrayPath = arrayPath.TrimEnd('/');
        Metadata = metadata;
        _pipeline = CodecFactory.BuildPipeline(metadata);

        // When sharding is active, the effective chunk shape is the inner chunk shape.
        // The outer chunk shape (shard shape) is used only for building store keys.
        var effectiveChunkShape = metadata.Sharding?.InnerChunkShape ?? metadata.ChunkShape;

        _chunkShapeLong = effectiveChunkShape.Select(s => (long)s).ToArray();
        _chunkElementCount = effectiveChunkShape.Aggregate(1L, (acc, s) => acc * s);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------
    // Keep per-tile chunk fan-out low so panning does not build up large
    // numbers of in-flight byte[] buffers before the previous viewport has
    // finished decoding and uploading.
    public const int MaxParallelChunks = 2;

    // Global semaphore: caps the total number of concurrent chunk S3 fetches
    // across all parallel tile requests to prevent memory spikes.
    private static readonly SemaphoreSlim s_globalFetchSemaphore = new SemaphoreSlim(16, 16);
    /// <summary>
    /// Reads a region of the array defined by per-axis [start, end) ranges.
    /// Returns the decoded bytes for that region, assembled from the
    /// relevant chunks. Shape of the returned region is (end - start) per axis.
    ///
    /// Chunks are fetched and decoded in parallel, bounded by maxParallelChunks.
    /// Each chunk maps to a non-overlapping slice of the output buffer, so the
    /// copy step is safe without locking.
    /// </summary>
    /// <param name="regionStart">Per-axis inclusive start indices.</param>
    /// <param name="regionEnd">Per-axis exclusive end indices.</param>
    /// <param name="maxParallelChunks">
    /// Maximum number of chunks to fetch/decode concurrently.
    /// Defaults to <see cref="Environment.ProcessorCount"/>.
    /// HTTP callers may benefit from higher values (e.g. 16–32);
    /// local disk callers can leave the default or lower it.
    /// Pass 1 to disable parallelism.
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    public async Task<byte[]> ReadRegionAsync(
        long[] regionStart,
        long[] regionEnd,
        int? maxParallelChunks = MaxParallelChunks,
        CancellationToken ct = default)
    {
        ValidateRegion(regionStart, regionEnd);

        var regionShape = ComputeRegionShape(regionStart, regionEnd);
        var elementSize = Metadata.DataType.ElementSize;
        var totalElements = ComputeTotalElements(regionShape);
        var outputBuffer = new byte[totalElements * elementSize];

        var chunkCoords = EnumerateChunkCoordinates(regionStart, regionEnd);
        var parallelism = maxParallelChunks ?? Environment.ProcessorCount;

        // When sharding is active, cache shard file bytes so that multiple inner
        // chunks within the same shard don't trigger redundant store reads.
        // The cache is scoped to this single ReadRegionAsync call.
        var shardCache = Metadata.Sharding is not null
            ? new ConcurrentDictionary<string, Task<byte[]?>>(StringComparer.Ordinal)
            : null;

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, parallelism),
            CancellationToken = ct
        };

        await Parallel.ForEachAsync(chunkCoords, options, async (chunkCoord, token) =>
        {
            var chunkData = await ReadChunkAsync(chunkCoord, shardCache, token).ConfigureAwait(false);
            if (chunkData != null)
            CopyChunkRegionToOutput(
                chunkCoord,
                chunkData,
                regionStart,
                regionEnd,
                regionShape,
                outputBuffer,
                elementSize);
        }).ConfigureAwait(false);

        return outputBuffer;
    }

    /// <summary>
    /// Writes a region of the array defined by per-axis [start, end) ranges.
    /// Data must be a flat byte array matching the region shape exactly.
    /// Performs read-modify-write for partial chunk writes.
    /// </summary>
    public async Task WriteRegionAsync(
        long[] regionStart,
        long[] regionEnd,
        byte[] data,
        CancellationToken ct = default)
    {
        ValidateRegion(regionStart, regionEnd);

        var regionShape = ComputeRegionShape(regionStart, regionEnd);
        var elementSize = Metadata.DataType.ElementSize;
        var expectedBytes = ComputeTotalElements(regionShape) * elementSize;

        if (data.Length != expectedBytes)
            throw new ArgumentException(
                $"Data length {data.Length} does not match region size {expectedBytes} bytes.");

        var chunkCoords = EnumerateChunkCoordinates(regionStart, regionEnd);

        foreach (var chunkCoord in chunkCoords)
        {
            ct.ThrowIfCancellationRequested();

            await WriteChunkRegionAsync(
                chunkCoord,
                regionStart,
                regionEnd,
                regionShape,
                data,
                elementSize,
                ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Enumerates every logical chunk in the array's regular chunk grid.
    /// The returned chunk shape is clamped to the array extent for edge chunks.
    /// </summary>
    public async IAsyncEnumerable<ZarrChunkRef> EnumerateChunksAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (Metadata.Shape.Any(s => s == 0))
            yield break;

        var start = new long[Metadata.Rank];
        var end = (long[])Metadata.Shape.Clone();

        await foreach (var chunk in EnumerateChunksAsync(start, end, ct).ConfigureAwait(false))
            yield return chunk;
    }

    /// <summary>
    /// Enumerates logical chunks intersecting the region [regionStart, regionEnd).
    /// The returned chunk shape is clamped to both the array extent and edge chunks,
    /// but not to the requested region; each reference identifies a whole chunk.
    /// </summary>
    public async IAsyncEnumerable<ZarrChunkRef> EnumerateChunksAsync(
        long[] regionStart,
        long[] regionEnd,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ValidateRegion(regionStart, regionEnd);

        foreach (var chunkCoord in EnumerateChunkCoordinates(regionStart, regionEnd))
        {
            ct.ThrowIfCancellationRequested();
            yield return BuildChunkRef(chunkCoord);
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <summary>
    /// Reads and decodes a full logical chunk without routing through a region
    /// output buffer. Missing chunks are returned as fill-value chunks.
    ///
    /// The returned buffer is padded to the full effective chunk shape used by
    /// this array. Use <see cref="ZarrChunkRef.Shape"/> to identify the valid
    /// in-array extent for edge chunks.
    /// </summary>
    public async Task<byte[]> ReadChunkDecodedAsync(
        ZarrChunkRef chunk,
        CancellationToken ct = default)
    {
        ValidateChunkRef(chunk);

        var shardCache = Metadata.Sharding is not null
            ? new ConcurrentDictionary<string, Task<byte[]?>>(StringComparer.Ordinal)
            : null;

        return await ReadChunkAsync(chunk.ChunkCoord, shardCache, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a full logical chunk into caller-owned memory. The destination must be
    /// at least the full effective chunk byte count. When <paramref name="allowBorrowedBuffer"/>
    /// is true and the codec pipeline is a no-op bytes path, the store may read
    /// directly into <paramref name="destination"/>.
    /// </summary>
    public async Task ReadChunkDecodedAsync(
        ZarrChunkRef chunk,
        Memory<byte> destination,
        bool allowBorrowedBuffer,
        CancellationToken ct = default)
    {
        ValidateChunkRef(chunk);

        var expectedBytes = checked((int)(_chunkElementCount * Metadata.DataType.ElementSize));
        if (destination.Length < expectedBytes)
            throw new ArgumentException(
                $"Destination has {destination.Length} bytes, expected at least {expectedBytes} bytes " +
                $"for full chunk shape [{string.Join(", ", _chunkShapeLong)}].",
                nameof(destination));

        var chunkDestination = destination[..expectedBytes];

        if (allowBorrowedBuffer
            && Metadata.Sharding is null
            && _pipeline.CanStoreDecodedBytesWithoutTransform)
        {
            var key = BuildChunkContainingStoreKey(chunk.ChunkCoord);
            var bytesRead = await _store.ReadAsync(key, chunkDestination, ct).ConfigureAwait(false);

            if (bytesRead is null)
            {
                BuildFillValueChunk().CopyTo(chunkDestination);
                return;
            }

            if (bytesRead.Value == expectedBytes)
                return;

            var normalized = PadOrValidateDecodedChunk(
                chunkDestination[..bytesRead.Value].ToArray(),
                chunk.ChunkCoord);
            normalized.CopyTo(chunkDestination);
            return;
        }

        var decoded = await ReadChunkDecodedAsync(chunk, ct).ConfigureAwait(false);
        decoded.CopyTo(chunkDestination);
    }

    public Task ReadChunkDecodedAsync(
        long[] chunkCoord,
        Memory<byte> destination,
        bool allowBorrowedBuffer,
        CancellationToken ct = default)
        => ReadChunkDecodedAsync(
            BuildChunkRef(chunkCoord),
            destination,
            allowBorrowedBuffer,
            ct);

    /// <summary>
    /// Encodes and writes a full logical chunk without read-modify-write.
    /// The input must be a full effective chunk buffer, including fill-value
    /// padding for any edge region outside the array extent.
    /// </summary>
    public async Task WriteChunkDecodedAsync(
        ZarrChunkRef chunk,
        byte[] decodedData,
        CancellationToken ct = default)
        => await WriteChunkDecodedAsync(
            chunk,
            decodedData.AsMemory(),
            allowBorrowedBuffer: false,
            ct).ConfigureAwait(false);

    /// <summary>
    /// Encodes and writes a full logical chunk without read-modify-write.
    /// The input memory length must be the full effective chunk byte count.
    /// Pooled buffers may be passed as a sliced memory region; the memory is
    /// consumed or copied before the returned task completes.
    /// </summary>
    public async Task WriteChunkDecodedAsync(
        ZarrChunkRef chunk,
        ReadOnlyMemory<byte> decodedData,
        CancellationToken ct = default)
        => await WriteChunkDecodedAsync(
            chunk,
            decodedData,
            allowBorrowedBuffer: false,
            ct).ConfigureAwait(false);

    /// <summary>
    /// Encodes and writes a full logical chunk without read-modify-write.
    /// When <paramref name="allowBorrowedBuffer"/> is true and the codec pipeline is
    /// a no-op bytes path, caller-owned memory is consumed directly by the store
    /// before this task completes.
    /// </summary>
    public async Task WriteChunkDecodedAsync(
        ZarrChunkRef chunk,
        ReadOnlyMemory<byte> decodedData,
        bool allowBorrowedBuffer,
        CancellationToken ct = default)
    {
        if (Metadata.Sharding is not null)
            throw new NotSupportedException(
                "Decoded chunk writes are not supported for sharded arrays.");

        ValidateChunkRef(chunk);

        var expectedBytes = checked((int)(_chunkElementCount * Metadata.DataType.ElementSize));
        if (decodedData.Length != expectedBytes)
            throw new ArgumentException(
                $"decodedData has {decodedData.Length} bytes, expected {expectedBytes} bytes " +
                $"for full chunk shape [{string.Join(", ", _chunkShapeLong)}].",
                nameof(decodedData));

        if (allowBorrowedBuffer && _pipeline.CanStoreDecodedBytesWithoutTransform)
        {
            var key = BuildChunkContainingStoreKey(chunk.ChunkCoord);
            await _store.WriteAsync(key, decodedData, ct).ConfigureAwait(false);
            return;
        }

        await WriteChunkAsync(
            chunk.ChunkCoord,
            ToExactArray(decodedData),
            ct).ConfigureAwait(false);
    }

    public Task WriteChunkDecodedAsync(
        long[] chunkCoord,
        ReadOnlyMemory<byte> decodedData,
        bool allowBorrowedBuffer,
        CancellationToken ct = default)
        => WriteChunkDecodedAsync(
            BuildChunkRef(chunkCoord),
            decodedData,
            allowBorrowedBuffer,
            ct);

    /// <summary>
    /// Reads the encoded bytes for a non-sharded chunk directly from the store.
    /// Returns null when the chunk key is absent, matching Zarr fill-value
    /// semantics for sparse chunks.
    /// </summary>
    public async Task<byte[]?> ReadChunkEncodedAsync(
        ZarrChunkRef chunk,
        CancellationToken ct = default)
    {
        EnsureNonShardedEncodedChunkAccess();
        ValidateChunkRef(chunk);

        var key = BuildChunkContainingStoreKey(chunk.ChunkCoord);
        return await _store.ReadAsync(key, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes encoded bytes for a non-sharded chunk directly to the store.
    /// Callers should only use this when the encoded bytes are compatible with
    /// this array's metadata, chunk shape, dtype, and codec pipeline.
    /// </summary>
    public async Task WriteChunkEncodedAsync(
        ZarrChunkRef chunk,
        byte[] encodedData,
        CancellationToken ct = default)
        => await WriteChunkEncodedAsync(
            chunk,
            encodedData.AsMemory(),
            ct).ConfigureAwait(false);

    /// <summary>
    /// Writes encoded bytes for a non-sharded chunk directly to the store.
    /// Pooled buffers may be passed as a sliced memory region; the memory is
    /// consumed or copied before the returned task completes.
    /// </summary>
    public async Task WriteChunkEncodedAsync(
        ZarrChunkRef chunk,
        ReadOnlyMemory<byte> encodedData,
        CancellationToken ct = default)
    {
        EnsureNonShardedEncodedChunkAccess();
        ValidateChunkRef(chunk);

        var key = BuildChunkContainingStoreKey(chunk.ChunkCoord);
        await _store.WriteAsync(key, encodedData, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Chunk reading / writing
    // -------------------------------------------------------------------------

    private async Task<byte[]> ReadChunkAsync(
        long[] chunkCoord,
        ConcurrentDictionary<string, Task<byte[]?>>? shardCache,
        CancellationToken ct)
    {
        // Sharded path — chunkCoord addresses an inner chunk, not a shard
        if (Metadata.Sharding is not null)
            return await ReadShardedChunkAsync(chunkCoord, shardCache!, ct).ConfigureAwait(false);

        // Non-sharded path — one store key per chunk
        return await ReadDirectChunkAsync(chunkCoord, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Non-sharded chunk read
    // -------------------------------------------------------------------------

    private async Task<byte[]> ReadDirectChunkAsync(long[] chunkCoord, CancellationToken ct)
    {
        var key = BuildChunkKey(chunkCoord);
        await s_globalFetchSemaphore.WaitAsync(ct).ConfigureAwait(false);
        byte[]? bytes;
        try
        {
            bytes = await _store.ReadAsync(key, ct).ConfigureAwait(false);
        }
        finally
        {
            s_globalFetchSemaphore.Release();
        }

        if (bytes is null)
            return BuildFillValueChunk();

        var decoded = await _pipeline.DecodeAsync(bytes, ct).ConfigureAwait(false);
        if (s_readDebugCount < 8)
        {
            Log($"[ZarrArray.ReadDirectChunkAsync] chunk={string.Join(",", chunkCoord)} encodedLen={bytes.Length} " +
                $"encodedSample={SampleBytes(bytes)} decodedLen={decoded.Length} decodedSample={SampleBytes(decoded)} decodedU16={SampleU16(decoded)}");
            s_readDebugCount++;
        }

        if (ShouldLogPlaneProbe(chunkCoord, s_readPlaneProbeLogged))
        {
            Log($"[ZarrArray.ReadDirectChunkAsync.Probe] chunk={string.Join(",", chunkCoord)} encodedLen={bytes.Length} " +
                $"encodedSample={SampleBytes(bytes)} decodedLen={decoded.Length} decodedSample={SampleBytes(decoded)} decodedU16={SampleU16(decoded)}");
        }

        return PadOrValidateDecodedChunk(decoded, chunkCoord);
    }

    // -------------------------------------------------------------------------
    // Sharded chunk read
    // -------------------------------------------------------------------------

    /// <summary>
    /// Reads an inner chunk from within a shard file. The chunkCoord here is
    /// the global inner-chunk coordinate (based on inner chunk shape). We compute
    /// which shard it belongs to, fetch that shard (with caching), and extract
    /// the inner chunk.
    /// </summary>
    private async Task<byte[]> ReadShardedChunkAsync(
        long[] chunkCoord,
        ConcurrentDictionary<string, Task<byte[]?>> shardCache,
        CancellationToken ct)
    {
        var sharding   = Metadata.Sharding!;
        var rank       = Metadata.Rank;

        // Compute which shard this inner chunk belongs to, and its position within
        var shardCoord      = new long[rank];
        var innerChunkCoord = new long[rank];

        for (int d = 0; d < rank; d++)
        {
            var innersPerShard   = sharding.InnerChunksPerShard[d];
            var globalInnerIndex = chunkCoord[d];

            shardCoord[d]      = globalInnerIndex / innersPerShard;
            innerChunkCoord[d] = globalInnerIndex % innersPerShard;
        }

        // Fetch the shard file, using the cache to avoid redundant reads
        var shardKey   = BuildChunkKey(shardCoord);
        var shardBytes = await shardCache.GetOrAdd(
            shardKey,
            key => _store.ReadAsync(key, ct)
        ).ConfigureAwait(false);

        if (shardBytes is null)
            return BuildFillValueChunk();

        // Extract the inner chunk from the shard
        var innerChunkBytes = await ShardReader.ReadInnerChunkAsync(
            shardBytes, innerChunkCoord, sharding, ct).ConfigureAwait(false);

        if (innerChunkBytes is null)
            return BuildFillValueChunk();

        var padded = PadOrValidateDecodedChunk(innerChunkBytes, chunkCoord);
        if (s_readDebugCount < 8)
        {
            Log($"[ZarrArray.ReadShardedChunkAsync] chunk={string.Join(",", chunkCoord)} innerLen={innerChunkBytes.Length} " +
                $"innerSample={SampleBytes(innerChunkBytes)} paddedLen={padded.Length} paddedSample={SampleBytes(padded)} paddedU16={SampleU16(padded)}");
            s_readDebugCount++;
        }
        return padded;
    }

    // -------------------------------------------------------------------------
    // Decoded chunk validation / padding (shared by both paths)
    // -------------------------------------------------------------------------

    private byte[] PadOrValidateDecodedChunk(byte[] decoded, long[] chunkCoord)
    {
        var expectedBytes = _chunkElementCount * Metadata.DataType.ElementSize;
        var actualBytes   = decoded.Length;

        if (actualBytes == expectedBytes)
            return decoded;

        // Truncated edge chunks: some implementations write only the valid portion
        if (actualBytes < expectedBytes)
        {
            int elementSize    = Metadata.DataType.ElementSize;
            var padded         = BuildFillValueChunk();
            var truncatedShape = ComputeTruncatedChunkShape(chunkCoord);
            var expectedTruncatedBytes = ComputeTotalElements(truncatedShape) * elementSize;

            if (actualBytes == (int)expectedTruncatedBytes)
                ExpandTruncatedChunk(decoded, truncatedShape, padded, _chunkShapeLong, elementSize);
            else
                Array.Copy(decoded, 0, padded, 0, decoded.Length);  // unknown truncation — best effort

            return padded;
        }

        throw new InvalidOperationException(
            $"Decoded chunk at {string.Join(",", chunkCoord)} has {actualBytes} bytes, " +
            $"expected {expectedBytes} bytes. Chunk shape: [{string.Join(", ", _chunkShapeLong)}], " +
            $"element size: {Metadata.DataType.ElementSize} bytes.");
    }

    private async Task WriteChunkAsync(long[] chunkCoord, byte[] decodedData, CancellationToken ct)
    {
        if (s_writeDebugCount < 8)
        {
            Log($"[ZarrArray.WriteChunkAsync] chunk={string.Join(",", chunkCoord)} decodedLen={decodedData.Length} " +
                $"decodedSample={SampleBytes(decodedData)} decodedU16={SampleU16(decodedData)}");
        }

        var encoded = await _pipeline.EncodeAsync(decodedData, ct).ConfigureAwait(false);
        var key = BuildChunkKey(chunkCoord);

        if (s_writeDebugCount < 8)
        {
            Log($"[ZarrArray.WriteChunkAsync] chunk={string.Join(",", chunkCoord)} encodedLen={encoded.Length} " +
                $"encodedSample={SampleBytes(encoded)}");
            s_writeDebugCount++;
        }

        await _store.WriteAsync(key, encoded, ct).ConfigureAwait(false);
    }

    private async Task WriteChunkRegionAsync(
        long[] chunkCoord,
        long[] regionStart,
        long[] regionEnd,
        long[] regionShape,
        byte[] sourceData,
        int elementSize,
        CancellationToken ct)
    {
        // Write path doesn't use shard caching — pass null (non-sharded only for now)
        var chunkData = await ReadChunkAsync(chunkCoord, shardCache: null, ct).ConfigureAwait(false);
        var chunkOrigin = ComputeChunkOrigin(chunkCoord);
        var clampedStart = ClampToChunk(regionStart, chunkOrigin, _chunkShapeLong, clampToStart: true);
        var clampedEnd = ClampToChunk(regionEnd, chunkOrigin, _chunkShapeLong, clampToStart: false);

        CopyNdRegion(
            src: sourceData,
            srcOrigin: regionStart,
            srcShape: regionShape,
            dst: chunkData,
            dstOrigin: chunkOrigin,
            dstShape: _chunkShapeLong,
            copyStart: clampedStart,
            copyEnd: clampedEnd,
            elementSize: elementSize);

        if (s_writeDebugCount < 8)
        {
            Log($"[ZarrArray.WriteChunkRegionAsync] chunk={string.Join(",", chunkCoord)} srcShape={string.Join("x", regionShape)} " +
                $"chunkOrigin={string.Join(",", chunkOrigin)} srcSample={SampleBytes(sourceData)} chunkSample={SampleBytes(chunkData)} " +
                $"srcU16={SampleU16(sourceData)} chunkU16={SampleU16(chunkData)}");
        }

        if (ShouldLogPlaneProbe(chunkCoord, s_writePlaneProbeLogged))
        {
            Log($"[ZarrArray.WriteChunkRegionAsync.Probe] chunk={string.Join(",", chunkCoord)} srcShape={string.Join("x", regionShape)} " +
                $"chunkOrigin={string.Join(",", chunkOrigin)} srcSample={SampleBytes(sourceData)} chunkSample={SampleBytes(chunkData)} " +
                $"srcU16={SampleU16(sourceData)} chunkU16={SampleU16(chunkData)}");
        }

        await WriteChunkAsync(chunkCoord, chunkData, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Region/chunk copy — row-contiguous fast path
    // -------------------------------------------------------------------------

    /// <summary>
    /// Copies the relevant portion of a decoded chunk into the output buffer
    /// at the correct offset for the requested region.
    ///
    /// Uses a row-contiguous fast path: the innermost (last) axis is copied
    /// with a single Buffer.BlockCopy per row rather than element-by-element.
    /// The outer axes are iterated with a reusable coordinate array to avoid
    /// per-element heap allocations.
    /// </summary>
    private void CopyChunkRegionToOutput(
        long[] chunkCoord,
        byte[] chunkData,
        long[] regionStart,
        long[] regionEnd,
        long[] regionShape,
        byte[] outputBuffer,
        int elementSize)
    {
        var chunkOrigin = ComputeChunkOrigin(chunkCoord);

        var rank = Metadata.Rank;
        var copyStart = new long[rank];
        var copyEnd = new long[rank];

        for (int d = 0; d < rank; d++)
        {
            copyStart[d] = Math.Max(regionStart[d], chunkOrigin[d]);
            copyEnd[d] = Math.Min(regionEnd[d], chunkOrigin[d] + _chunkShapeLong[d]);
        }

        CopyNdRegion(
            src: chunkData,
            srcOrigin: chunkOrigin,
            srcShape: _chunkShapeLong,
            dst: outputBuffer,
            dstOrigin: regionStart,
            dstShape: regionShape,
            copyStart: copyStart,
            copyEnd: copyEnd,
            elementSize: elementSize);
    }

    /// <summary>
    /// General-purpose N-dimensional copy between two C-order flat buffers.
    /// Copies the region [copyStart, copyEnd) from src into dst, where each
    /// buffer has its own origin and shape.
    ///
    /// The innermost axis is copied as a contiguous row with a single
    /// Buffer.BlockCopy call. Outer axes are iterated with a single reusable
    /// coordinate array — no per-element heap allocations.
    /// </summary>
    private static void CopyNdRegion(
        byte[] src,
        long[] srcOrigin,
        long[] srcShape,
        byte[] dst,
        long[] dstOrigin,
        long[] dstShape,
        long[] copyStart,
        long[] copyEnd,
        int elementSize)
    {
        var rank = srcShape.Length;

        // How many elements to copy along the innermost axis per row
        var innerCount = copyEnd[rank - 1] - copyStart[rank - 1];
        var rowBytes = (int)(innerCount * elementSize);

        if (rowBytes <= 0)
            return;

        // Pre-compute strides for src and dst (C-order: last axis has stride 1)
        var srcStrides = ComputeStrides(srcShape);
        var dstStrides = ComputeStrides(dstShape);

        // For rank-1 arrays, just do one copy
        if (rank == 1)
        {
            var srcByteOffset = (int)((copyStart[0] - srcOrigin[0]) * srcStrides[0] * elementSize);
            var dstByteOffset = (int)((copyStart[0] - dstOrigin[0]) * dstStrides[0] * elementSize);
            Buffer.BlockCopy(src, srcByteOffset, dst, dstByteOffset, rowBytes);
            return;
        }

        // Iterate outer axes [0..rank-2], copy full inner row each time.
        // Uses a single reusable coordinate array — no per-iteration allocations.
        var outerRank = rank - 1;
        var current = new long[outerRank];
        for (int d = 0; d < outerRank; d++)
            current[d] = copyStart[d];

        while (true)
        {
            // Compute flat byte offsets for the start of this row in src and dst
            long srcElement = (copyStart[rank - 1] - srcOrigin[rank - 1]);
            long dstElement = (copyStart[rank - 1] - dstOrigin[rank - 1]);

            for (int d = 0; d < outerRank; d++)
            {
                srcElement += (current[d] - srcOrigin[d]) * srcStrides[d];
                dstElement += (current[d] - dstOrigin[d]) * dstStrides[d];
            }

            var srcByteOffset = (int)(srcElement * elementSize);
            var dstByteOffset = (int)(dstElement * elementSize);

            Buffer.BlockCopy(src, srcByteOffset, dst, dstByteOffset, rowBytes);

            // Advance outer coordinates (last outer axis first, C order)
            int axis = outerRank - 1;
            while (axis >= 0)
            {
                current[axis]++;
                if (current[axis] < copyEnd[axis])
                    break;
                current[axis] = copyStart[axis];
                axis--;
            }

            if (axis < 0)
                break;
        }
    }

    /// <summary>
    /// Computes C-order strides for a given shape.
    /// stride[d] = product of shape[d+1..rank-1].  stride[rank-1] = 1.
    /// </summary>
    private static long[] ComputeStrides(long[] shape)
    {
        var rank = shape.Length;
        var strides = new long[rank];
        strides[rank - 1] = 1;

        for (int d = rank - 2; d >= 0; d--)
            strides[d] = strides[d + 1] * shape[d + 1];

        return strides;
    }

    // -------------------------------------------------------------------------
    // Chunk enumeration and key building
    // -------------------------------------------------------------------------

    private IEnumerable<long[]> EnumerateChunkCoordinates(long[] regionStart, long[] regionEnd)
    {
        var rank = Metadata.Rank;

        // Use inner chunk shape when sharded, outer chunk shape otherwise.
        // _chunkShapeLong is already set to the effective shape in the constructor.
        var chunkShape = _chunkShapeLong;

        var firstChunk = new long[rank];
        var lastChunkExclusive = new long[rank];

        for (int d = 0; d < rank; d++)
        {
            firstChunk[d] = regionStart[d] / chunkShape[d];

            // Last chunk that intersects the region, then +1 for exclusive end
            lastChunkExclusive[d] = ((regionEnd[d] - 1) / chunkShape[d]) + 1;
        }

        return IterateNdCoordinates(firstChunk, lastChunkExclusive, rank);
    }

    private ZarrChunkRef BuildChunkRef(long[] chunkCoord)
    {
        var coord = (long[])chunkCoord.Clone();
        var origin = ComputeChunkOrigin(coord);
        var shape = ComputeTruncatedChunkShape(coord);
        var key = BuildChunkContainingStoreKey(coord);

        return new ZarrChunkRef(coord, origin, shape, key);
    }

    private string BuildChunkContainingStoreKey(long[] chunkCoord)
    {
        if (Metadata.Sharding is null)
            return BuildChunkKey(chunkCoord);

        var shardCoord = ComputeShardCoord(chunkCoord);
        return BuildChunkKey(shardCoord);
    }

    private long[] ComputeShardCoord(long[] innerChunkCoord)
    {
        var sharding = Metadata.Sharding
            ?? throw new InvalidOperationException("Array is not sharded.");

        var shardCoord = new long[Metadata.Rank];
        for (int d = 0; d < Metadata.Rank; d++)
            shardCoord[d] = innerChunkCoord[d] / sharding.InnerChunksPerShard[d];

        return shardCoord;
    }

    private string BuildChunkKey(long[] chunkCoord)
    {
        var sep = Metadata.ChunkKeySeparator;
        var coord = string.Join(sep, chunkCoord);

        // Zarr v2: chunks directly under array path
        //   - With "." separator: "arrayPath/0.0.0.0.0"
        //   - With "/" separator: "arrayPath/0/0/0/0/0"
        //
        // Zarr v3: chunks under c/ subdirectory with "/" separator
        //   - Always: "arrayPath/c/0/0/0/0/0"

        if (Metadata.ZarrVersion == 2)
        {
            return $"{_arrayPath}/{coord}";
        }
        else  // v3
        {
            return $"{_arrayPath}/c/{coord}";
        }
    }

    // -------------------------------------------------------------------------
    // Fill value
    // -------------------------------------------------------------------------

    private byte[] BuildFillValueChunk()
    {
        return new byte[_chunkElementCount * Metadata.DataType.ElementSize];
        // Fill value of 0 is used — a more complete implementation would
        // deserialise the fill_value from ZarrJsonDocument and populate here.
    }

    // -------------------------------------------------------------------------
    // Index / offset mathematics
    // -------------------------------------------------------------------------

    private long[] ComputeChunkOrigin(long[] chunkCoord)
    {
        var origin = new long[Metadata.Rank];
        for (int d = 0; d < Metadata.Rank; d++)
            origin[d] = chunkCoord[d] * _chunkShapeLong[d];
        return origin;
    }

    /// <summary>
    /// Returns the actual element count per dimension for a chunk at chunkCoord.
    /// For interior chunks this equals the effective chunk shape. For edge chunks
    /// it is clamped to the array extent, matching the layout of truncated
    /// edge-chunk files.
    /// </summary>
    private long[] ComputeTruncatedChunkShape(long[] chunkCoord)
    {
        var rank = Metadata.Rank;
        var truncated = new long[rank];

        for (int d = 0; d < rank; d++)
        {
            var origin = chunkCoord[d] * _chunkShapeLong[d];
            truncated[d] = Math.Min(Metadata.Shape[d] - origin, _chunkShapeLong[d]);
        }

        return truncated;
    }

    /// <summary>
    /// Copies elements from a C-order truncated chunk buffer (srcShape strides) into
    /// the correct positions of a full-size C-order buffer (dstShape strides).
    /// This is necessary when a Zarr implementation stores edge chunks without
    /// fill-value padding — the decoded rows are narrower than a full chunk row,
    /// so a flat copy would produce wrong strides starting from the second row.
    ///
    /// Uses the same row-contiguous fast path as CopyNdRegion: copies full rows
    /// along the innermost axis rather than element-by-element.
    /// </summary>
    private static void ExpandTruncatedChunk(
        byte[] src,
        long[] srcShape,
        byte[] dst,
        long[] dstShape,
        int elementSize)
    {
        var rank = srcShape.Length;
        var start = new long[rank];

        // src covers [0, srcShape) and dst covers [0, dstShape).
        // Both origins are zero, so we can use CopyNdRegion directly.
        var zeroOrigin = new long[rank];

        CopyNdRegion(
            src: src,
            srcOrigin: zeroOrigin,
            srcShape: srcShape,
            dst: dst,
            dstOrigin: zeroOrigin,
            dstShape: dstShape,
            copyStart: zeroOrigin,
            copyEnd: srcShape,
            elementSize: elementSize);
    }

    private static long[] ClampToChunk(
        long[] values,
        long[] chunkOrigin,
        long[] chunkShape,
        bool clampToStart)
    {
        var result = new long[values.Length];
        for (int d = 0; d < values.Length; d++)
        {
            result[d] = clampToStart
                ? Math.Max(values[d], chunkOrigin[d])
                : Math.Min(values[d], chunkOrigin[d] + chunkShape[d]);
        }
        return result;
    }

    private static long ComputeFlatIndex(long[] localIndices, long[] shape)
    {
        long index = 0;
        long stride = 1;

        for (int d = shape.Length - 1; d >= 0; d--)
        {
            index += localIndices[d] * stride;
            stride *= shape[d];
        }

        return index;
    }

    private static long[] ComputeRegionShape(long[] start, long[] end)
    {
        var shape = new long[start.Length];
        for (int d = 0; d < start.Length; d++)
            shape[d] = end[d] - start[d];
        return shape;
    }

    private static long ComputeTotalElements(long[] shape)
        => shape.Aggregate(1L, (acc, s) => acc * s);

    private static byte[] ToExactArray(ReadOnlyMemory<byte> data)
    {
        if (MemoryMarshal.TryGetArray(data, out ArraySegment<byte> segment) &&
            segment.Array is not null &&
            segment.Offset == 0 &&
            segment.Count == segment.Array.Length)
        {
            return segment.Array;
        }

        return data.ToArray();
    }

    private static void Log(string message)
    {
        System.Diagnostics.Debug.WriteLine(message);
    }

    private static string SampleBytes(byte[] data, int count = 16)
        => string.Join(",", data.Take(Math.Min(count, data.Length)));

    private static string SampleU16(byte[] data, int count = 8)
    {
        if (data.Length < 2)
            return string.Empty;

        int n = Math.Min(count, data.Length / 2);
        var vals = new ushort[n];
        for (int i = 0; i < n; i++)
            vals[i] = BitConverter.ToUInt16(data, i * 2);
        return string.Join(",", vals);
    }

    private static bool ShouldLogPlaneProbe(long[] chunkCoord, ConcurrentDictionary<string, byte> seen)
    {
        if (chunkCoord.Length < 3)
            return false;

        // Only log the chunk anchored at the spatial origin for each logical plane.
        if (chunkCoord[^1] != 0 || chunkCoord[^2] != 0)
            return false;

        long a = chunkCoord[0];
        long b = chunkCoord.Length > 1 ? chunkCoord[1] : 0;
        long c = chunkCoord.Length > 2 ? chunkCoord[2] : 0;
        var key = $"{chunkCoord.Length}:{a}:{b}:{c}";

        return seen.TryAdd(key, 0);
    }

    // -------------------------------------------------------------------------
    // N-dimensional iteration helpers
    // All iteration uses exclusive end: [start, end)
    // -------------------------------------------------------------------------

    private static IEnumerable<long[]> IterateNdCoordinates(
        long[] start,
        long[] end,    // EXCLUSIVE - [start, end)
        int rank)
    {
        var current = (long[])start.Clone();

        while (true)
        {
            yield return (long[])current.Clone();

            // Advance last-axis-first (C order / row-major)
            int d = rank - 1;
            while (d >= 0)
            {
                current[d]++;
                if (current[d] < end[d])  // Exclusive end
                    break;
                current[d] = start[d];
                d--;
            }

            if (d < 0)
                yield break;
        }
    }

    // -------------------------------------------------------------------------
    // Validation
    // -------------------------------------------------------------------------

    private void ValidateRegion(long[] regionStart, long[] regionEnd)
    {
        var rank = Metadata.Rank;

        if (regionStart.Length != rank)
            throw new ArgumentException(
                $"regionStart has {regionStart.Length} dimensions, expected {rank}.");

        if (regionEnd.Length != rank)
            throw new ArgumentException(
                $"regionEnd has {regionEnd.Length} dimensions, expected {rank}.");

        for (int d = 0; d < rank; d++)
        {
            if (regionStart[d] < 0 || regionStart[d] >= Metadata.Shape[d])
                throw new ArgumentOutOfRangeException(
                    $"regionStart[{d}] = {regionStart[d]} is out of bounds [0, {Metadata.Shape[d]}).");

            if (regionEnd[d] <= regionStart[d] || regionEnd[d] > Metadata.Shape[d])
                throw new ArgumentOutOfRangeException(
                    $"regionEnd[{d}] = {regionEnd[d]} is out of bounds ({regionStart[d]}, {Metadata.Shape[d]}].");
        }
    }

    private void ValidateChunkRef(ZarrChunkRef chunk)
    {
        var rank = Metadata.Rank;

        if (chunk.ChunkCoord.Length != rank)
            throw new ArgumentException(
                $"ChunkCoord has {chunk.ChunkCoord.Length} dimensions, expected {rank}.",
                nameof(chunk));

        var chunkCounts = ComputeChunkCounts();
        for (int d = 0; d < rank; d++)
        {
            if (chunk.ChunkCoord[d] < 0 || chunk.ChunkCoord[d] >= chunkCounts[d])
                throw new ArgumentOutOfRangeException(
                    nameof(chunk),
                    $"ChunkCoord[{d}] = {chunk.ChunkCoord[d]} is out of bounds [0, {chunkCounts[d]}).");
        }
    }

    private long[] ComputeChunkCounts()
    {
        var counts = new long[Metadata.Rank];
        for (int d = 0; d < Metadata.Rank; d++)
            counts[d] = (Metadata.Shape[d] + _chunkShapeLong[d] - 1) / _chunkShapeLong[d];
        return counts;
    }

    private void EnsureNonShardedEncodedChunkAccess()
    {
        if (Metadata.Sharding is not null)
            throw new NotSupportedException(
                "Encoded chunk access is not supported for sharded arrays because " +
                "logical inner chunks are stored inside shard objects.");
    }
}
