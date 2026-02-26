using OmeZarr.Core.Zarr.Codecs;
using OmeZarr.Core.Zarr.Metadata;
using OmeZarr.Core.Zarr.Store;

namespace OmeZarr.Core.Zarr;

/// <summary>
/// Represents a single Zarr v3 array. Knows how to read and write chunks
/// via the store using the array's codec pipeline and chunk key encoding.
///
/// Does not interpret array contents — that is the responsibility of callers
/// who know the OME axis semantics.
/// </summary>
public sealed class ZarrArray
{
    private readonly IZarrStore _store;
    private readonly string _arrayPath;   // store-relative path to the array root
    private readonly CodecPipeline _pipeline;

    public ZarrArrayMetadata Metadata { get; }

    internal ZarrArray(IZarrStore store, string arrayPath, ZarrArrayMetadata metadata)
    {
        _store = store;
        _arrayPath = arrayPath.TrimEnd('/');
        Metadata = metadata;
        _pipeline = CodecFactory.BuildPipeline(metadata);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------
    public const int MaxParallelChunks = 16;
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

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, parallelism),
            CancellationToken = ct
        };

        await Parallel.ForEachAsync(chunkCoords, options, async (chunkCoord, token) =>
        {
            var chunkData = await ReadChunkAsync(chunkCoord, token).ConfigureAwait(false);

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

    // -------------------------------------------------------------------------
    // Chunk reading / writing
    // -------------------------------------------------------------------------

    private async Task<byte[]> ReadChunkAsync(long[] chunkCoord, CancellationToken ct)
    {
        var key = BuildChunkKey(chunkCoord);
        var bytes = await _store.ReadAsync(key, ct).ConfigureAwait(false);

        if (bytes is null)
        {
            // Chunk doesn't exist - return fill value
            return BuildFillValueChunk();
        }

        var decoded = await _pipeline.DecodeAsync(bytes, ct).ConfigureAwait(false);

        // Validate decoded size - must match expected chunk size
        var expectedElements = Metadata.ChunkShape.Aggregate(1L, (acc, s) => acc * s);
        var expectedBytes = expectedElements * Metadata.DataType.ElementSize;
        var actualBytes = decoded.Length;

        if (actualBytes != expectedBytes)
        {
            // Truncated edge chunks: some Zarr implementations write only the valid portion
            // of edge chunks (no fill-value padding). We must expand them into the full chunk
            // shape so CopyChunkRegionToOutput can use a consistent stride.
            //
            // A flat Array.Copy to offset 0 is only correct when the innermost dimension
            // is also full width — if ANY dimension is clipped, the decoded rows are narrower
            // than the nominal chunkShape, and a flat copy produces wrong strides for all
            // rows after the first, causing black bands at image edges.
            if (actualBytes < expectedBytes)
            {
                int elementSize = Metadata.DataType.ElementSize;
                var padded = BuildFillValueChunk();
                var fullChunkShape = Metadata.ChunkShape.Select(s => (long)s).ToArray();
                var truncatedShape = ComputeTruncatedChunkShape(chunkCoord);

                var expectedTruncatedBytes = ComputeTotalElements(truncatedShape) * elementSize;

                if (actualBytes == (int)expectedTruncatedBytes)
                    ExpandTruncatedChunk(decoded, truncatedShape, padded, fullChunkShape, elementSize);
                else
                    Array.Copy(decoded, 0, padded, 0, decoded.Length);  // unknown truncation — best effort

                return padded;
            }

            throw new InvalidOperationException(
                $"Decoded chunk at {string.Join(",", chunkCoord)} has {actualBytes} bytes, " +
                $"expected {expectedBytes} bytes. Chunk shape: [{string.Join(", ", Metadata.ChunkShape)}], " +
                $"element size: {Metadata.DataType.ElementSize} bytes.");
        }

        // DIAGNOSTIC: Check if decoded data is all zeros
        var hasNonZero = decoded.Any(b => b != 0);
        if (!hasNonZero)
        {
            // This might be legitimate (empty chunk) or a decoding issue
            // Log the raw encoded size for debugging
            System.Diagnostics.Debug.WriteLine(
                $"Warning: Chunk [{string.Join(",", chunkCoord)}] decoded to all zeros. " +
                $"Raw size: {bytes.Length} bytes, decoded size: {decoded.Length} bytes.");
        }

        return decoded;
    }

    private async Task WriteChunkAsync(long[] chunkCoord, byte[] decodedData, CancellationToken ct)
    {
        var encoded = await _pipeline.EncodeAsync(decodedData, ct).ConfigureAwait(false);
        var key = BuildChunkKey(chunkCoord);

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
        var chunkData = await ReadChunkAsync(chunkCoord, ct).ConfigureAwait(false);
        var chunkOrigin = ComputeChunkOrigin(chunkCoord);
        var chunkShape = Metadata.ChunkShape.Select(s => (long)s).ToArray();
        var clampedStart = ClampToChunk(regionStart, chunkOrigin, chunkShape, clampToStart: true);
        var clampedEnd = ClampToChunk(regionEnd, chunkOrigin, chunkShape, clampToStart: false);

        CopySourceRegionToChunk(
            chunkOrigin,
            chunkShape,
            clampedStart,
            clampedEnd,
            regionStart,
            regionShape,
            sourceData,
            chunkData,
            elementSize);

        await WriteChunkAsync(chunkCoord, chunkData, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Region/chunk copy helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Copies the relevant portion of a decoded chunk into the output buffer
    /// at the correct offset for the requested region.
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
        var rank = Metadata.Rank;
        var chunkOrigin = ComputeChunkOrigin(chunkCoord);
        var chunkShape = Metadata.ChunkShape.Select(s => (long)s).ToArray();

        // The range within the array that this chunk covers, clipped to our region
        var copyStart = new long[rank];
        var copyEnd = new long[rank];

        for (int d = 0; d < rank; d++)
        {
            copyStart[d] = Math.Max(regionStart[d], chunkOrigin[d]);
            copyEnd[d] = Math.Min(regionEnd[d], chunkOrigin[d] + chunkShape[d]);
        }

        // Iterate over all elements in the copy range
        IterateNdRegion(copyStart, copyEnd, rank, indices =>
        {
            var chunkOffset = ComputeFlatIndex(SubtractArrays(indices, chunkOrigin), chunkShape);
            var outputOffset = ComputeFlatIndex(SubtractArrays(indices, regionStart), regionShape);

            var chunkByteOffset = (int)(chunkOffset * elementSize);
            var outputByteOffset = (int)(outputOffset * elementSize);

            // Bounds check before copy
            if (chunkByteOffset + elementSize > chunkData.Length)
            {
                throw new InvalidOperationException(
                    $"Chunk bounds exceeded: trying to read at byte offset {chunkByteOffset} " +
                    $"(element {chunkOffset}) from chunk with {chunkData.Length} bytes " +
                    $"({chunkData.Length / elementSize} elements). " +
                    $"Chunk shape: [{string.Join(", ", chunkShape)}], element size: {elementSize}.");
            }

            if (outputByteOffset + elementSize > outputBuffer.Length)
            {
                throw new InvalidOperationException(
                    $"Output bounds exceeded: trying to write at byte offset {outputByteOffset} " +
                    $"(element {outputOffset}) to buffer with {outputBuffer.Length} bytes " +
                    $"({outputBuffer.Length / elementSize} elements). " +
                    $"Region shape: [{string.Join(", ", regionShape)}], element size: {elementSize}.");
            }

            Buffer.BlockCopy(
                chunkData, chunkByteOffset,
                outputBuffer, outputByteOffset,
                elementSize);
        });
    }

    private void CopySourceRegionToChunk(
        long[] chunkOrigin,
        long[] chunkShape,
        long[] clampedStart,
        long[] clampedEnd,
        long[] regionStart,
        long[] regionShape,
        byte[] sourceData,
        byte[] chunkData,
        int elementSize)
    {
        var rank = Metadata.Rank;

        IterateNdRegion(clampedStart, clampedEnd, rank, indices =>
        {
            var chunkOffset = ComputeFlatIndex(SubtractArrays(indices, chunkOrigin), chunkShape);
            var sourceOffset = ComputeFlatIndex(SubtractArrays(indices, regionStart), regionShape);

            Buffer.BlockCopy(
                sourceData, (int)(sourceOffset * elementSize),
                chunkData, (int)(chunkOffset * elementSize),
                elementSize);
        });
    }

    // -------------------------------------------------------------------------
    // Chunk enumeration and key building
    // -------------------------------------------------------------------------

    private IEnumerable<long[]> EnumerateChunkCoordinates(long[] regionStart, long[] regionEnd)
    {
        var rank = Metadata.Rank;
        var chunkShape = Metadata.ChunkShape;

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
        var chunkElements = Metadata.ChunkShape.Aggregate(1L, (acc, s) => acc * s);
        return new byte[chunkElements * Metadata.DataType.ElementSize];
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
            origin[d] = chunkCoord[d] * Metadata.ChunkShape[d];
        return origin;
    }

    /// <summary>
    /// Returns the actual element count per dimension for a chunk at chunkCoord.
    /// For interior chunks this equals ChunkShape. For edge chunks it is clamped to
    /// the array extent, matching the layout of truncated edge-chunk files.
    /// </summary>
    private long[] ComputeTruncatedChunkShape(long[] chunkCoord)
    {
        var rank = Metadata.Rank;
        var truncated = new long[rank];

        for (int d = 0; d < rank; d++)
        {
            var origin = chunkCoord[d] * Metadata.ChunkShape[d];
            truncated[d] = Math.Min(Metadata.Shape[d] - origin, Metadata.ChunkShape[d]);
        }

        return truncated;
    }

    /// <summary>
    /// Copies elements from a C-order truncated chunk buffer (srcShape strides) into
    /// the correct positions of a full-size C-order buffer (dstShape strides).
    /// This is necessary when a Zarr implementation stores edge chunks without
    /// fill-value padding — the decoded rows are narrower than a full chunk row,
    /// so a flat copy would produce wrong strides starting from the second row.
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

        IterateNdRegion(start, srcShape, rank, indices =>
        {
            var srcByteOffset = (int)(ComputeFlatIndex(indices, srcShape) * elementSize);
            var dstByteOffset = (int)(ComputeFlatIndex(indices, dstShape) * elementSize);

            Buffer.BlockCopy(src, srcByteOffset, dst, dstByteOffset, elementSize);
        });
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

    private static long[] SubtractArrays(long[] a, long[] b)
    {
        var result = new long[a.Length];
        for (int i = 0; i < a.Length; i++)
            result[i] = a[i] - b[i];
        return result;
    }

    // -------------------------------------------------------------------------
    // N-dimensional iteration helpers
    // All iteration uses exclusive end: [start, end)
    // -------------------------------------------------------------------------

    private static void IterateNdRegion(
        long[] start,
        long[] end,
        int rank,
        Action<long[]> body)
    {
        foreach (var coord in IterateNdCoordinates(start, end, rank))
            body(coord);
    }

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
}