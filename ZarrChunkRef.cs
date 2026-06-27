namespace ZarrNET.Core.Zarr;

/// <summary>
/// Identifies one logical chunk of a Zarr array.
/// ChunkCoord is the chunk-grid coordinate, Origin is the array element origin,
/// Shape is the valid element shape within the array extent, and Key is the
/// store key containing the encoded chunk bytes.
/// </summary>
public readonly record struct ZarrChunkRef(
    long[] ChunkCoord,
    long[] Origin,
    long[] Shape,
    string Key);

/// <summary>
/// Encoded payload for one logical Zarr chunk.
/// When IsPresent is false, the chunk key is absent and Zarr fill-value
/// semantics apply.
/// </summary>
public readonly record struct ZarrEncodedChunk(
    ZarrChunkRef Chunk,
    ReadOnlyMemory<byte> EncodedBytes,
    bool IsPresent)
{
    public static ZarrEncodedChunk Present(ZarrChunkRef chunk, ReadOnlyMemory<byte> encodedBytes)
        => new(chunk, encodedBytes, IsPresent: true);

    public static ZarrEncodedChunk Missing(ZarrChunkRef chunk)
        => new(chunk, ReadOnlyMemory<byte>.Empty, IsPresent: false);
}

/// <summary>
/// Decoded full-chunk payload for one logical Zarr chunk.
/// </summary>
public readonly record struct ZarrDecodedChunkWrite(
    ZarrChunkRef Chunk,
    ReadOnlyMemory<byte> DecodedBytes);
