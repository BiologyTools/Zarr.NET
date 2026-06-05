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
