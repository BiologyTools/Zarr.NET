using OmeZarr.Core.Zarr.Metadata;

namespace OmeZarr.Core.Zarr.Codecs;

/// <summary>
/// Builds a CodecPipeline from the CodecInfo descriptors stored in ZarrArrayMetadata.
/// This is the only place that knows which codec name maps to which implementation.
/// </summary>
public static class CodecFactory
{
    public static CodecPipeline BuildPipeline(ZarrArrayMetadata metadata)
    {
        var codecs = metadata.Codecs
            .Select(info => BuildCodec(info))
            .ToList();

        return new CodecPipeline(codecs, metadata.DataType.ElementSize);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static IZarrCodec BuildCodec(CodecInfo info)
    {
        return info.Name switch
        {
            "bytes"  => BuildBytesCodec(info),
            "gzip"   => BuildGzipCodec(info),
            "zstd"   => BuildZstdCodec(info),
            "blosc"  => throw new NotSupportedException(
                            "Blosc codec is not yet supported. " +
                            "Consider re-saving the array with gzip or zstd."),
            _ => throw new NotSupportedException(
                     $"Unknown or unsupported codec: '{info.Name}'")
        };
    }

    private static BytesCodec BuildBytesCodec(CodecInfo info)
    {
        var endian = info.Configuration?.TryGetProperty("endian", out var endianProp) == true
            ? endianProp.GetString()
            : "little";

        var byteOrder = endian == "big"
            ? ByteOrder.BigEndian
            : ByteOrder.LittleEndian;

        return new BytesCodec(byteOrder);
    }

    private static GzipCodec BuildGzipCodec(CodecInfo info)
    {
        var level = info.Configuration?.TryGetProperty("level", out var levelProp) == true
            ? levelProp.GetInt32()
            : 6;

        return new GzipCodec(level);
    }

    private static ZstdCodec BuildZstdCodec(CodecInfo info)
    {
        var level = info.Configuration?.TryGetProperty("level", out var levelProp) == true
            ? levelProp.GetInt32()
            : 3;

        return new ZstdCodec(level);
    }
}
