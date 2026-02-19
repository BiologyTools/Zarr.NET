using OmeZarr.Core.Zarr.Metadata;

namespace OmeZarr.Core.Zarr.Codecs;

/// <summary>
/// Builds a CodecPipeline from the CodecInfo descriptors stored in ZarrArrayMetadata.
/// This is the only place that knows which codec name maps to which implementation.
/// Handles both Zarr v3 (named pipeline) and Zarr v2 (single compressor, synthesised pipeline).
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
    // v3 codec building — from CodecInfo descriptors in zarr.json
    // -------------------------------------------------------------------------

    private static IZarrCodec BuildCodec(CodecInfo info)
    {
        return info.Name switch
        {
            "bytes" => BuildBytesCodec(info),
            "gzip"  => BuildGzipCodec(info),
            "zstd"  => BuildZstdCodec(info),
            "blosc" => BuildBloscCodecFromV3(info),
            _       => throw new NotSupportedException(
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

    private static BloscCodec BuildBloscCodecFromV3(CodecInfo info)
    {
        var cfg = info.Configuration;

        // cname — inner compressor name
        var cname = cfg?.TryGetProperty("cname", out var cnameProp) == true
            ? cnameProp.GetString() ?? "lz4"
            : "lz4";

        // clevel — compression level
        var clevel = cfg?.TryGetProperty("clevel", out var clevelProp) == true
            ? clevelProp.GetInt32()
            : 5;

        // shuffle — v3 spec uses string ("noshuffle", "byteshuffle", "bitshuffle")
        // but some implementations also write integers — accept both
        var shuffle = BloscShuffle.ByteShuffle;
        if (cfg?.TryGetProperty("shuffle", out var shuffleProp) == true)
        {
            shuffle = shuffleProp.ValueKind == System.Text.Json.JsonValueKind.Number
                ? (BloscShuffle)shuffleProp.GetInt32()
                : ParseShuffleString(shuffleProp.GetString());
        }

        // typesize — element size for shuffle; 0 or absent means 1 (no-op for shuffle)
        var typesize = cfg?.TryGetProperty("typesize", out var typesizeProp) == true
            ? typesizeProp.GetInt32()
            : 1;

        // blocksize — 0 means auto
        var blocksize = cfg?.TryGetProperty("blocksize", out var blocksizeProp) == true
            ? blocksizeProp.GetInt32()
            : 0;

        return new BloscCodec(cname, clevel, shuffle, typesize, blocksize);
    }

    // -------------------------------------------------------------------------
    // v2 codec building — from ZarrV2CompressorDocument in .zarray
    // -------------------------------------------------------------------------

    /// <summary>
    /// Builds a v3-style CodecInfo array from a v2 compressor descriptor.
    /// v2 always has an implicit bytes codec first, then an optional compression codec.
    /// </summary>
    internal static CodecInfo[] BuildV2CodecPipeline(
        ZarrV2CompressorDocument?           compressor,
        Zarr.Codecs.ByteOrder  byteOrder)
    {
        var bytesCodec = new CodecInfo(
            "bytes",
            System.Text.Json.JsonSerializer.SerializeToElement(
                new { endian = byteOrder == Zarr.Codecs.ByteOrder.BigEndian
                    ? "big"
                    : "little" }));

        if (compressor is null)
            return new[] { bytesCodec };

        var compressionCodec = compressor.Id switch
        {
            "gzip"  => new CodecInfo(
                           "gzip",
                           System.Text.Json.JsonSerializer.SerializeToElement(
                               new { level = compressor.Level ?? 6 })),

            "zstd"  => new CodecInfo(
                           "zstd",
                           System.Text.Json.JsonSerializer.SerializeToElement(
                               new { level = compressor.Level ?? 3 })),

            "blosc" => BuildBloscCodecInfoFromV2(compressor),

            _       => throw new NotSupportedException(
                           $"Unknown or unsupported v2 compressor: '{compressor.Id}'")
        };

        return new[] { bytesCodec, compressionCodec };
    }

    private static CodecInfo BuildBloscCodecInfoFromV2(ZarrV2CompressorDocument compressor)
    {
        // v2 blosc config fields: cname, clevel, shuffle (int), blocksize
        // Map to a v3-style configuration element that BuildBloscCodecFromV3 can parse
        var config = new
        {
            cname     = compressor.Cname     ?? "lz4",
            clevel    = compressor.Clevel    ?? 5,
            shuffle   = compressor.Shuffle   ?? 1,      // int form — accepted by BuildBloscCodecFromV3
            typesize  = 1,                              // not in v2 config; read from frame header at decode
            blocksize = compressor.Blocksize ?? 0
        };

        return new CodecInfo(
            "blosc",
            System.Text.Json.JsonSerializer.SerializeToElement(config));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static BloscShuffle ParseShuffleString(string? value)
    {
        return value?.ToLowerInvariant() switch
        {
            "noshuffle"    => BloscShuffle.None,
            "byteshuffle"  => BloscShuffle.ByteShuffle,
            "bitshuffle"   => BloscShuffle.BitShuffle,
            _              => BloscShuffle.ByteShuffle    // default to byte shuffle
        };
    }
}
