using System.Text.Json;
using System.Text.Json.Serialization;

namespace OmeZarr.Core.Zarr.Metadata;

// =============================================================================
// Zarr v2 .zarray document
// =============================================================================

/// <summary>
/// Raw deserialization of a .zarray file (Zarr v2).
/// Maps array metadata — shape, chunks, dtype, compression.
/// </summary>
public sealed class ZarrV2ArrayDocument
{
    [JsonPropertyName("zarr_format")]
    public int ZarrFormat { get; init; }

    [JsonPropertyName("shape")]
    public long[] Shape { get; init; } = Array.Empty<long>();

    [JsonPropertyName("chunks")]
    public int[] Chunks { get; init; } = Array.Empty<int>();

    /// <summary>
    /// Numpy-style dtype string, e.g. "<u2" (little-endian uint16), ">f4" (big-endian float32).
    /// </summary>
    [JsonPropertyName("dtype")]
    public string Dtype { get; init; } = string.Empty;

    /// <summary>
    /// Compressor configuration. Null means no compression.
    /// Example: { "id": "gzip", "level": 5 }
    /// </summary>
    [JsonPropertyName("compressor")]
    public ZarrV2CompressorDocument? Compressor { get; init; }

    [JsonPropertyName("fill_value")]
    public JsonElement? FillValue { get; init; }

    /// <summary>
    /// Array order: "C" (row-major) or "F" (column-major). Almost always "C".
    /// </summary>
    [JsonPropertyName("order")]
    public string Order { get; init; } = "C";

    [JsonPropertyName("filters")]
    public JsonElement? Filters { get; init; }

    [JsonPropertyName("dimension_separator")]
    public string? DimensionSeparator { get; init; }

    // -------------------------------------------------------------------------
    // Parsing
    // -------------------------------------------------------------------------

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling         = JsonCommentHandling.Skip,
        AllowTrailingCommas         = true
    };

    public static ZarrV2ArrayDocument Parse(string json)
        => JsonSerializer.Deserialize<ZarrV2ArrayDocument>(json, _jsonOptions)
           ?? throw new InvalidOperationException("Failed to deserialize .zarray: null result.");

    public static ZarrV2ArrayDocument Parse(byte[] utf8Json)
        => JsonSerializer.Deserialize<ZarrV2ArrayDocument>(utf8Json, _jsonOptions)
           ?? throw new InvalidOperationException("Failed to deserialize .zarray: null result.");
}

// =============================================================================
// Zarr v2 compressor
// =============================================================================

public sealed class ZarrV2CompressorDocument
{
    [JsonPropertyName("id")]
    public string Id { get; init; } = string.Empty;

    [JsonPropertyName("level")]
    public int? Level { get; init; }

    [JsonPropertyName("cname")]
    public string? Cname { get; init; }  // Blosc compressor name

    [JsonPropertyName("clevel")]
    public int? Clevel { get; init; }    // Blosc compression level

    [JsonPropertyName("shuffle")]
    public int? Shuffle { get; init; }   // Blosc shuffle mode
}

// =============================================================================
// Zarr v2 .zgroup document
// =============================================================================

/// <summary>
/// Raw deserialization of a .zgroup file (Zarr v2).
/// Just contains the format version — no other metadata.
/// </summary>
public sealed class ZarrV2GroupDocument
{
    [JsonPropertyName("zarr_format")]
    public int ZarrFormat { get; init; }

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling         = JsonCommentHandling.Skip,
        AllowTrailingCommas         = true
    };

    public static ZarrV2GroupDocument Parse(string json)
        => JsonSerializer.Deserialize<ZarrV2GroupDocument>(json, _jsonOptions)
           ?? throw new InvalidOperationException("Failed to deserialize .zgroup: null result.");

    public static ZarrV2GroupDocument Parse(byte[] utf8Json)
        => JsonSerializer.Deserialize<ZarrV2GroupDocument>(utf8Json, _jsonOptions)
           ?? throw new InvalidOperationException("Failed to deserialize .zgroup: null result.");
}

// =============================================================================
// Zarr v2 .zattrs document
// =============================================================================

/// <summary>
/// Raw deserialization of a .zattrs file (Zarr v2).
/// This is freeform JSON — OME-Zarr metadata lives here.
/// We just parse it as a raw JsonElement and pass to OmeAttributesParser.
/// </summary>
public sealed class ZarrV2AttrsDocument
{
    public JsonElement Root { get; init; }

    public static ZarrV2AttrsDocument Parse(string json)
    {
        var root = JsonSerializer.Deserialize<JsonElement>(json);
        return new ZarrV2AttrsDocument { Root = root };
    }

    public static ZarrV2AttrsDocument Parse(byte[] utf8Json)
    {
        var root = JsonSerializer.Deserialize<JsonElement>(utf8Json);
        return new ZarrV2AttrsDocument { Root = root };
    }
}
