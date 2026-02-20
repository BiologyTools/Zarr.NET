using System.Text.Json;
using System.Text.Json.Serialization;

namespace OmeZarr.Core.OmeZarr.Metadata;

/// <summary>
/// Detects the OME-Zarr node type from a raw attributes JsonElement and
/// deserializes the relevant metadata models. This is the single entry
/// point between the raw Zarr attributes and the typed OME metadata layer.
/// </summary>
public static class OmeAttributesParser
{
    private static readonly JsonSerializerOptions _options = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling         = JsonCommentHandling.Skip,
        AllowTrailingCommas         = true,
        NumberHandling              = JsonNumberHandling.AllowReadingFromString,
        Converters                  = { new AxisMetadataJsonConverter() }
    };

    // -------------------------------------------------------------------------
    // Node type detection
    // -------------------------------------------------------------------------

    public enum OmeNodeType
    {
        Unknown,
        MultiscaleImage,
        Plate,
        Well,
        LabelGroup,
        LabelImage      // a multiscale that also carries image-label metadata
    }

    public static OmeNodeType DetectNodeType(JsonElement? attributes)
    {
        if (attributes is null)
            return OmeNodeType.Unknown;

        var attrs = attributes.Value;

        if (attrs.TryGetProperty("plate", out _))
            return OmeNodeType.Plate;

        if (attrs.TryGetProperty("well", out _))
            return OmeNodeType.Well;

        if (attrs.TryGetProperty("labels", out _))
            return OmeNodeType.LabelGroup;

        if (attrs.TryGetProperty("multiscales", out _))
        {
            return attrs.TryGetProperty("image-label", out _)
                ? OmeNodeType.LabelImage
                : OmeNodeType.MultiscaleImage;
        }

        return OmeNodeType.Unknown;
    }

    // -------------------------------------------------------------------------
    // Typed metadata parsing
    // -------------------------------------------------------------------------

    public static MultiscaleMetadata[] ParseMultiscales(JsonElement attributes)
    {
        if (!attributes.TryGetProperty("multiscales", out var multiscalesEl))
            throw new InvalidOperationException("Attributes do not contain a 'multiscales' key.");

        return JsonSerializer.Deserialize<MultiscaleMetadataJson[]>(multiscalesEl.GetRawText(), _options)
               !.Select(ToMultiscaleMetadata)
               .ToArray();
    }

    public static PlateMetadata ParsePlate(JsonElement attributes)
    {
        if (!attributes.TryGetProperty("plate", out var plateEl))
            throw new InvalidOperationException("Attributes do not contain a 'plate' key.");

        return JsonSerializer.Deserialize<PlateMetadataJson>(plateEl.GetRawText(), _options)
               !.ToModel();
    }

    public static WellMetadata ParseWell(JsonElement attributes)
    {
        if (!attributes.TryGetProperty("well", out var wellEl))
            throw new InvalidOperationException("Attributes do not contain a 'well' key.");

        return JsonSerializer.Deserialize<WellMetadataJson>(wellEl.GetRawText(), _options)
               !.ToModel();
    }

    public static LabelGroupMetadata ParseLabelGroup(JsonElement attributes)
    {
        if (!attributes.TryGetProperty("labels", out var labelsEl))
            throw new InvalidOperationException("Attributes do not contain a 'labels' key.");

        var labels = JsonSerializer.Deserialize<string[]>(labelsEl.GetRawText(), _options)
                     ?? Array.Empty<string>();

        return new LabelGroupMetadata { Labels = labels };
    }

    public static ImageLabelMetadata ParseImageLabel(JsonElement attributes)
    {
        if (!attributes.TryGetProperty("image-label", out var labelEl))
            throw new InvalidOperationException("Attributes do not contain an 'image-label' key.");

        return JsonSerializer.Deserialize<ImageLabelMetadataJson>(labelEl.GetRawText(), _options)
               !.ToModel();
    }

    // -------------------------------------------------------------------------
    // JSON intermediate models → typed metadata models
    // These exist so we can handle JSON naming conventions separately from
    // the clean domain model, keeping the metadata classes free of JsonProperty noise.
    // -------------------------------------------------------------------------

    private static MultiscaleMetadata ToMultiscaleMetadata(MultiscaleMetadataJson json) =>
        new()
        {
            Version  = json.Version ?? string.Empty,
            Name     = json.Name,
            Type     = json.Type,
            Axes     = json.Axes?.Select(ToAxisMetadata).ToArray() ?? Array.Empty<AxisMetadata>(),
            Datasets = json.Datasets?.Select(ToDatasetMetadata).ToArray() ?? Array.Empty<DatasetMetadata>(),
            CoordinateTransformations = json.CoordinateTransformations?
                .Select(ToCoordinateTransformation).ToArray(),
            Omero = json.Omero is null ? null : ToOmeMetadata(json.Omero)
        };

    private static AxisMetadata ToAxisMetadata(AxisMetadataJson json) =>
        new() { Name = json.Name ?? string.Empty, Type = json.Type, Unit = json.Unit };

    private static DatasetMetadata ToDatasetMetadata(DatasetMetadataJson json) =>
        new()
        {
            Path = json.Path ?? string.Empty,
            CoordinateTransformations = json.CoordinateTransformations?
                .Select(ToCoordinateTransformation).ToArray() ?? Array.Empty<CoordinateTransformation>()
        };

    private static CoordinateTransformation ToCoordinateTransformation(CoordinateTransformationJson json) =>
        new()
        {
            Type        = json.Type ?? string.Empty,
            Scale       = json.Scale,
            Translation = json.Translation,
            Path        = json.Path
        };

    private static OmeMetadata ToOmeMetadata(OmeMetadataJson json) =>
        new()
        {
            Channels = json.Channels?.Select(ToChannelMetadata).ToArray() ?? Array.Empty<ChannelDisplayMetadata>(),
            Rdefs    = json.Rdefs is null ? null : new RenderingWindowMetadata
            {
                DefaultZ = json.Rdefs.DefaultZ,
                DefaultT = json.Rdefs.DefaultT,
                Model    = json.Rdefs.Model
            }
        };

    private static ChannelDisplayMetadata ToChannelMetadata(ChannelDisplayMetadataJson json) =>
        new()
        {
            Active = json.Active,
            Color  = json.Color,
            Label  = json.Label,
            Window = json.Window is null ? null : new WindowMetadata
            {
                Min   = json.Window.Min,
                Max   = json.Window.Max,
                Start = json.Window.Start,
                End   = json.Window.End
            }
        };

    // -------------------------------------------------------------------------
    // JSON intermediate types (private — not part of the public API)
    // -------------------------------------------------------------------------

    private sealed class MultiscaleMetadataJson
    {
        [JsonPropertyName("version")]     public string? Version  { get; init; }
        [JsonPropertyName("name")]        public string? Name     { get; init; }
        [JsonPropertyName("type")]        public string? Type     { get; init; }
        [JsonPropertyName("axes")]        public AxisMetadataJson[]? Axes { get; init; }
        [JsonPropertyName("datasets")]    public DatasetMetadataJson[]? Datasets { get; init; }
        [JsonPropertyName("coordinateTransformations")]
        public CoordinateTransformationJson[]? CoordinateTransformations { get; init; }
        [JsonPropertyName("omero")]       public OmeMetadataJson? Omero { get; init; }
    }

    private sealed class AxisMetadataJson
    {
        [JsonPropertyName("name")] public string? Name { get; init; }
        [JsonPropertyName("type")] public string? Type { get; init; }
        [JsonPropertyName("unit")] public string? Unit { get; init; }
    }

    /// <summary>
    /// Handles both OME-Zarr axis representations:
    ///   v0.1/v0.2 — axes as a plain string array:  ["t", "c", "z", "y", "x"]
    ///   v0.3+     — axes as an object array:        [{"name":"t","type":"time"}, ...]
    /// When a string token is encountered the name is taken from the string value
    /// directly; type and unit are left null and inferred later by EffectiveAxes.
    /// </summary>
    private sealed class AxisMetadataJsonConverter : JsonConverter<AxisMetadataJson>
    {
        public override AxisMetadataJson Read(
            ref Utf8JsonReader   reader,
            Type                 typeToConvert,
            JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
                return new AxisMetadataJson { Name = reader.GetString() };

            if (reader.TokenType != JsonTokenType.StartObject)
                throw new JsonException(
                    $"Unexpected token {reader.TokenType} reading AxisMetadataJson.");

            string? name = null, type = null, unit = null;

            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                if (reader.TokenType != JsonTokenType.PropertyName)
                    continue;

                var propertyName = reader.GetString()!.ToLowerInvariant();
                reader.Read();

                switch (propertyName)
                {
                    case "name": name = reader.GetString(); break;
                    case "type": type = reader.GetString(); break;
                    case "unit": unit = reader.GetString(); break;
                    default:     reader.Skip();             break;
                }
            }

            return new AxisMetadataJson { Name = name, Type = type, Unit = unit };
        }

        public override void Write(
            Utf8JsonWriter        writer,
            AxisMetadataJson      value,
            JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            if (value.Name is not null) writer.WriteString("name", value.Name);
            if (value.Type is not null) writer.WriteString("type", value.Type);
            if (value.Unit is not null) writer.WriteString("unit", value.Unit);
            writer.WriteEndObject();
        }
    }

    private sealed class DatasetMetadataJson
    {
        [JsonPropertyName("path")] public string? Path { get; init; }
        [JsonPropertyName("coordinateTransformations")]
        public CoordinateTransformationJson[]? CoordinateTransformations { get; init; }
    }

    private sealed class CoordinateTransformationJson
    {
        [JsonPropertyName("type")]        public string?   Type        { get; init; }
        [JsonPropertyName("scale")]       public double[]? Scale       { get; init; }
        [JsonPropertyName("translation")] public double[]? Translation { get; init; }
        [JsonPropertyName("path")]        public string?   Path        { get; init; }
    }

    private sealed class PlateMetadataJson
    {
        [JsonPropertyName("name")]         public string? Name         { get; init; }
        [JsonPropertyName("columns")]      public ColumnJson[]? Columns { get; init; }
        [JsonPropertyName("rows")]         public RowJson[]? Rows       { get; init; }
        [JsonPropertyName("wells")]        public WellReferenceJson[]? Wells { get; init; }
        [JsonPropertyName("acquisitions")] public AcquisitionJson[]? Acquisitions { get; init; }
        [JsonPropertyName("field_count")]  public int? FieldCount { get; init; }
        [JsonPropertyName("version")]      public string? Version { get; init; }

        public PlateMetadata ToModel() => new()
        {
            Name         = Name ?? string.Empty,
            Columns      = Columns?.Select(c => new ColumnMetadata { Name = c.Name ?? "" }).ToArray() ?? Array.Empty<ColumnMetadata>(),
            Rows         = Rows?.Select(r => new RowMetadata { Name = r.Name ?? "" }).ToArray() ?? Array.Empty<RowMetadata>(),
            Wells        = Wells?.Select(w => new WellReference { ColumnIndex = w.ColumnIndex ?? "", RowIndex = w.RowIndex ?? "", Path = w.Path ?? "" }).ToArray() ?? Array.Empty<WellReference>(),
            Acquisitions = Acquisitions?.Select(a => new AcquisitionMetadata { Id = a.Id, Name = a.Name, Description = a.Description }).ToArray() ?? Array.Empty<AcquisitionMetadata>(),
            Version      = Version
        };
    }

    private sealed class ColumnJson
    {
        [JsonPropertyName("name")] public string? Name { get; init; }
    }

    private sealed class RowJson
    {
        [JsonPropertyName("name")] public string? Name { get; init; }
    }

    private sealed class WellReferenceJson
    {
        [JsonPropertyName("columnIndex")] public string? ColumnIndex { get; init; }
        [JsonPropertyName("rowIndex")]    public string? RowIndex    { get; init; }
        [JsonPropertyName("path")]        public string? Path        { get; init; }
    }

    private sealed class AcquisitionJson
    {
        [JsonPropertyName("id")]          public int     Id          { get; init; }
        [JsonPropertyName("name")]        public string? Name        { get; init; }
        [JsonPropertyName("description")] public string? Description { get; init; }
        [JsonPropertyName("starttime")]   public long?   StartTime   { get; init; }
        [JsonPropertyName("endtime")]     public long?   EndTime     { get; init; }
    }

    private sealed class WellMetadataJson
    {
        [JsonPropertyName("images")]  public FieldReferenceJson[]? Images  { get; init; }
        [JsonPropertyName("version")] public string?               Version { get; init; }

        public WellMetadata ToModel() => new()
        {
            Images  = Images?.Select(i => new FieldReference { AcquisitionId = i.Acquisition, Path = i.Path ?? "" }).ToArray() ?? Array.Empty<FieldReference>(),
            Version = Version
        };
    }

    private sealed class FieldReferenceJson
    {
        [JsonPropertyName("acquisition")] public int     Acquisition { get; init; }
        [JsonPropertyName("path")]        public string? Path        { get; init; }
    }

    private sealed class ImageLabelMetadataJson
    {
        [JsonPropertyName("version")]    public string?              Version    { get; init; }
        [JsonPropertyName("colors")]     public LabelColorJson[]?    Colors     { get; init; }
        [JsonPropertyName("properties")] public LabelPropertyJson[]? Properties { get; init; }
        [JsonPropertyName("source")]     public LabelSourceJson?     Source     { get; init; }

        public ImageLabelMetadata ToModel() => new()
        {
            Version    = Version,
            Colors     = Colors?.Select(c => new LabelColorEntry { LabelValue = c.LabelValue, Rgba = c.Rgba }).ToArray() ?? Array.Empty<LabelColorEntry>(),
            Properties = Properties?.Select(p => new LabelProperty { LabelValue = p.LabelValue }).ToArray() ?? Array.Empty<LabelProperty>(),
            Source     = Source is null ? null : new LabelSourceLink { Href = Source.Href }
        };
    }

    private sealed class LabelColorJson
    {
        [JsonPropertyName("label-value")] public int    LabelValue { get; init; }
        [JsonPropertyName("rgba")]        public int[]? Rgba       { get; init; }
    }

    private sealed class LabelPropertyJson
    {
        [JsonPropertyName("label-value")] public int LabelValue { get; init; }
    }

    private sealed class LabelSourceJson
    {
        [JsonPropertyName("href")] public string? Href { get; init; }
    }

    private sealed class OmeMetadataJson
    {
        [JsonPropertyName("channels")] public ChannelDisplayMetadataJson[]? Channels { get; init; }
        [JsonPropertyName("rdefs")]    public RenderingWindowJson?           Rdefs    { get; init; }
    }

    private sealed class ChannelDisplayMetadataJson
    {
        [JsonPropertyName("active")] public bool?   Active { get; init; }
        [JsonPropertyName("color")]  public string? Color  { get; init; }
        [JsonPropertyName("label")]  public string? Label  { get; init; }
        [JsonPropertyName("window")] public WindowJson? Window { get; init; }
    }

    private sealed class WindowJson
    {
        [JsonPropertyName("min")]   public double? Min   { get; init; }
        [JsonPropertyName("max")]   public double? Max   { get; init; }
        [JsonPropertyName("start")] public double? Start { get; init; }
        [JsonPropertyName("end")]   public double? End   { get; init; }
    }

    private sealed class RenderingWindowJson
    {
        [JsonPropertyName("defaultZ")] public string? DefaultZ { get; init; }
        [JsonPropertyName("defaultT")] public string? DefaultT { get; init; }
        [JsonPropertyName("model")]    public string? Model    { get; init; }
    }
}
