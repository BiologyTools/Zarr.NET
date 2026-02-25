using OmeZarr.Core.OmeZarr.Metadata;
using OmeZarr.Core.OmeZarr.Nodes;
using OmeZarr.Core.Zarr;
using OmeZarr.Core.Zarr.Store;

namespace OmeZarr.Core.OmeZarr;

/// <summary>
/// Entry point for reading OME-Zarr datasets.
///
/// Usage:
/// <code>
///   await using var reader = await OmeZarrReader.OpenAsync("/path/to/dataset.zarr");
///
///   // Multiscale image:
///   var image = reader.AsMultiscaleImage();
///   var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);
///   var roi   = new PhysicalROI(origin: [0, 0, 0], size: [100, 512, 512]);
///   var result = await level.ReadRegionAsync(roi);
///
///   // HCS plate:
///   var plate = reader.AsPlate();
///   var well  = await plate.OpenWellAsync("A", "1");
///   var field = await well.OpenFieldAsync(0);
///   var level = await field.OpenResolutionLevelAsync();
/// </code>
/// </summary>
public sealed class OmeZarrReader : IAsyncDisposable
{
    private readonly IZarrStore  _store;
    private readonly ZarrGroup   _rootGroup;
    private bool                 _disposed;

    public OmeAttributesParser.OmeNodeType RootNodeType { get; }

    /// <summary>
    /// The detected OME-NGFF specification version (e.g. "0.4", "0.5").
    /// Determined from the "ome.version" envelope (0.5+) or from
    /// "multiscales[0].version" (0.4 and earlier).
    /// Null if no version string is present in the metadata.
    /// </summary>
    public string? NgffVersion { get; }

    private OmeZarrReader(
        IZarrStore  store,
        ZarrGroup   rootGroup,
        OmeAttributesParser.OmeNodeType rootNodeType,
        string?     ngffVersion)
    {
        _store       = store;
        _rootGroup   = rootGroup;
        RootNodeType = rootNodeType;
        NgffVersion  = ngffVersion;
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /// <summary>
    /// Opens a Zarr store at the given path or URL and detects
    /// the OME-Zarr node type at the root.
    ///
    /// Supports:
    /// - Local filesystem paths (e.g., "C:\data\image.zarr" or "/data/image.zarr")
    /// - HTTP/HTTPS URLs (e.g., "https://example.com/data.zarr")
    /// - S3 URLs (e.g., "https://s3.amazonaws.com/bucket/data.zarr")
    /// </summary>
    public static async Task<OmeZarrReader> OpenAsync(
        string            pathOrUrl,
        CancellationToken ct = default)
    {
        IZarrStore store = CreateStore(pathOrUrl);

        try
        {
            var rootGroup   = await ZarrGroup.OpenRootAsync(store, ct).ConfigureAwait(false);
            var attributes  = rootGroup.Metadata.RawAttributes;
            var nodeType    = OmeAttributesParser.DetectNodeType(attributes);
            var ngffVersion = OmeAttributesParser.DetectNgffVersion(attributes);

            return new OmeZarrReader(store, rootGroup, nodeType, ngffVersion);
        }
        catch
        {
            await store.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Opens a Zarr store with a custom IZarrStore implementation.
    /// Useful for testing or custom storage backends.
    /// </summary>
    public static async Task<OmeZarrReader> OpenAsync(
        IZarrStore        store,
        CancellationToken ct = default)
    {
        var rootGroup   = await ZarrGroup.OpenRootAsync(store, ct).ConfigureAwait(false);
        var attributes  = rootGroup.Metadata.RawAttributes;
        var nodeType    = OmeAttributesParser.DetectNodeType(attributes);
        var ngffVersion = OmeAttributesParser.DetectNgffVersion(attributes);

        return new OmeZarrReader(store, rootGroup, nodeType, ngffVersion);
    }

    // -------------------------------------------------------------------------
    // Store creation
    // -------------------------------------------------------------------------

    private static IZarrStore CreateStore(string pathOrUrl)
    {
        // Detect if this is a URL or local path
        if (Uri.TryCreate(pathOrUrl, UriKind.Absolute, out var uri))
        {
            if (uri.Scheme == "http" || uri.Scheme == "https")
            {
                return new HttpZarrStore(pathOrUrl);
            }
            else if (uri.Scheme == "file")
            {
                return new LocalFileSystemStore(uri.LocalPath);
            }
        }

        // Treat as local filesystem path
        return new LocalFileSystemStore(pathOrUrl);
    }

    // -------------------------------------------------------------------------
    // Node access — returns a typed node based on the detected root type
    // -------------------------------------------------------------------------

    /// <summary>
    /// Returns the root as a MultiscaleNode.
    /// Throws if the root is not a multiscale image.
    /// </summary>
    public MultiscaleNode AsMultiscaleImage()
    {
        EnsureNodeType(OmeAttributesParser.OmeNodeType.MultiscaleImage, nameof(AsMultiscaleImage));

        var attributes  = RequireAttributes();
        var multiscales = OmeAttributesParser.ParseMultiscales(attributes);

        return new MultiscaleNode(_rootGroup, multiscales);
    }

    /// <summary>
    /// Returns the root as a PlateNode.
    /// Throws if the root is not an HCS plate.
    /// </summary>
    public PlateNode AsPlate()
    {
        EnsureNodeType(OmeAttributesParser.OmeNodeType.Plate, nameof(AsPlate));

        var attributes = RequireAttributes();
        var plateMeta  = OmeAttributesParser.ParsePlate(attributes);

        return new PlateNode(_rootGroup, plateMeta);
    }

    /// <summary>
    /// Returns the root as a WellNode.
    /// Useful when opening a well sub-path directly as the store root.
    /// </summary>
    public WellNode AsWell()
    {
        EnsureNodeType(OmeAttributesParser.OmeNodeType.Well, nameof(AsWell));

        var attributes = RequireAttributes();
        var wellMeta   = OmeAttributesParser.ParseWell(attributes);

        return new WellNode(_rootGroup, wellMeta);
    }

    /// <summary>
    /// Attempts to determine the root node type and return a general-purpose
    /// navigation entry point without needing to know the type in advance.
    /// Returns one of: MultiscaleNode, PlateNode, WellNode, LabelGroupNode.
    /// </summary>
    public OmeZarrNode OpenRoot()
    {
        var attributes = _rootGroup.Metadata.RawAttributes;

        return RootNodeType switch
        {
            OmeAttributesParser.OmeNodeType.Plate =>
                new PlateNode(_rootGroup, OmeAttributesParser.ParsePlate(attributes!.Value)),

            OmeAttributesParser.OmeNodeType.Well =>
                new WellNode(_rootGroup, OmeAttributesParser.ParseWell(attributes!.Value)),

            OmeAttributesParser.OmeNodeType.LabelGroup =>
                new LabelGroupNode(_rootGroup, OmeAttributesParser.ParseLabelGroup(attributes!.Value)),

            OmeAttributesParser.OmeNodeType.MultiscaleImage or
            OmeAttributesParser.OmeNodeType.LabelImage =>
                new MultiscaleNode(_rootGroup, OmeAttributesParser.ParseMultiscales(attributes!.Value)),

            _ => throw new InvalidOperationException(
                $"Cannot determine OME-Zarr node type. " +
                $"Root attributes do not contain a recognised OME-Zarr key " +
                $"(multiscales, plate, well, labels). " +
                $"Path: {_rootGroup.GroupPath}")
        };
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void EnsureNodeType(OmeAttributesParser.OmeNodeType expected, string callerName)
    {
        if (RootNodeType != expected)
        {
            var attributesHint = DescribeRawAttributes();

            throw new InvalidOperationException(
                $"{callerName} requires root node type '{expected}', " +
                $"but detected '{RootNodeType}'. " +
                $"NGFF version: {NgffVersion ?? "(none)"}. " +
                $"Root attributes: {attributesHint}. " +
                (RootNodeType == OmeAttributesParser.OmeNodeType.Unknown
                    ? "The root group attributes do not contain any recognised OME-Zarr key " +
                      "(multiscales, plate, well, labels) — check whether this is a " +
                      "bioformats2raw layout (navigate into sub-groups) or an unsupported " +
                      "metadata format."
                    : $"Use OpenRoot() for automatic dispatch, or " +
                      $"use the appropriate accessor (e.g. AsPlate() for plate data)."));
        }
    }

    /// <summary>
    /// Returns a short summary of the root attribute keys for diagnostics.
    /// </summary>
    private string DescribeRawAttributes()
    {
        var attrs = _rootGroup.Metadata.RawAttributes;
        if (attrs is null)
            return "(null — no attributes found)";

        try
        {
            var keys = new List<string>();
            foreach (var prop in attrs.Value.EnumerateObject())
                keys.Add(prop.Name);

            return keys.Count == 0
                ? "(empty object)"
                : $"{{ {string.Join(", ", keys)} }}";
        }
        catch
        {
            return "(unable to enumerate)";
        }
    }

    private System.Text.Json.JsonElement RequireAttributes()
    {
        if (_rootGroup.Metadata.RawAttributes is null)
            throw new InvalidOperationException(
                "Root group has no attributes. " +
                "This does not appear to be a valid OME-Zarr dataset.");

        return _rootGroup.Metadata.RawAttributes.Value;
    }

    // -------------------------------------------------------------------------
    // Disposal
    // -------------------------------------------------------------------------

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _store.DisposeAsync().ConfigureAwait(false);
    }
}
