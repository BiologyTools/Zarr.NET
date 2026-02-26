using OmeZarr.Core.OmeZarr.Coordinates;
using OmeZarr.Core.OmeZarr.Metadata;
using OmeZarr.Core.Zarr;

namespace OmeZarr.Core.OmeZarr.Nodes;

// =============================================================================
// Base node
// =============================================================================

/// <summary>
/// Base class for all OME-Zarr navigable nodes.
/// Carries the underlying ZarrGroup and the detected node type.
/// </summary>
public abstract class OmeZarrNode
{
    protected ZarrGroup Group { get; }

    public OmeAttributesParser.OmeNodeType NodeType { get; }
    public string Path => Group.GroupPath;

    protected OmeZarrNode(ZarrGroup group, OmeAttributesParser.OmeNodeType nodeType)
    {
        Group    = group;
        NodeType = nodeType;
    }
}

// =============================================================================
// Multiscale image node
// =============================================================================

/// <summary>
/// Represents a multiscale image group. Contains one or more resolution levels
/// and optionally a labels sub-group.
/// </summary>
public sealed class MultiscaleNode : OmeZarrNode
{
    public MultiscaleMetadata[] Multiscales { get; }

    internal MultiscaleNode(ZarrGroup group, MultiscaleMetadata[] multiscales)
        : base(group, OmeAttributesParser.OmeNodeType.MultiscaleImage)
    {
        Multiscales = multiscales;
    }

    // -------------------------------------------------------------------------
    // Resolution level access
    // -------------------------------------------------------------------------

    /// <summary>
    /// Opens a specific resolution level by index (0 = full resolution).
    /// </summary>
    public async Task<ResolutionLevelNode> OpenResolutionLevelAsync(
        int multiscaleIndex = 0,
        int datasetIndex    = 0,
        CancellationToken ct = default)
    {
        var multiscale = Multiscales[multiscaleIndex];
        var dataset    = multiscale.Datasets[datasetIndex];
        var array      = await Group.OpenArrayAsync(dataset.Path, ct).ConfigureAwait(false);

        return new ResolutionLevelNode(array, dataset, multiscale);
    }

    /// <summary>
    /// Opens all resolution levels for a multiscale entry, ordered from
    /// full resolution (index 0) to lowest resolution.
    /// </summary>
    public async Task<IReadOnlyList<ResolutionLevelNode>> OpenAllResolutionLevelsAsync(
        int multiscaleIndex = 0,
        CancellationToken ct = default)
    {
        var multiscale = Multiscales[multiscaleIndex];
        var levels     = new List<ResolutionLevelNode>(multiscale.Datasets.Length);

        foreach (var dataset in multiscale.Datasets)
        {
            var array = await Group.OpenArrayAsync(dataset.Path, ct).ConfigureAwait(false);
            levels.Add(new ResolutionLevelNode(array, dataset, multiscale));
        }

        return levels;
    }

    // -------------------------------------------------------------------------
    // Labels access
    // -------------------------------------------------------------------------

    public async Task<bool> HasLabelsAsync(CancellationToken ct = default)
        => await Group.HasChildAsync("labels", ct).ConfigureAwait(false);

    public async Task<LabelGroupNode> OpenLabelsAsync(CancellationToken ct = default)
    {
        var labelsGroup = await Group.OpenGroupAsync("labels", ct).ConfigureAwait(false);
        var attributes  = labelsGroup.Metadata.RawAttributes;

        if (attributes is null)
            throw new InvalidOperationException("Labels group has no attributes.");

        var labelGroupMeta = OmeAttributesParser.ParseLabelGroup(attributes.Value);

        return new LabelGroupNode(labelsGroup, labelGroupMeta);
    }
}

// =============================================================================
// Resolution level node
// =============================================================================

/// <summary>
/// A single resolution level within a multiscale image.
/// This is the leaf node that provides actual data access.
/// </summary>
public sealed class ResolutionLevelNode
{
    private readonly ZarrArray                    _array;
    private readonly CoordinateTransformService   _transformService;

    public DatasetMetadata    Dataset    { get; }
    public MultiscaleMetadata Multiscale { get; }

    public long[]  Shape     => _array.Metadata.Shape;
    public string  DataType  => _array.Metadata.DataType.TypeString;
    public string  Path      => _array.Metadata.DataType.TypeString;
    public int     Rank      => _array.Metadata.Rank;

    private AxisMetadata[]? _effectiveAxes;

    /// <summary>
    /// Returns the axes for this resolution level. When the multiscale metadata
    /// does not declare axes (OME-Zarr spec v0.1/v0.2), standard axis names are
    /// inferred from the array rank using the conventional suffix of (t, c, z, y, x).
    /// This ensures callers never receive an empty axes array against a ranked array.
    /// </summary>
    public AxisMetadata[] EffectiveAxes
    {
        get
        {
            if (_effectiveAxes is not null)
                return _effectiveAxes;

            if (Multiscale.Axes.Length > 0)
                return _effectiveAxes = Multiscale.Axes;

            // OME-Zarr v0.1/v0.2 files carry no explicit axes field.
            // The conventional axis order is the suffix of (t, c, z, y, x).
            var standardNames = new[] { "t", "c", "z", "y", "x" };
            var rank          = Rank;
            var offset        = standardNames.Length - rank;

            return _effectiveAxes = Enumerable.Range(0, rank)
                .Select(i => new AxisMetadata { Name = standardNames[offset + i] })
                .ToArray();
        }
    }

    internal ResolutionLevelNode(
        ZarrArray          array,
        DatasetMetadata    dataset,
        MultiscaleMetadata multiscale)
    {
        _array            = array;
        Dataset           = dataset;
        Multiscale        = multiscale;
        _transformService = new CoordinateTransformService();
    }

    // -------------------------------------------------------------------------
    // Data access
    // -------------------------------------------------------------------------

    /// <summary>
    /// Reads a region defined by a physical ROI.
    /// Returns raw bytes in the array's native data type.
    /// </summary>
    public async Task<RegionResult> ReadRegionAsync(
        PhysicalROI       roi,
        int?              maxParallelChunks = null,
        CancellationToken ct = default)
    {
        var pixelRegion = _transformService.PhysicalToPixel(roi, Dataset, Multiscale, Shape);

        var data = await _array.ReadRegionAsync(
            pixelRegion.Start,
            pixelRegion.End,
            maxParallelChunks,
            ct).ConfigureAwait(false);

        return new RegionResult(
            data,
            pixelRegion.Shape.Select(s => (long)s).ToArray(),
            _array.Metadata.DataType.TypeString,
            EffectiveAxes);
    }

    /// <summary>
    /// Reads a region defined directly by pixel coordinates.
    /// </summary>
    public async Task<RegionResult> ReadPixelRegionAsync(
        PixelRegion       region,
        int?              maxParallelChunks = null,
        CancellationToken ct = default)
    {
        var data = await _array.ReadRegionAsync(
            region.Start,
            region.End,
            maxParallelChunks,
            ct).ConfigureAwait(false);

        return new RegionResult(
            data,
            region.Shape.Select(s => (long)s).ToArray(),
            _array.Metadata.DataType.TypeString,
            EffectiveAxes);
    }

    /// <summary>
    /// Writes a region back from a RegionResult. Used for ROI save operations.
    /// </summary>
    public Task WriteRegionAsync(
        PixelRegion       region,
        byte[]            data,
        CancellationToken ct = default)
    {
        return _array.WriteRegionAsync(region.Start, region.End, data, ct);
    }

    /// <summary>
    /// Returns the physical extent of this entire resolution level.
    /// </summary>
    public PhysicalROI GetPhysicalExtent()
    {
        var rank      = Rank;
        var fullStart = new PixelRegion(new long[rank], Shape);
        return _transformService.PixelToPhysical(fullStart, Dataset, Multiscale);
    }

    /// <summary>
    /// Returns the physical pixel size (scale) at this resolution level.
    /// </summary>
    public double[] GetPixelSize()
        => _transformService.GetPixelSize(Dataset, Multiscale);
}

// =============================================================================
// Label nodes
// =============================================================================

/// <summary>
/// The "labels" sub-group of a multiscale image. Lists available label arrays.
/// </summary>
public sealed class LabelGroupNode : OmeZarrNode
{
    public LabelGroupMetadata LabelGroupMetadata { get; }

    internal LabelGroupNode(ZarrGroup group, LabelGroupMetadata labelGroupMetadata)
        : base(group, OmeAttributesParser.OmeNodeType.LabelGroup)
    {
        LabelGroupMetadata = labelGroupMetadata;
    }

    public IReadOnlyList<string> LabelNames => LabelGroupMetadata.Labels;

    public async Task<LabelNode> OpenLabelAsync(string labelName, CancellationToken ct = default)
    {
        var labelGroup = await Group.OpenGroupAsync(labelName, ct).ConfigureAwait(false);
        var attributes = labelGroup.Metadata.RawAttributes;

        if (attributes is null)
            throw new InvalidOperationException($"Label '{labelName}' group has no attributes.");

        var multiscales     = OmeAttributesParser.ParseMultiscales(attributes.Value);
        var imageLabelMeta  = OmeAttributesParser.ParseImageLabel(attributes.Value);

        return new LabelNode(labelGroup, multiscales, imageLabelMeta);
    }
}

/// <summary>
/// A single named label array with its own multiscale pyramid.
/// Semantically identical to a MultiscaleNode but carries additional
/// label-specific metadata (colors, properties, source link).
/// </summary>
public sealed class LabelNode : OmeZarrNode
{
    public MultiscaleMetadata[] Multiscales       { get; }
    public ImageLabelMetadata   ImageLabelMetadata { get; }

    internal LabelNode(
        ZarrGroup          group,
        MultiscaleMetadata[] multiscales,
        ImageLabelMetadata  imageLabelMetadata)
        : base(group, OmeAttributesParser.OmeNodeType.LabelImage)
    {
        Multiscales        = multiscales;
        ImageLabelMetadata = imageLabelMetadata;
    }

    public async Task<ResolutionLevelNode> OpenResolutionLevelAsync(
        int multiscaleIndex = 0,
        int datasetIndex    = 0,
        CancellationToken ct = default)
    {
        var multiscale = Multiscales[multiscaleIndex];
        var dataset    = multiscale.Datasets[datasetIndex];
        var array      = await Group.OpenArrayAsync(dataset.Path, ct).ConfigureAwait(false);

        return new ResolutionLevelNode(array, dataset, multiscale);
    }
}

// =============================================================================
// Bioformats2raw collection node
// =============================================================================

/// <summary>
/// A bioformats2raw.layout wrapper group containing one or more image series.
/// Analogous to a PlateNode — it's a container you navigate into.
///
/// The series are typically numbered sub-groups (0, 1, 2, ...).
/// If the OME sub-group provides explicit series paths, those are used;
/// otherwise series are discovered by probing consecutive numbered groups.
/// </summary>
public sealed class Bioformats2RawCollectionNode : OmeZarrNode
{
    public Bioformats2RawMetadata CollectionMetadata { get; }

    internal Bioformats2RawCollectionNode(
        ZarrGroup                group,
        Bioformats2RawMetadata   collectionMetadata)
        : base(group, OmeAttributesParser.OmeNodeType.Bioformats2RawCollection)
    {
        CollectionMetadata = collectionMetadata;
    }

    /// <summary>
    /// Number of known image series, or null if discovery is required.
    /// </summary>
    public int? SeriesCount => CollectionMetadata.SeriesPaths?.Length;

    // -------------------------------------------------------------------------
    // Series navigation
    // -------------------------------------------------------------------------

    /// <summary>
    /// Opens an image series by index. For single-series datasets this is
    /// just index 0. Returns a MultiscaleNode ready for resolution level access.
    /// </summary>
    public async Task<MultiscaleNode> OpenSeriesAsync(
        int               seriesIndex = 0,
        CancellationToken ct = default)
    {
        var seriesPath  = ResolveSeriesPath(seriesIndex);
        var seriesGroup = await Group.OpenGroupAsync(seriesPath, ct).ConfigureAwait(false);

        var attributes = seriesGroup.Metadata.RawAttributes
            ?? throw new InvalidOperationException(
                $"Series group at '{seriesPath}' has no attributes.");

        var multiscales = OmeAttributesParser.ParseMultiscales(attributes);

        return new MultiscaleNode(seriesGroup, multiscales);
    }

    /// <summary>
    /// Discovers all available series paths. Uses the OME series list if
    /// available, otherwise probes numbered sub-groups (0, 1, 2, ...).
    /// </summary>
    public async Task<IReadOnlyList<string>> DiscoverSeriesPathsAsync(
        CancellationToken ct = default)
    {
        if (CollectionMetadata.SeriesPaths is not null)
            return CollectionMetadata.SeriesPaths;

        var paths = new List<string>();
        for (int i = 0; ; i++)
        {
            if (!await Group.HasChildAsync(i.ToString(), ct).ConfigureAwait(false))
                break;
            paths.Add(i.ToString());
        }

        return paths;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private string ResolveSeriesPath(int seriesIndex)
    {
        if (CollectionMetadata.SeriesPaths is not null)
        {
            if (seriesIndex < 0 || seriesIndex >= CollectionMetadata.SeriesPaths.Length)
                throw new ArgumentOutOfRangeException(nameof(seriesIndex),
                    $"Series index {seriesIndex} is out of range. " +
                    $"Collection has {CollectionMetadata.SeriesPaths.Length} series.");

            return CollectionMetadata.SeriesPaths[seriesIndex];
        }

        if (seriesIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(seriesIndex),
                "Series index must be non-negative.");

        return seriesIndex.ToString();
    }
}

// =============================================================================
// HCS Plate and Well nodes
// =============================================================================

/// <summary>
/// Root node for an HCS (High-Content Screening) plate.
/// Provides navigation to rows, columns, and individual wells.
/// </summary>
public sealed class PlateNode : OmeZarrNode
{
    public PlateMetadata PlateMetadata { get; }

    internal PlateNode(ZarrGroup group, PlateMetadata plateMetadata)
        : base(group, OmeAttributesParser.OmeNodeType.Plate)
    {
        PlateMetadata = plateMetadata;
    }

    public IReadOnlyList<RowMetadata>    Rows    => PlateMetadata.Rows;
    public IReadOnlyList<ColumnMetadata> Columns => PlateMetadata.Columns;
    public IReadOnlyList<WellReference>  Wells   => PlateMetadata.Wells;

    /// <summary>Opens a well by its path (e.g. "A/1").</summary>
    public async Task<WellNode> OpenWellAsync(string wellPath, CancellationToken ct = default)
    {
        var wellGroup  = await Group.OpenGroupAsync(wellPath, ct).ConfigureAwait(false);
        var attributes = wellGroup.Metadata.RawAttributes;

        if (attributes is null)
            throw new InvalidOperationException($"Well at '{wellPath}' has no attributes.");

        var wellMeta = OmeAttributesParser.ParseWell(attributes.Value);

        return new WellNode(wellGroup, wellMeta);
    }

    /// <summary>Opens a well by its row name and column name (e.g. row "A", column "1").</summary>
    public Task<WellNode> OpenWellAsync(string rowName, string columnName, CancellationToken ct = default)
        => OpenWellAsync($"{rowName}/{columnName}", ct);
}

/// <summary>
/// A single well within an HCS plate.
/// Contains one or more fields (image acquisitions).
/// </summary>
public sealed class WellNode : OmeZarrNode
{
    public WellMetadata WellMetadata { get; }

    internal WellNode(ZarrGroup group, WellMetadata wellMetadata)
        : base(group, OmeAttributesParser.OmeNodeType.Well)
    {
        WellMetadata = wellMetadata;
    }

    public IReadOnlyList<FieldReference> Fields => WellMetadata.Images;

    /// <summary>Opens a field (acquisition) by its path within the well (e.g. "0").</summary>
    public async Task<FieldNode> OpenFieldAsync(string fieldPath, CancellationToken ct = default)
    {
        var fieldGroup = await Group.OpenGroupAsync(fieldPath, ct).ConfigureAwait(false);
        var attributes = fieldGroup.Metadata.RawAttributes;

        if (attributes is null)
            throw new InvalidOperationException($"Field at '{fieldPath}' has no attributes.");

        var multiscales = OmeAttributesParser.ParseMultiscales(attributes.Value);

        return new FieldNode(fieldGroup, multiscales);
    }

    /// <summary>Opens a field by index.</summary>
    public Task<FieldNode> OpenFieldAsync(int fieldIndex = 0, CancellationToken ct = default)
        => OpenFieldAsync(WellMetadata.Images[fieldIndex].Path, ct);
}

/// <summary>
/// A single field (image acquisition) within a well.
/// Behaves like a MultiscaleNode — has resolution levels and optionally labels.
/// </summary>
public sealed class FieldNode : OmeZarrNode
{
    public MultiscaleMetadata[] Multiscales { get; }

    internal FieldNode(ZarrGroup group, MultiscaleMetadata[] multiscales)
        : base(group, OmeAttributesParser.OmeNodeType.MultiscaleImage)
    {
        Multiscales = multiscales;
    }

    public async Task<ResolutionLevelNode> OpenResolutionLevelAsync(
        int multiscaleIndex = 0,
        int datasetIndex    = 0,
        CancellationToken ct = default)
    {
        var multiscale = Multiscales[multiscaleIndex];
        var dataset    = multiscale.Datasets[datasetIndex];
        var array      = await Group.OpenArrayAsync(dataset.Path, ct).ConfigureAwait(false);

        return new ResolutionLevelNode(array, dataset, multiscale);
    }

    public async Task<bool> HasLabelsAsync(CancellationToken ct = default)
        => await Group.HasChildAsync("labels", ct).ConfigureAwait(false);
}
