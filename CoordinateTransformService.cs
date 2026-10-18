using ZarrNET.Core.OmeZarr.Metadata;

namespace ZarrNET.Core.OmeZarr.Coordinates;

/// <summary>
/// Converts between physical coordinate space (ROI in microns, etc.) and
/// array index space (PixelRegion) using the coordinate transformations
/// declared in OME-Zarr metadata.
///
/// OME-Zarr transform semantics map array index coordinates to physical
/// coordinates. For an axis-aligned ROI, all corners are transformed and the
/// returned pixel region is the smallest axis-aligned array-space enclosure.
///
/// Two levels of transforms are composed:
///   1. Per-dataset transforms (on each DatasetMetadata)
///   2. Top-level multiscale transforms (on MultiscaleMetadata itself)
///
/// Per-dataset transforms are applied first, then top-level transforms.
/// </summary>
public sealed class CoordinateTransformService
{
    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /// <summary>
    /// Converts a physical ROI to a pixel region for a specific resolution level.
    /// The pixel region is clamped to the array bounds and snapped to whole pixels.
    /// Rotated or sheared ROIs return an enclosing source rectangle; this method
    /// does not resample pixels into a rotated output grid.
    /// </summary>
    public PixelRegion PhysicalToPixel(
        PhysicalROI         roi,
        DatasetMetadata     dataset,
        MultiscaleMetadata  multiscale,
        long[]              arrayShape)
    {
        var composedTransforms = ComposeTransforms(dataset, multiscale);
        EnsureRank(arrayShape, roi.Rank, nameof(arrayShape));

        // A transformed axis-aligned physical ROI is not necessarily
        // axis-aligned in array space (for example after a rotation). Map all
        // corners back to index space and read the minimal enclosing box.
        var indexCorners = EnumerateCorners(roi.Origin, roi.End)
            .Select(corner => TransformInverse(composedTransforms, corner))
            .ToArray();
        EnsureRank(indexCorners, arrayShape.Length, "inverse transformation output");

        var (minimum, maximum) = Bounds(indexCorners);
        var clampedStart = ClampAndFloor(minimum, arrayShape);
        var clampedEnd   = ClampAndCeil(maximum, arrayShape);

        EnsureMinimumRegion(clampedStart, clampedEnd, arrayShape);

        return new PixelRegion(clampedStart, clampedEnd);
    }

    /// <summary>
    /// Converts a pixel region back to an axis-aligned physical bounding ROI for
    /// a specific resolution level. This is the exact extent for non-rotating
    /// transforms and an enclosing box for rotated/sheared transforms.
    /// </summary>
    public PhysicalROI PixelToPhysical(
        PixelRegion        region,
        DatasetMetadata    dataset,
        MultiscaleMetadata multiscale)
    {
        var composedTransforms = ComposeTransforms(dataset, multiscale);
        var physicalCorners = EnumerateCorners(
                region.Start.Select(value => (double)value).ToArray(),
                region.End.Select(value => (double)value).ToArray())
            .Select(corner => TransformForward(composedTransforms, corner))
            .ToArray();
        EnsureRank(physicalCorners, region.Rank, "forward transformation output");

        var (origin, end) = Bounds(physicalCorners);
        return new PhysicalROI(origin, origin.Zip(end, (start, finish) => finish - start).ToArray());
    }

    /// <summary>
    /// Returns the physical pixel size (scale) for a given dataset.
    /// Useful for resolution level selection — pick the level closest to
    /// the desired physical resolution.
    /// </summary>
    public double[] GetPixelSize(DatasetMetadata dataset, MultiscaleMetadata multiscale)
    {
        var rank = multiscale.GetIntrinsicAxes().Length;
        if (rank == 0) return Array.Empty<double>();

        var composedTransforms = ComposeTransforms(dataset, multiscale);
        var origin = TransformForward(composedTransforms, new double[rank]);
        var size = new double[rank];
        for (var axis = 0; axis < rank; axis++)
        {
            var adjacent = new double[rank];
            adjacent[axis] = 1;
            var transformed = TransformForward(composedTransforms, adjacent);
            if (transformed.Length != origin.Length)
                throw new NotSupportedException("Pixel size is undefined for a rank-changing transformation.");
            size[axis] = Math.Sqrt(transformed.Zip(origin, (a, b) => (a - b) * (a - b)).Sum());
        }
        return size;
    }

    // -------------------------------------------------------------------------
    // Transform composition
    // -------------------------------------------------------------------------

    /// <summary>
    /// Composes per-dataset transforms with top-level multiscale transforms
    /// into an ordered list to apply sequentially.
    /// Dataset transforms come first per the OME-Zarr spec.
    /// </summary>
    private static IReadOnlyList<CoordinateTransformation> ComposeTransforms(
        DatasetMetadata    dataset,
        MultiscaleMetadata multiscale)
    {
        var transforms = new List<CoordinateTransformation>();

        transforms.AddRange(dataset.CoordinateTransformations);

        if (multiscale.CoordinateTransformations is not null)
            transforms.AddRange(multiscale.CoordinateTransformations);

        return transforms;
    }

    private static double[] TransformForward(
        IReadOnlyList<CoordinateTransformation> transforms,
        IReadOnlyList<double> point)
        => transforms.Aggregate(point.ToArray(),
            (current, transformation) => CoordinateTransformationEvaluator.TransformForward(transformation, current));

    private static double[] TransformInverse(
        IReadOnlyList<CoordinateTransformation> transforms,
        IReadOnlyList<double> point)
        => transforms.Reverse().Aggregate(point.ToArray(),
            (current, transformation) => CoordinateTransformationEvaluator.TransformInverse(transformation, current));

    private static IEnumerable<double[]> EnumerateCorners(IReadOnlyList<double> start, IReadOnlyList<double> end)
    {
        EnsureRank(start, end.Count, "ROI end");
        if (start.Count >= sizeof(int) * 8 - 1)
            throw new NotSupportedException("ROI corner enumeration supports fewer than 31 dimensions.");
        for (var mask = 0; mask < (1 << start.Count); mask++)
            yield return Enumerable.Range(0, start.Count)
                .Select(axis => (mask & (1 << axis)) == 0 ? start[axis] : end[axis])
                .ToArray();
    }

    private static (double[] Minimum, double[] Maximum) Bounds(IReadOnlyList<double[]> points)
    {
        if (points.Count == 0) throw new ArgumentException("At least one point is required.", nameof(points));
        var rank = points[0].Length;
        EnsureRank(points, rank, "transformation output");
        return (Enumerable.Range(0, rank).Select(axis => points.Min(point => point[axis])).ToArray(),
                Enumerable.Range(0, rank).Select(axis => points.Max(point => point[axis])).ToArray());
    }

    private static void EnsureRank(IReadOnlyList<double[]> points, int expected, string name)
    {
        if (points.Any(point => point.Length != expected))
            throw new NotSupportedException($"{name} rank does not match the array rank.");
    }

    private static void EnsureRank(IReadOnlyList<long> values, int expected, string name)
    {
        if (values.Count != expected)
            throw new ArgumentException($"{name} rank does not match the ROI rank.", name);
    }

    private static void EnsureRank(IReadOnlyList<double> values, int expected, string name)
    {
        if (values.Count != expected)
            throw new ArgumentException($"{name} rank does not match the ROI rank.", name);
    }

    // -------------------------------------------------------------------------
    // Clamping and snapping helpers
    // -------------------------------------------------------------------------

    private static long[] ClampAndFloor(double[] values, long[] shape)
    {
        var result = new long[values.Length];
        for (int d = 0; d < values.Length; d++)
            result[d] = (long)Math.Max(0, Math.Floor(values[d]));
        return result;
    }

    private static long[] ClampAndCeil(double[] values, long[] shape)
    {
        var result = new long[values.Length];
        for (int d = 0; d < values.Length; d++)
            result[d] = Math.Min(shape[d], (long)Math.Ceiling(values[d]));
        return result;
    }

    private static void EnsureMinimumRegion(long[] start, long[] end, long[] shape)
    {
        for (int d = 0; d < start.Length; d++)
        {
            if (end[d] <= start[d])
            {
                // Expand by one pixel in this axis, clamped to array bounds
                end[d] = Math.Min(start[d] + 1, shape[d]);

                if (end[d] <= start[d])
                    start[d] = Math.Max(0, end[d] - 1);
            }
        }
    }
}
