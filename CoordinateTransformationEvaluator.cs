using ZarrNET.Core.OmeZarr.Metadata;

namespace ZarrNET.Core.OmeZarr.Coordinates;

/// <summary>
/// Evaluates the closed-form OME-Zarr 0.6 coordinate transformations for a
/// single point. Array-backed coordinate/displacement fields intentionally use
/// a separate sampled-data layer because evaluating them requires asynchronous
/// Zarr I/O and interpolation.
/// </summary>
public static class CoordinateTransformationEvaluator
{
    public static double[] TransformForward(CoordinateTransformation transformation, IReadOnlyList<double> point)
        => Transform(transformation, point, inverse: false);

    public static double[] TransformInverse(CoordinateTransformation transformation, IReadOnlyList<double> point)
        => Transform(transformation, point, inverse: true);

    private static double[] Transform(CoordinateTransformation t, IReadOnlyList<double> point, bool inverse)
    {
        return t.Type switch
        {
            "identity" => point.ToArray(),
            "scale" => Scale(t, point, inverse),
            "translation" => Translation(t, point, inverse),
            "mapAxis" => MapAxis(t, point, inverse),
            "projectAxis" => ProjectAxis(t, point, inverse),
            "affine" => Affine(t, point, inverse),
            "rotation" => Rotation(t, point, inverse),
            "sequence" => Sequence(t, point, inverse),
            "byDimension" => ByDimension(t, point, inverse),
            "bijection" => Bijection(t, point, inverse),
            "coordinates" or "displacements" => throw new NotSupportedException(
                $"'{t.Type}' requires a sampled vector-field provider and cannot be evaluated synchronously."),
            _ => throw new NotSupportedException($"Unsupported coordinate transformation type '{t.Type}'.")
        };
    }

    private static double[] Scale(CoordinateTransformation t, IReadOnlyList<double> point, bool inverse)
    {
        var scale = t.Scale ?? throw new InvalidOperationException("Scale transform is missing 'scale'.");
        EnsureRank(point, scale.Length, "scale");
        return point.Select((value, i) =>
        {
            if (inverse && scale[i] == 0)
                throw new InvalidOperationException("Cannot invert a scale transform with a zero component.");
            return inverse ? value / scale[i] : value * scale[i];
        }).ToArray();
    }

    private static double[] Translation(CoordinateTransformation t, IReadOnlyList<double> point, bool inverse)
    {
        var translation = t.Translation ?? throw new InvalidOperationException("Translation transform is missing 'translation'.");
        EnsureRank(point, translation.Length, "translation");
        return point.Select((value, i) => inverse ? value - translation[i] : value + translation[i]).ToArray();
    }

    private static double[] MapAxis(CoordinateTransformation t, IReadOnlyList<double> point, bool inverse)
    {
        var map = t.MapAxis ?? throw new InvalidOperationException("mapAxis transform is missing 'mapAxis'.");
        EnsureRank(point, map.Length, "mapAxis");
        if (map.Distinct().Count() != map.Length || map.Any(i => i < 0 || i >= map.Length))
            throw new InvalidOperationException("mapAxis must be a permutation of the input axes.");

        var result = new double[map.Length];
        if (!inverse)
            for (var output = 0; output < map.Length; output++) result[output] = point[map[output]];
        else
            for (var output = 0; output < map.Length; output++) result[map[output]] = point[output];
        return result;
    }

    private static double[] ProjectAxis(CoordinateTransformation t, IReadOnlyList<double> point, bool inverse)
    {
        var created = (t.CreatedOutputs ?? Array.Empty<int>()).Order().ToArray();
        var dropped = (t.DroppedInputs ?? Array.Empty<int>()).Order().ToArray();
        if (created.Distinct().Count() != created.Length || dropped.Distinct().Count() != dropped.Length)
            throw new InvalidOperationException("projectAxis indices must be unique.");

        if (inverse && dropped.Length > 0)
            throw new NotSupportedException("A projectAxis transform that drops input axes has no unique inverse.");

        if (!inverse)
        {
            var outputLength = point.Count - dropped.Length + created.Length;
            if (outputLength < 0) throw new InvalidOperationException("projectAxis drops more axes than the input contains.");
            var retained = point.Where((_, i) => !dropped.Contains(i)).ToArray();
            var result = new double[outputLength];
            var source = 0;
            for (var output = 0; output < outputLength; output++)
                result[output] = created.Contains(output) ? 0 : retained[source++];
            return result;
        }

        var resultInverse = new List<double>(point);
        for (var i = created.Length - 1; i >= 0; i--)
        {
            var index = created[i];
            if (index < 0 || index >= resultInverse.Count || resultInverse[index] != 0)
                throw new InvalidOperationException("Cannot invert projectAxis unless every created output axis is zero.");
            resultInverse.RemoveAt(index);
        }
        return resultInverse.ToArray();
    }

    private static double[] Affine(CoordinateTransformation t, IReadOnlyList<double> point, bool inverse)
    {
        var matrix = t.Affine ?? throw new NotSupportedException("Array-backed affine matrices are not yet available.");
        if (!inverse) return MultiplyAffine(matrix, point);
        if (matrix.Length != point.Count || matrix.Any(row => row.Length != point.Count + 1))
            throw new NotSupportedException("Only square affine matrices can be inverted.");
        return InvertAffine(matrix, point);
    }

    private static double[] Rotation(CoordinateTransformation t, IReadOnlyList<double> point, bool inverse)
    {
        var matrix = t.Rotation ?? throw new NotSupportedException("Array-backed rotation matrices are not yet available.");
        EnsureSquare(matrix, point.Count, "rotation");
        var result = new double[point.Count];
        for (var r = 0; r < result.Length; r++)
            for (var c = 0; c < result.Length; c++)
                result[r] += (inverse ? matrix[c][r] : matrix[r][c]) * point[c];
        return result;
    }

    private static double[] Sequence(CoordinateTransformation t, IReadOnlyList<double> point, bool inverse)
    {
        if (t.Transformations.Length == 0) throw new InvalidOperationException("sequence requires transformations.");
        IEnumerable<CoordinateTransformation> transforms = inverse ? t.Transformations.Reverse() : t.Transformations;
        return transforms.Aggregate(point.ToArray(), (current, child) => Transform(child, current, inverse));
    }

    private static double[] ByDimension(CoordinateTransformation t, IReadOnlyList<double> point, bool inverse)
    {
        var parts = t.ByDimensionTransformations;
        if (parts.Length == 0) throw new InvalidOperationException("byDimension requires transformations.");
        var outputLength = inverse
            ? parts.SelectMany(p => p.InputAxes).DefaultIfEmpty(-1).Max() + 1
            : parts.SelectMany(p => p.OutputAxes).DefaultIfEmpty(-1).Max() + 1;
        var result = new double[outputLength];
        foreach (var part in parts)
        {
            var sourceAxes = inverse ? part.OutputAxes : part.InputAxes;
            var destinationAxes = inverse ? part.InputAxes : part.OutputAxes;
            var source = sourceAxes.Select(axis => point[axis]).ToArray();
            var mapped = Transform(part.Transformation, source, inverse);
            if (mapped.Length != destinationAxes.Length)
                throw new InvalidOperationException("byDimension child transformation has an incompatible output rank.");
            for (var i = 0; i < mapped.Length; i++) result[destinationAxes[i]] = mapped[i];
        }
        return result;
    }

    private static double[] Bijection(CoordinateTransformation t, IReadOnlyList<double> point, bool inverse)
    {
        var selected = inverse ? t.Inverse : t.Forward;
        return selected is null
            ? throw new InvalidOperationException($"bijection is missing its {(inverse ? "inverse" : "forward")} transformation.")
            : Transform(selected, point, inverse: false);
    }

    private static double[] MultiplyAffine(double[][] matrix, IReadOnlyList<double> point)
    {
        if (matrix.Length == 0 || matrix.Any(row => row.Length != point.Count + 1))
            throw new InvalidOperationException("Affine matrix must have M rows of N+1 values.");
        return matrix.Select(row => row.Take(point.Count).Select((value, i) => value * point[i]).Sum() + row[^1]).ToArray();
    }

    private static double[] InvertAffine(double[][] matrix, IReadOnlyList<double> point)
    {
        var n = point.Count;
        var augmented = new double[n, 2 * n + 1];
        for (var r = 0; r < n; r++)
        {
            for (var c = 0; c < n; c++) augmented[r, c] = matrix[r][c];
            augmented[r, n + r] = 1;
            augmented[r, 2 * n] = point[r] - matrix[r][n];
        }
        for (var pivot = 0; pivot < n; pivot++)
        {
            var row = Enumerable.Range(pivot, n - pivot)
                .OrderByDescending(r => Math.Abs(augmented[r, pivot]))
                .First();
            if (Math.Abs(augmented[row, pivot]) < 1e-12) throw new InvalidOperationException("Affine matrix is singular.");
            for (var c = pivot; c < 2 * n + 1; c++) (augmented[pivot, c], augmented[row, c]) = (augmented[row, c], augmented[pivot, c]);
            var divisor = augmented[pivot, pivot];
            for (var c = pivot; c < 2 * n + 1; c++) augmented[pivot, c] /= divisor;
            for (var r = 0; r < n; r++) if (r != pivot)
            {
                var factor = augmented[r, pivot];
                for (var c = pivot; c < 2 * n + 1; c++) augmented[r, c] -= factor * augmented[pivot, c];
            }
        }
        return Enumerable.Range(0, n).Select(r => augmented[r, 2 * n]).ToArray();
    }

    private static void EnsureRank(IReadOnlyList<double> point, int rank, string type)
    {
        if (point.Count != rank) throw new ArgumentException($"{type} requires rank {rank}, got {point.Count}.", nameof(point));
    }

    private static void EnsureSquare(double[][] matrix, int rank, string type)
    {
        if (matrix.Length != rank || matrix.Any(row => row.Length != rank))
            throw new InvalidOperationException($"{type} matrix must be {rank}x{rank}.");
    }
}
