using ZarrNET.Core.OmeZarr.Metadata;
using ZarrNET.Core.Zarr;

namespace ZarrNET.Core.OmeZarr.Coordinates;

/// <summary>
/// Opens and evaluates OME-Zarr 0.6 <c>coordinates</c> and <c>displacements</c>
/// transformations backed by a regularly sampled vector-field multiscale group.
/// </summary>
public static class VectorFieldTransformResolver
{
    public static async Task<VectorFieldTransformation> ResolveAsync(
        ZarrGroup container,
        CoordinateTransformation transformation,
        int inputRank,
        int outputRank,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(transformation);
        if (transformation.Type is not ("coordinates" or "displacements"))
            throw new ArgumentException("Only coordinates and displacements transformations use vector fields.", nameof(transformation));
        if (string.IsNullOrWhiteSpace(transformation.Path))
            throw new InvalidOperationException($"{transformation.Type} transform is missing its vector-field path.");

        var fieldGroup = await container.OpenGroupAsync(transformation.Path, ct).ConfigureAwait(false);
        var attributes = fieldGroup.Metadata.RawAttributes
            ?? throw new InvalidOperationException($"Vector field '{transformation.Path}' has no OME-Zarr attributes.");
        var multiscale = OmeAttributesParser.ParseMultiscales(attributes).FirstOrDefault()
            ?? throw new InvalidOperationException($"Vector field '{transformation.Path}' has no multiscales metadata.");
        var dataset = multiscale.Datasets.FirstOrDefault()
            ?? throw new InvalidOperationException($"Vector field '{transformation.Path}' has no dataset.");
        var array = await fieldGroup.OpenArrayAsync(dataset.Path, ct).ConfigureAwait(false);
        var axes = multiscale.GetIntrinsicAxes();
        var vectorAxis = Array.FindIndex(axes, axis => axis.Type is "coordinate" or "displacement");
        if (vectorAxis < 0)
            throw new InvalidOperationException($"Vector field '{transformation.Path}' has no coordinate/displacement axis.");
        if (array.Metadata.Rank != inputRank + 1 || axes.Length != array.Metadata.Rank)
            throw new InvalidOperationException($"Vector field '{transformation.Path}' must have input rank + 1 dimensions.");

        var componentCount = checked((int)array.Metadata.Shape[vectorAxis]);
        var expectedComponents = transformation.Type == "coordinates" ? outputRank : inputRank;
        if (componentCount != expectedComponents)
            throw new InvalidOperationException(
                $"Vector field '{transformation.Path}' has {componentCount} components, expected {expectedComponents}.");

        var coordinateAxes = Enumerable.Range(0, array.Metadata.Rank)
            .Where(i => i != vectorAxis)
            .ToArray();
        var arrayToInput = new CoordinateTransformation
        {
            Type = "sequence",
            Transformations = dataset.CoordinateTransformations
                .Concat(multiscale.CoordinateTransformations ?? Array.Empty<CoordinateTransformation>())
                .ToArray()
        };

        return new VectorFieldTransformation(
            transformation, array, vectorAxis, coordinateAxes, arrayToInput,
            transformation.Interpolation ?? "linear");
    }
}

/// <summary>An opened vector-field transform. Instances retain the underlying Zarr array handle.</summary>
public sealed class VectorFieldTransformation
{
    private readonly CoordinateTransformation _transformation;
    private readonly ZarrArray _array;
    private readonly int _vectorAxis;
    private readonly int[] _coordinateAxes;
    private readonly CoordinateTransformation _arrayToInput;
    private readonly string _interpolation;

    internal VectorFieldTransformation(
        CoordinateTransformation transformation,
        ZarrArray array,
        int vectorAxis,
        int[] coordinateAxes,
        CoordinateTransformation arrayToInput,
        string interpolation)
    {
        _transformation = transformation;
        _array = array;
        _vectorAxis = vectorAxis;
        _coordinateAxes = coordinateAxes;
        _arrayToInput = arrayToInput;
        _interpolation = interpolation;
    }

    /// <summary>Maps an input-space point through this vector-field transform.</summary>
    public async Task<double[]> TransformAsync(IReadOnlyList<double> point, CancellationToken ct = default)
    {
        var arrayPoint = _arrayToInput.Transformations.Length == 0
            ? point.ToArray()
            : CoordinateTransformationEvaluator.TransformInverse(_arrayToInput, point);
        if (arrayPoint.Length != _coordinateAxes.Length)
            throw new InvalidOperationException("Vector-field coordinate transform returned an unexpected rank.");

        var vector = await SampleAsync(arrayPoint, ct).ConfigureAwait(false);
        return _transformation.Type == "coordinates"
            ? vector
            : point.Zip(vector, (coordinate, displacement) => coordinate + displacement).ToArray();
    }

    /// <summary>
    /// Numerically maps an output-space point back to vector-field input space.
    /// This is used for pull-based image resampling, where each output pixel
    /// must locate the source pixel that produced it.
    /// </summary>
    public async Task<double[]> InverseTransformAsync(
        IReadOnlyList<double> point,
        int maximumIterations = 12,
        double tolerance = 1e-6,
        CancellationToken ct = default)
    {
        if (point.Count != _coordinateAxes.Length)
            throw new ArgumentException("Vector-field inverse requires matching input and output ranks.", nameof(point));
        if (maximumIterations <= 0) throw new ArgumentOutOfRangeException(nameof(maximumIterations));
        if (tolerance <= 0) throw new ArgumentOutOfRangeException(nameof(tolerance));

        var estimate = point.ToArray();
        for (var iteration = 0; iteration < maximumIterations; iteration++)
        {
            ct.ThrowIfCancellationRequested();
            var mapped = await TransformAsync(estimate, ct).ConfigureAwait(false);
            var residual = mapped.Zip(point, (actual, target) => actual - target).ToArray();
            if (Math.Sqrt(residual.Sum(value => value * value)) <= tolerance)
                return estimate;

            var jacobian = new double[estimate.Length][];
            for (var axis = 0; axis < estimate.Length; axis++)
            {
                var step = 1e-3 * Math.Max(1.0, Math.Abs(estimate[axis]));
                var perturbed = estimate.ToArray();
                perturbed[axis] += step;
                try
                {
                    var shifted = await TransformAsync(perturbed, ct).ConfigureAwait(false);
                    jacobian[axis] = shifted.Zip(mapped, (next, current) => (next - current) / step).ToArray();
                }
                catch (ArgumentOutOfRangeException)
                {
                    perturbed[axis] = estimate[axis] - step;
                    var shifted = await TransformAsync(perturbed, ct).ConfigureAwait(false);
                    jacobian[axis] = mapped.Zip(shifted, (current, previous) => (current - previous) / step).ToArray();
                }
            }

            // The columns above are the numerical derivatives with respect to
            // input axes; solve J * delta = residual before Newton's update.
            var rows = Enumerable.Range(0, estimate.Length)
                .Select(row => Enumerable.Range(0, estimate.Length).Select(column => jacobian[column][row]).ToArray())
                .ToArray();
            var delta = SolveLinearSystem(rows, residual);
            for (var axis = 0; axis < estimate.Length; axis++) estimate[axis] -= delta[axis];
        }
        throw new InvalidOperationException(
            $"Vector-field inverse did not converge within {maximumIterations} iterations.");
    }

    private async Task<double[]> SampleAsync(double[] coordinate, CancellationToken ct)
    {
        if (_interpolation.Equals("bspline-cubic", StringComparison.OrdinalIgnoreCase))
            return await SampleCubicBsplineAsync(coordinate, ct).ConfigureAwait(false);

        var lower = new long[coordinate.Length];
        var upper = new long[coordinate.Length];
        var fractions = new double[coordinate.Length];
        for (var i = 0; i < coordinate.Length; i++)
        {
            var extent = _array.Metadata.Shape[_coordinateAxes[i]];
            if (coordinate[i] < 0 || coordinate[i] > extent - 1)
                throw new ArgumentOutOfRangeException(nameof(coordinate),
                    $"Vector-field coordinate {coordinate[i]} is outside [0, {extent - 1}] for axis {i}.");
            if (_interpolation.Equals("nearest", StringComparison.OrdinalIgnoreCase))
            {
                lower[i] = upper[i] = (long)Math.Round(coordinate[i], MidpointRounding.AwayFromZero);
                continue;
            }
            if (!_interpolation.Equals("linear", StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedException(
                    $"Vector-field interpolation '{_interpolation}' is not supported. " +
                    "Supported: nearest, linear, bspline-cubic.");

            lower[i] = (long)Math.Floor(coordinate[i]);
            upper[i] = Math.Min(lower[i] + 1, extent - 1);
            fractions[i] = coordinate[i] - lower[i];
        }

        var start = new long[_array.Metadata.Rank];
        var end = new long[_array.Metadata.Rank];
        for (var dim = 0; dim < start.Length; dim++)
        {
            var coordinateIndex = Array.IndexOf(_coordinateAxes, dim);
            start[dim] = coordinateIndex < 0 ? 0 : lower[coordinateIndex];
            end[dim] = coordinateIndex < 0 ? _array.Metadata.Shape[dim] : upper[coordinateIndex] + 1;
        }
        var data = await _array.ReadRegionAsync(start, end, ct: ct).ConfigureAwait(false);
        var values = DecodeNumericValues(data, _array.Metadata.DataType.TypeString);
        var regionShape = start.Zip(end, (s, e) => e - s).ToArray();
        var components = checked((int)regionShape[_vectorAxis]);
        var result = new double[components];
        var corners = 1 << coordinate.Length;
        for (var corner = 0; corner < corners; corner++)
        {
            var weight = 1.0;
            var local = new long[regionShape.Length];
            for (var i = 0; i < coordinate.Length; i++)
            {
                var chooseUpper = (corner & (1 << i)) != 0;
                local[_coordinateAxes[i]] = chooseUpper ? upper[i] - lower[i] : 0;
                weight *= chooseUpper ? fractions[i] : 1.0 - fractions[i];
            }
            for (var component = 0; component < components; component++)
            {
                local[_vectorAxis] = component;
                result[component] += weight * values[GetFlatIndex(local, regionShape)];
            }
        }
        return result;
    }

    private async Task<double[]> SampleCubicBsplineAsync(double[] coordinate, CancellationToken ct)
    {
        var samples = new long[coordinate.Length][];
        var weights = new double[coordinate.Length][];
        var start = new long[_array.Metadata.Rank];
        var end = new long[_array.Metadata.Rank];
        for (var dimension = 0; dimension < start.Length; dimension++)
        {
            var coordinateIndex = Array.IndexOf(_coordinateAxes, dimension);
            if (coordinateIndex < 0)
            {
                start[dimension] = 0;
                end[dimension] = _array.Metadata.Shape[dimension];
                continue;
            }

            var extent = _array.Metadata.Shape[dimension];
            if (coordinate[coordinateIndex] < 0 || coordinate[coordinateIndex] > extent - 1)
                throw new ArgumentOutOfRangeException(nameof(coordinate),
                    $"Vector-field coordinate {coordinate[coordinateIndex]} is outside [0, {extent - 1}] for axis {coordinateIndex}.");

            var baseIndex = (long)Math.Floor(coordinate[coordinateIndex]);
            samples[coordinateIndex] = Enumerable.Range(-1, 4)
                .Select(offset => Math.Clamp(baseIndex + offset, 0, extent - 1))
                .ToArray();
            weights[coordinateIndex] = Enumerable.Range(-1, 4)
                .Select(offset => CubicBsplineWeight(coordinate[coordinateIndex] - (baseIndex + offset)))
                .ToArray();
            start[dimension] = samples[coordinateIndex].Min();
            end[dimension] = samples[coordinateIndex].Max() + 1;
        }

        var data = await _array.ReadRegionAsync(start, end, ct: ct).ConfigureAwait(false);
        var values = DecodeNumericValues(data, _array.Metadata.DataType.TypeString);
        var regionShape = start.Zip(end, (s, e) => e - s).ToArray();
        var components = checked((int)regionShape[_vectorAxis]);
        var result = new double[components];
        var combinations = (int)Math.Pow(4, coordinate.Length);
        for (var combination = 0; combination < combinations; combination++)
        {
            var encoded = combination;
            var weight = 1.0;
            var local = new long[regionShape.Length];
            for (var axis = 0; axis < coordinate.Length; axis++)
            {
                var sample = encoded % 4;
                encoded /= 4;
                local[_coordinateAxes[axis]] = samples[axis][sample] - start[_coordinateAxes[axis]];
                weight *= weights[axis][sample];
            }
            for (var component = 0; component < components; component++)
            {
                local[_vectorAxis] = component;
                result[component] += weight * values[GetFlatIndex(local, regionShape)];
            }
        }
        return result;
    }

    private static double CubicBsplineWeight(double distance)
    {
        var x = Math.Abs(distance);
        return x switch
        {
            < 1.0 => (4.0 - 6.0 * x * x + 3.0 * x * x * x) / 6.0,
            < 2.0 => Math.Pow(2.0 - x, 3) / 6.0,
            _ => 0.0
        };
    }

    private static double[] SolveLinearSystem(double[][] matrix, double[] rightHandSide)
    {
        var rank = rightHandSide.Length;
        var augmented = new double[rank, rank + 1];
        for (var row = 0; row < rank; row++)
        {
            if (matrix[row].Length != rank) throw new InvalidOperationException("Vector-field Jacobian is not square.");
            for (var column = 0; column < rank; column++) augmented[row, column] = matrix[row][column];
            augmented[row, rank] = rightHandSide[row];
        }
        for (var pivot = 0; pivot < rank; pivot++)
        {
            var selected = Enumerable.Range(pivot, rank - pivot)
                .OrderByDescending(row => Math.Abs(augmented[row, pivot]))
                .First();
            if (Math.Abs(augmented[selected, pivot]) < 1e-12)
                throw new InvalidOperationException("Vector-field inverse has a singular numerical Jacobian.");
            for (var column = pivot; column <= rank; column++)
                (augmented[pivot, column], augmented[selected, column]) =
                    (augmented[selected, column], augmented[pivot, column]);
            var divisor = augmented[pivot, pivot];
            for (var column = pivot; column <= rank; column++) augmented[pivot, column] /= divisor;
            for (var row = 0; row < rank; row++) if (row != pivot)
            {
                var factor = augmented[row, pivot];
                for (var column = pivot; column <= rank; column++) augmented[row, column] -= factor * augmented[pivot, column];
            }
        }
        return Enumerable.Range(0, rank).Select(row => augmented[row, rank]).ToArray();
    }

    private static int GetFlatIndex(long[] index, long[] shape)
    {
        long flat = 0;
        for (var i = 0; i < shape.Length; i++) flat = flat * shape[i] + index[i];
        return checked((int)flat);
    }

    private static double[] DecodeNumericValues(byte[] bytes, string dataType)
    {
        var type = ZarrDataType.Parse(dataType);
        if (type.IsComplex || bytes.Length % type.ElementSize != 0)
            throw new NotSupportedException($"Vector field data type '{dataType}' is not a supported real numeric type.");
        var values = new double[bytes.Length / type.ElementSize];
        for (var i = 0; i < values.Length; i++)
        {
            var offset = i * type.ElementSize;
            values[i] = dataType switch
            {
                "int8" => unchecked((sbyte)bytes[offset]), "uint8" or "bool" => bytes[offset],
                "int16" => BitConverter.ToInt16(bytes, offset), "uint16" => BitConverter.ToUInt16(bytes, offset),
                "int32" => BitConverter.ToInt32(bytes, offset), "uint32" => BitConverter.ToUInt32(bytes, offset),
                "int64" => BitConverter.ToInt64(bytes, offset), "uint64" => BitConverter.ToUInt64(bytes, offset),
                "float32" => BitConverter.ToSingle(bytes, offset), "float64" => BitConverter.ToDouble(bytes, offset),
                _ => throw new NotSupportedException($"Vector field data type '{dataType}' is not supported.")
            };
        }
        return values;
    }
}
