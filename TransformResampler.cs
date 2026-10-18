using ZarrNET.Core.OmeZarr.Metadata;
using ZarrNET.Core.Nodes;
using ZarrNET.Core.OmeZarr.Nodes;
using ZarrNET.Core.Zarr;

namespace ZarrNET.Core.OmeZarr.Coordinates;

/// <summary>Interpolation used while resampling an OME-Zarr image into a physical output grid.</summary>
public enum ImageResamplingInterpolation
{
    Nearest,
    Linear,
    CubicBspline
}

/// <summary>
/// Resamples image pixels through the closed-form OME-Zarr coordinate
/// transformation chain. The requested output grid is axis-aligned in the
/// image's physical coordinate system; every output pixel centre is mapped
/// back into source array-index space before it is sampled.
/// </summary>
public static class TransformResampler
{
    /// <summary>
    /// Reads <paramref name="source"/> into a physical output grid. Unlike
    /// <see cref="ResolutionLevelNode.ReadRegionAsync"/>, this produces a
    /// resampled grid for rotated, sheared, and axis-mapped coordinate systems.
    /// The result retains the source data type and uses the source axes.
    /// </summary>
    public static async Task<RegionResult> ResamplePhysicalRegionAsync(
        this ResolutionLevelNode source,
        PhysicalROI physicalRegion,
        long[] outputShape,
        ImageResamplingInterpolation interpolation = ImageResamplingInterpolation.Linear,
        CancellationToken ct = default)
        => await ResampleAsync(source, physicalRegion, outputShape, BuildTransforms(source), null, interpolation, ct)
            .ConfigureAwait(false);

    /// <summary>
    /// Resamples an image while resolving array-backed affine/rotation matrices
    /// and vector fields below <paramref name="transformContainer"/>.
    /// Pass the group that owns the transformations (for a root multiscale
    /// image, this is normally <see cref="OmeZarrReader.RootGroup"/>).
    /// </summary>
    public static async Task<RegionResult> ResamplePhysicalRegionAsync(
        this ResolutionLevelNode source,
        ZarrGroup transformContainer,
        PhysicalROI physicalRegion,
        long[] outputShape,
        ImageResamplingInterpolation interpolation = ImageResamplingInterpolation.Linear,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transformContainer);
        var transforms = await ResolveMatricesAsync(transformContainer, BuildTransforms(source), source.Rank, ct)
            .ConfigureAwait(false);
        var vectorFields = await ResolveVectorFieldsAsync(transformContainer, transforms, source.Rank, ct)
            .ConfigureAwait(false);
        return await ResampleAsync(source, physicalRegion, outputShape, transforms, vectorFields, interpolation, ct)
            .ConfigureAwait(false);
    }

    private static async Task<RegionResult> ResampleAsync(
        ResolutionLevelNode source,
        PhysicalROI physicalRegion,
        long[] outputShape,
        IReadOnlyList<CoordinateTransformation> transforms,
        IReadOnlyDictionary<CoordinateTransformation, VectorFieldTransformation>? vectorFields,
        ImageResamplingInterpolation interpolation,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(physicalRegion);
        ArgumentNullException.ThrowIfNull(outputShape);
        if (outputShape.Length != source.Rank || physicalRegion.Rank != source.Rank)
            throw new ArgumentException("Physical region and output shape must match the source array rank.");
        if (outputShape.Any(length => length <= 0))
            throw new ArgumentOutOfRangeException(nameof(outputShape), "Every output dimension must be positive.");
        if (source.Rank >= sizeof(int) * 8 - 1)
            throw new NotSupportedException("Resampling supports fewer than 31 dimensions.");
        if (interpolation is not ImageResamplingInterpolation.Nearest and not ImageResamplingInterpolation.Linear and not ImageResamplingInterpolation.CubicBspline)
            throw new ArgumentOutOfRangeException(nameof(interpolation));

        var dataType = ZarrDataType.Parse(source.DataType);
        if (dataType.IsComplex)
            throw new NotSupportedException("Transform-aware resampling currently supports real numeric data types only.");
        var elementCount = Product(outputShape);
        var outputBytes = checked((int)(elementCount * dataType.ElementSize));
        var output = new byte[outputBytes];
        var sourceCoordinates = new double[checked((int)elementCount)][];
        for (var flat = 0; flat < sourceCoordinates.Length; flat++)
        {
            ct.ThrowIfCancellationRequested();
            var index = UnravelIndex(flat, outputShape);
            var physicalPoint = index.Select((value, axis) =>
                physicalRegion.Origin[axis] + (value + 0.5) * physicalRegion.Size[axis] / outputShape[axis]).ToArray();
            sourceCoordinates[flat] = await TransformInverseAsync(transforms, vectorFields, physicalPoint, ct)
                .ConfigureAwait(false);
            if (sourceCoordinates[flat].Length != source.Rank)
                throw new NotSupportedException("Resampling requires a coordinate transformation with unchanged rank.");
        }

        var sourceRegion = SourceRegion(sourceCoordinates, source.Shape, interpolation);
        if (sourceRegion is null)
            return new RegionResult(output, outputShape, source.DataType, source.EffectiveAxes);

        var sourceData = await source.ReadPixelRegionAsync(sourceRegion, ct: ct).ConfigureAwait(false);
        var sourceValues = Decode(sourceData.Data, source.DataType);
        for (var flat = 0; flat < sourceCoordinates.Length; flat++)
        {
            ct.ThrowIfCancellationRequested();
            var value = Sample(sourceValues, sourceData.Shape, sourceRegion.Start,
                sourceCoordinates[flat], source.Shape, interpolation);
            Encode(output, flat * dataType.ElementSize, source.DataType, value);
        }
        return new RegionResult(output, outputShape, source.DataType, source.EffectiveAxes);
    }

    private static CoordinateTransformation[] BuildTransforms(ResolutionLevelNode source)
        => source.Dataset.CoordinateTransformations
            .Concat(source.Multiscale.CoordinateTransformations ?? Array.Empty<CoordinateTransformation>())
            .ToArray();

    private static async Task<IReadOnlyDictionary<CoordinateTransformation, VectorFieldTransformation>> ResolveVectorFieldsAsync(
        ZarrGroup container,
        IEnumerable<CoordinateTransformation> transforms,
        int rank,
        CancellationToken ct)
    {
        var fields = new Dictionary<CoordinateTransformation, VectorFieldTransformation>();
        foreach (var transform in transforms)
        {
            if (transform.Type is not ("coordinates" or "displacements")) continue;
            fields[transform] = await VectorFieldTransformResolver.ResolveAsync(container, transform, rank, rank, ct)
                .ConfigureAwait(false);
        }
        return fields;
    }

    private static async Task<CoordinateTransformation[]> ResolveMatricesAsync(
        ZarrGroup container,
        IEnumerable<CoordinateTransformation> transforms,
        int rank,
        CancellationToken ct)
    {
        var resolved = new List<CoordinateTransformation>();
        foreach (var transform in transforms)
            resolved.Add(await ResolveMatricesAsync(container, transform, rank, rank, ct).ConfigureAwait(false));
        return resolved.ToArray();
    }

    private static async Task<CoordinateTransformation> ResolveMatricesAsync(
        ZarrGroup container,
        CoordinateTransformation transform,
        int inputRank,
        int outputRank,
        CancellationToken ct)
    {
        if (transform.Type is "affine" or "rotation")
            return await TransformParameterResolver.ResolveMatrixAsync(container, transform, inputRank, outputRank, ct)
                .ConfigureAwait(false);

        if (transform.Type == "sequence")
        {
            var children = new List<CoordinateTransformation>();
            foreach (var child in transform.Transformations)
                children.Add(await ResolveMatricesAsync(container, child, inputRank, outputRank, ct).ConfigureAwait(false));
            return Copy(transform, transformations: children.ToArray());
        }

        if (transform.Type == "byDimension")
        {
            var parts = new List<ByDimensionTransformation>();
            foreach (var part in transform.ByDimensionTransformations)
            {
                var child = await ResolveMatricesAsync(container, part.Transformation,
                    part.InputAxes.Length, part.OutputAxes.Length, ct).ConfigureAwait(false);
                parts.Add(new ByDimensionTransformation
                {
                    InputAxes = part.InputAxes,
                    OutputAxes = part.OutputAxes,
                    Transformation = child
                });
            }
            return Copy(transform, byDimension: parts.ToArray());
        }

        if (transform.Type == "bijection")
            return Copy(transform,
                forward: transform.Forward is null ? null : await ResolveMatricesAsync(container, transform.Forward, inputRank, outputRank, ct).ConfigureAwait(false),
                inverse: transform.Inverse is null ? null : await ResolveMatricesAsync(container, transform.Inverse, outputRank, inputRank, ct).ConfigureAwait(false));
        return transform;
    }

    private static CoordinateTransformation Copy(
        CoordinateTransformation source,
        CoordinateTransformation[]? transformations = null,
        ByDimensionTransformation[]? byDimension = null,
        CoordinateTransformation? forward = null,
        CoordinateTransformation? inverse = null)
        => new()
        {
            Type = source.Type, Scale = source.Scale, Translation = source.Translation,
            Path = source.Path, Name = source.Name, Input = source.Input, Output = source.Output,
            Affine = source.Affine, Rotation = source.Rotation, MapAxis = source.MapAxis,
            CreatedOutputs = source.CreatedOutputs, DroppedInputs = source.DroppedInputs,
            Interpolation = source.Interpolation,
            Transformations = transformations ?? source.Transformations,
            ByDimensionTransformations = byDimension ?? source.ByDimensionTransformations,
            Forward = forward ?? source.Forward, Inverse = inverse ?? source.Inverse
        };

    private static PixelRegion? SourceRegion(
        IReadOnlyList<double[]> coordinates,
        IReadOnlyList<long> sourceShape,
        ImageResamplingInterpolation interpolation)
    {
        var start = new long[sourceShape.Count];
        var end = new long[sourceShape.Count];
        for (var axis = 0; axis < sourceShape.Count; axis++)
        {
            var min = coordinates.Min(point => point[axis]);
            var max = coordinates.Max(point => point[axis]);
            var padding = interpolation switch
            {
                ImageResamplingInterpolation.Linear => 1,
                ImageResamplingInterpolation.CubicBspline => 2,
                _ => 0
            };
            start[axis] = Math.Max(0, (long)Math.Floor(min) - padding);
            end[axis] = Math.Min(sourceShape[axis], (long)Math.Ceiling(max) + padding + 1);
            if (end[axis] <= start[axis]) return null;
        }
        return new PixelRegion(start, end);
    }

    private static double Sample(
        IReadOnlyList<double> values,
        IReadOnlyList<long> regionShape,
        IReadOnlyList<long> regionStart,
        IReadOnlyList<double> coordinate,
        IReadOnlyList<long> sourceShape,
        ImageResamplingInterpolation interpolation)
    {
        if (coordinate.Where((value, axis) => value < 0 || value > sourceShape[axis] - 1).Any())
            return 0;

        if (interpolation == ImageResamplingInterpolation.Nearest)
        {
            var index = coordinate.Select(value => (long)Math.Round(value, MidpointRounding.AwayFromZero)).ToArray();
            return values[FlatIndex(index.Zip(regionStart, (value, start) => value - start).ToArray(), regionShape)];
        }

        if (interpolation == ImageResamplingInterpolation.CubicBspline)
            return SampleCubicBspline(values, regionShape, regionStart, coordinate, sourceShape);

        var lower = coordinate.Select(value => (long)Math.Floor(value)).ToArray();
        var upper = lower.Select((value, axis) => Math.Min(value + 1, sourceShape[axis] - 1)).ToArray();
        var fraction = coordinate.Select((value, axis) => value - lower[axis]).ToArray();
        var result = 0.0;
        for (var corner = 0; corner < (1 << coordinate.Count); corner++)
        {
            var weight = 1.0;
            var index = new long[coordinate.Count];
            for (var axis = 0; axis < index.Length; axis++)
            {
                var useUpper = (corner & (1 << axis)) != 0;
                index[axis] = useUpper ? upper[axis] : lower[axis];
                weight *= useUpper ? fraction[axis] : 1 - fraction[axis];
            }
            result += weight * values[FlatIndex(index.Zip(regionStart, (value, start) => value - start).ToArray(), regionShape)];
        }
        return result;
    }

    private static double SampleCubicBspline(
        IReadOnlyList<double> values,
        IReadOnlyList<long> regionShape,
        IReadOnlyList<long> regionStart,
        IReadOnlyList<double> coordinate,
        IReadOnlyList<long> sourceShape)
    {
        var baseIndex = coordinate.Select(value => (long)Math.Floor(value)).ToArray();
        var combinations = (int)Math.Pow(4, coordinate.Count);
        var result = 0.0;
        for (var combination = 0; combination < combinations; combination++)
        {
            var encoded = combination;
            var weight = 1.0;
            var index = new long[coordinate.Count];
            for (var axis = 0; axis < index.Length; axis++)
            {
                var offset = encoded % 4 - 1;
                encoded /= 4;
                index[axis] = Math.Clamp(baseIndex[axis] + offset, 0, sourceShape[axis] - 1);
                weight *= CubicBsplineWeight(coordinate[axis] - (baseIndex[axis] + offset));
            }
            result += weight * values[FlatIndex(index.Zip(regionStart, (value, start) => value - start).ToArray(), regionShape)];
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

    private static async Task<double[]> TransformInverseAsync(
        IReadOnlyList<CoordinateTransformation> transforms,
        IReadOnlyDictionary<CoordinateTransformation, VectorFieldTransformation>? vectorFields,
        IReadOnlyList<double> point,
        CancellationToken ct)
    {
        var current = point.ToArray();
        foreach (var transform in transforms.Reverse())
            current = vectorFields is not null && vectorFields.TryGetValue(transform, out var field)
                ? await field.InverseTransformAsync(current, ct: ct).ConfigureAwait(false)
                : CoordinateTransformationEvaluator.TransformInverse(transform, current);
        return current;
    }

    private static long[] UnravelIndex(int flat, IReadOnlyList<long> shape)
    {
        var index = new long[shape.Count];
        for (var axis = shape.Count - 1; axis >= 0; axis--)
        {
            index[axis] = flat % shape[axis];
            flat /= checked((int)shape[axis]);
        }
        return index;
    }

    private static int FlatIndex(IReadOnlyList<long> index, IReadOnlyList<long> shape)
    {
        long flat = 0;
        for (var axis = 0; axis < shape.Count; axis++) flat = flat * shape[axis] + index[axis];
        return checked((int)flat);
    }

    private static long Product(IEnumerable<long> values)
        => values.Aggregate(1L, (product, value) => checked(product * value));

    private static double[] Decode(byte[] bytes, string dataType)
    {
        var type = ZarrDataType.Parse(dataType);
        var values = new double[bytes.Length / type.ElementSize];
        for (var index = 0; index < values.Length; index++)
        {
            var offset = index * type.ElementSize;
            values[index] = dataType switch
            {
                "bool" or "uint8" => bytes[offset], "int8" => unchecked((sbyte)bytes[offset]),
                "uint16" => BitConverter.ToUInt16(bytes, offset), "int16" => BitConverter.ToInt16(bytes, offset),
                "uint32" => BitConverter.ToUInt32(bytes, offset), "int32" => BitConverter.ToInt32(bytes, offset),
                "uint64" => BitConverter.ToUInt64(bytes, offset), "int64" => BitConverter.ToInt64(bytes, offset),
                "float32" => BitConverter.ToSingle(bytes, offset), "float64" => BitConverter.ToDouble(bytes, offset),
                _ => throw new NotSupportedException($"Data type '{dataType}' is not supported.")
            };
        }
        return values;
    }

    private static void Encode(byte[] bytes, int offset, string dataType, double value)
    {
        switch (dataType)
        {
            case "bool": bytes[offset] = value == 0 ? (byte)0 : (byte)1; break;
            case "uint8": bytes[offset] = (byte)Math.Clamp(Math.Round(value), 0, byte.MaxValue); break;
            case "int8": bytes[offset] = unchecked((byte)(sbyte)Math.Clamp(Math.Round(value), sbyte.MinValue, sbyte.MaxValue)); break;
            case "uint16": BitConverter.GetBytes((ushort)Math.Clamp(Math.Round(value), 0, ushort.MaxValue)).CopyTo(bytes, offset); break;
            case "int16": BitConverter.GetBytes((short)Math.Clamp(Math.Round(value), short.MinValue, short.MaxValue)).CopyTo(bytes, offset); break;
            case "uint32": BitConverter.GetBytes((uint)Math.Clamp(Math.Round(value), 0, uint.MaxValue)).CopyTo(bytes, offset); break;
            case "int32": BitConverter.GetBytes((int)Math.Clamp(Math.Round(value), int.MinValue, int.MaxValue)).CopyTo(bytes, offset); break;
            case "uint64": BitConverter.GetBytes((ulong)Math.Clamp(Math.Round(value), 0, ulong.MaxValue)).CopyTo(bytes, offset); break;
            case "int64": BitConverter.GetBytes((long)Math.Clamp(Math.Round(value), long.MinValue, long.MaxValue)).CopyTo(bytes, offset); break;
            case "float32": BitConverter.GetBytes((float)value).CopyTo(bytes, offset); break;
            case "float64": BitConverter.GetBytes(value).CopyTo(bytes, offset); break;
            default: throw new NotSupportedException($"Data type '{dataType}' is not supported.");
        }
    }
}
