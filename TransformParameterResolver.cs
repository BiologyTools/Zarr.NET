using ZarrNET.Core.OmeZarr.Metadata;
using ZarrNET.Core.Zarr;

namespace ZarrNET.Core.OmeZarr.Coordinates;

/// <summary>
/// Loads Zarr-array-backed affine and rotation parameters defined by OME-Zarr
/// 0.6. The returned transformation is self-contained and can be passed to
/// <see cref="CoordinateTransformationEvaluator"/>.
/// </summary>
public static class TransformParameterResolver
{
    public static async Task<CoordinateTransformation> ResolveMatrixAsync(
        ZarrGroup group,
        CoordinateTransformation transformation,
        int inputRank,
        int outputRank,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(group);
        ArgumentNullException.ThrowIfNull(transformation);

        if (transformation.Type is not ("affine" or "rotation"))
            throw new ArgumentException("Only affine and rotation transformations have matrix parameters.", nameof(transformation));

        if (transformation.Type == "affine" && transformation.Affine is not null ||
            transformation.Type == "rotation" && transformation.Rotation is not null)
            return transformation;

        if (string.IsNullOrWhiteSpace(transformation.Path))
            throw new InvalidOperationException($"{transformation.Type} transform has neither inline parameters nor a path.");

        var array = await group.OpenArrayAsync(transformation.Path, ct).ConfigureAwait(false);
        var expectedColumns = transformation.Type == "affine" ? inputRank + 1 : inputRank;
        if (array.Metadata.Rank != 2 || array.Metadata.Shape[0] != outputRank || array.Metadata.Shape[1] != expectedColumns)
            throw new InvalidOperationException(
                $"{transformation.Type} matrix at '{transformation.Path}' has shape " +
                $"[{string.Join(", ", array.Metadata.Shape)}], expected [{outputRank}, {expectedColumns}].");

        var bytes = await array.ReadRegionAsync(
            new long[] { 0, 0 }, array.Metadata.Shape, ct: ct).ConfigureAwait(false);
        var values = DecodeNumericValues(bytes, array.Metadata.DataType.TypeString);
        var matrix = new double[outputRank][];
        for (var row = 0; row < outputRank; row++)
            matrix[row] = values.Skip(row * expectedColumns).Take(expectedColumns).ToArray();

        return CopyWithMatrix(transformation, matrix);
    }

    private static CoordinateTransformation CopyWithMatrix(CoordinateTransformation source, double[][] matrix) => new()
    {
        Type = source.Type,
        Scale = source.Scale,
        Translation = source.Translation,
        Path = source.Path,
        Name = source.Name,
        Input = source.Input,
        Output = source.Output,
        Affine = source.Type == "affine" ? matrix : source.Affine,
        Rotation = source.Type == "rotation" ? matrix : source.Rotation,
        MapAxis = source.MapAxis,
        CreatedOutputs = source.CreatedOutputs,
        DroppedInputs = source.DroppedInputs,
        Interpolation = source.Interpolation,
        Transformations = source.Transformations,
        ByDimensionTransformations = source.ByDimensionTransformations,
        Forward = source.Forward,
        Inverse = source.Inverse
    };

    private static double[] DecodeNumericValues(byte[] bytes, string dataType)
    {
        var elementSize = ZarrDataType.Parse(dataType).ElementSize;
        if (ZarrDataType.Parse(dataType).IsComplex || bytes.Length % elementSize != 0)
            throw new NotSupportedException($"Matrix data type '{dataType}' is not a supported real numeric type.");

        var result = new double[bytes.Length / elementSize];
        for (var i = 0; i < result.Length; i++)
        {
            var offset = i * elementSize;
            result[i] = dataType switch
            {
                "int8" => unchecked((sbyte)bytes[offset]),
                "uint8" or "bool" => bytes[offset],
                "int16" => BitConverter.ToInt16(bytes, offset),
                "uint16" => BitConverter.ToUInt16(bytes, offset),
                "int32" => BitConverter.ToInt32(bytes, offset),
                "uint32" => BitConverter.ToUInt32(bytes, offset),
                "int64" => BitConverter.ToInt64(bytes, offset),
                "uint64" => BitConverter.ToUInt64(bytes, offset),
                "float32" => BitConverter.ToSingle(bytes, offset),
                "float64" => BitConverter.ToDouble(bytes, offset),
                _ => throw new NotSupportedException($"Matrix data type '{dataType}' is not supported.")
            };
        }
        return result;
    }
}
