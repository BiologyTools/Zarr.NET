namespace ZarrNET;

/// <summary>
/// Zarr v3 <c>transpose</c> array-to-array codec. It permutes a decoded
/// full-chunk buffer before it reaches the bytes codec, or reverses that
/// permutation while decoding.
/// </summary>
public sealed class TransposeCodec : IZarrCodec
{
    private readonly int[] _order;
    private readonly int[] _inputShape;
    private readonly int[] _outputShape;
    private readonly int _elementSize;

    public string Name => "transpose";

    public TransposeCodec(int[] order, int[] inputShape, int elementSize)
    {
        ArgumentNullException.ThrowIfNull(order);
        ArgumentNullException.ThrowIfNull(inputShape);
        if (order.Length != inputShape.Length || order.Order().Distinct().Count() != order.Length ||
            order.Any(axis => axis < 0 || axis >= inputShape.Length))
            throw new ArgumentException("Transpose order must be a permutation of every chunk axis.", nameof(order));
        if (inputShape.Any(size => size <= 0))
            throw new ArgumentOutOfRangeException(nameof(inputShape), "Chunk dimensions must be positive.");
        if (elementSize <= 0) throw new ArgumentOutOfRangeException(nameof(elementSize));

        _order = order.ToArray();
        _inputShape = inputShape.ToArray();
        _outputShape = order.Select(axis => inputShape[axis]).ToArray();
        _elementSize = elementSize;
    }

    public Task<byte[]> EncodeAsync(byte[] input, CancellationToken ct = default)
        => Task.FromResult(Transpose(input, decode: false, ct));

    public Task<byte[]> DecodeAsync(byte[] input, CancellationToken ct = default)
        => Task.FromResult(Transpose(input, decode: true, ct));

    private byte[] Transpose(byte[] input, bool decode, CancellationToken ct)
    {
        var expectedLength = checked(_inputShape.Aggregate(1L, (count, size) => count * size) * _elementSize);
        if (input.Length != expectedLength)
            throw new InvalidOperationException(
                $"Transpose codec expected {expectedLength} bytes for chunk shape [{string.Join(", ", _inputShape)}], got {input.Length}.");

        var output = new byte[input.Length];
        var index = new int[_inputShape.Length];
        for (var inputFlat = 0; inputFlat < input.Length / _elementSize; inputFlat++)
        {
            ct.ThrowIfCancellationRequested();
            Unravel(inputFlat, _inputShape, index);
            var transposedIndex = _order.Select(axis => index[axis]).ToArray();
            var outputFlat = FlatIndex(transposedIndex, _outputShape);
            var sourceOffset = (decode ? outputFlat : inputFlat) * _elementSize;
            var destinationOffset = (decode ? inputFlat : outputFlat) * _elementSize;
            Buffer.BlockCopy(input, sourceOffset, output, destinationOffset, _elementSize);
        }
        return output;
    }

    private static void Unravel(int flat, IReadOnlyList<int> shape, int[] index)
    {
        for (var axis = shape.Count - 1; axis >= 0; axis--)
        {
            index[axis] = flat % shape[axis];
            flat /= shape[axis];
        }
    }

    private static int FlatIndex(IReadOnlyList<int> index, IReadOnlyList<int> shape)
    {
        var flat = 0;
        for (var axis = 0; axis < shape.Count; axis++) flat = flat * shape[axis] + index[axis];
        return flat;
    }
}
