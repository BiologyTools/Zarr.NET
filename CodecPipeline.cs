namespace OmeZarr.Core.Zarr.Codecs;

/// <summary>
/// Applies an ordered list of codecs as a pipeline.
///
/// Zarr v3 codec pipeline:
///   Decode: last codec → ... → first codec   (compressed bytes → array bytes)
///   Encode: first codec → ... → last codec   (array bytes → compressed bytes)
///
/// The pipeline also resolves byte-order swapping for the BytesCodec based on
/// the data type element size from ZarrArrayMetadata.
/// </summary>
public sealed class CodecPipeline
{
    private readonly IReadOnlyList<IZarrCodec> _codecs;
    private readonly int _elementSize;

    public CodecPipeline(IReadOnlyList<IZarrCodec> codecs, int elementSize)
    {
        _codecs      = codecs;
        _elementSize = elementSize;
    }

    // -------------------------------------------------------------------------
    // Pipeline execution
    // -------------------------------------------------------------------------

    public async Task<byte[]> DecodeAsync(byte[] input, CancellationToken ct = default)
    {
        var data = input;

        // Decode passes through codecs in reverse order
        for (int i = _codecs.Count - 1; i >= 0; i--)
        {
            data = await ApplyDecodeStepAsync(_codecs[i], data, ct).ConfigureAwait(false);
        }

        return data;
    }

    public async Task<byte[]> EncodeAsync(byte[] input, CancellationToken ct = default)
    {
        var data = input;

        foreach (var codec in _codecs)
        {
            data = await ApplyEncodeStepAsync(codec, data, ct).ConfigureAwait(false);
        }

        return data;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Task<byte[]> ApplyDecodeStepAsync(IZarrCodec codec, byte[] data, CancellationToken ct)
    {
        // BytesCodec needs element size to handle endianness correctly
        if (codec is BytesCodec bytesCodec)
            return bytesCodec.DecodeWithElementSizeAsync(data, _elementSize, ct);

        return codec.DecodeAsync(data, ct);
    }

    private Task<byte[]> ApplyEncodeStepAsync(IZarrCodec codec, byte[] data, CancellationToken ct)
    {
        if (codec is BytesCodec bytesCodec)
            return bytesCodec.EncodeWithElementSizeAsync(data, _elementSize, ct);

        return codec.EncodeAsync(data, ct);
    }
}
