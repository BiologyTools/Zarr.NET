using Xunit;

namespace ZarrNET.Tests;

public sealed class BloscCodecTests
{
    [Fact]
    public async Task DecodeAsync_DecodesLz4BitShuffle_UInt16()
    {
        const string encoded =
            "AgEkAgAIAAAACAAARgAAABQAAAAcAAAAH6oBAGwfzAEAbB/wAQBsHwABAP//aVAAAAAAAA4AAAAfAAEA////6lAAAAAAAA==";
        var expected = new byte[2048];

        for (int element = 0; element < 1024; element++)
        {
            var value = (ushort)(element % 8);
            expected[element * 2] = (byte)value;
            expected[element * 2 + 1] = (byte)(value >> 8);
        }

        var actual = await new BloscCodec().DecodeAsync(Convert.FromBase64String(encoded));

        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task DecodeAsync_DecodesLz4BitShuffle_Byte()
    {
        const string encoded =
            "AgEkAQAEAAAABAAAOAAAABQAAAAgAAAAH6oBAGwfzAEAbB/wAQBsLwD/AgBsDwEA/+hQAAAAAAA=";
        var expected = Enumerable.Range(0, 1024)
            .Select(index => (byte)(index % 16))
            .ToArray();

        var actual = await new BloscCodec().DecodeAsync(Convert.FromBase64String(encoded));

        Assert.Equal(expected, actual);
    }
}
