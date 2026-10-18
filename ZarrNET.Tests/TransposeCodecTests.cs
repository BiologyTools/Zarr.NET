using Xunit;

namespace ZarrNET.Tests;

public sealed class TransposeCodecTests
{
    [Fact]
    public async Task EncodeAndDecode_PermuteChunkAxes()
    {
        var codec = new TransposeCodec(order: [1, 0], inputShape: [2, 3], elementSize: 1);
        var source = new byte[] { 0, 1, 2, 3, 4, 5 };

        var encoded = await codec.EncodeAsync(source);
        var decoded = await codec.DecodeAsync(encoded);

        Assert.Equal(new byte[] { 0, 3, 1, 4, 2, 5 }, encoded);
        Assert.Equal(source, decoded);
    }
}
