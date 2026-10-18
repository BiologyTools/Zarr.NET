using ZarrNET.Core;
using ZarrNET.Core.OmeZarr.Coordinates;
using Xunit;

namespace ZarrNET.Tests;

public sealed class OmeZarrWriterReaderTests
{
    [Fact]
    public async Task WriterAndReader_RoundTripNativePixelsAndPhysicalRegion()
    {
        using var workspace = new TestWorkspace();
        var path = System.IO.Path.Combine(workspace.Path, "roundtrip.zarr");
        var descriptor = new BioImageDescriptor(3, 2, new ZCT(1, 1, 1))
        {
            DataType = "uint8",
            Compression = ZarrCompression.None,
            PhysicalSizeY = 2,
            PhysicalSizeX = 3
        };
        var expected = new byte[] { 0, 1, 2, 3, 4, 5 };

        await using (var writer = await OmeZarrWriter.CreateAsync(path, descriptor))
            await writer.WritePixelDataAsync(expected);

        await using var reader = await OmeZarrReader.OpenAsync(path);
        var level = await reader.AsMultiscaleImage().OpenResolutionLevelAsync(0);
        var pixelResult = await level.ReadPixelRegionAsync(
            new PixelRegion(start: [0, 0, 0, 0, 0], end: [1, 1, 1, 2, 3]));
        var physicalResult = await level.ReadRegionAsync(
            new PhysicalROI(origin: [0, 0, 0, 0, 0], size: [1, 1, 1, 4, 9]));

        Assert.Equal(expected, pixelResult.Data);
        Assert.Equal(expected, physicalResult.Data);
        Assert.Equal(new long[] { 1, 1, 1, 2, 3 }, physicalResult.Shape);
        Assert.Equal("0.6", reader.NgffVersion);
    }
}
