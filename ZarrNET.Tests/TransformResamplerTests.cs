using ZarrNET.Core;
using ZarrNET.Core.OmeZarr.Coordinates;
using ZarrNET.Core.OmeZarr.Metadata;
using Xunit;

namespace ZarrNET.Tests;

public sealed class TransformResamplerTests
{
    [Fact]
    public async Task Resampler_UsesRequestedPhysicalGrid()
    {
        using var workspace = new TestWorkspace();
        var path = System.IO.Path.Combine(workspace.Path, "resample.zarr");
        var expected = new byte[] { 0, 1, 2, 3, 4, 5 };
        var descriptor = new BioImageDescriptor(3, 2, new ZCT(1, 1, 1))
        {
            DataType = "uint8",
            Compression = ZarrCompression.None
        };

        await using (var writer = await OmeZarrWriter.CreateAsync(path, descriptor))
            await writer.WritePixelDataAsync(expected);

        await using var reader = await OmeZarrReader.OpenAsync(path);
        var level = await reader.AsMultiscaleImage().OpenResolutionLevelAsync(0);
        var output = await level.ResamplePhysicalRegionAsync(
            new PhysicalROI(origin: [-0.5, -0.5, -0.5, -0.5, -0.5], size: [1, 1, 1, 2, 3]),
            outputShape: [1, 1, 1, 2, 3],
            interpolation: ImageResamplingInterpolation.Linear);

        Assert.Equal(expected, output.Data);
        Assert.Equal(new long[] { 1, 1, 1, 2, 3 }, output.Shape);
    }

    [Fact]
    public void PhysicalRoi_EnclosesRotatedSourceRegion()
    {
        var rotation = new CoordinateTransformation
        {
            Type = "rotation",
            Rotation = [[0, -1], [1, 0]]
        };
        var dataset = new DatasetMetadata { CoordinateTransformations = [rotation] };
        var multiscale = new MultiscaleMetadata
        {
            Axes = [new AxisMetadata { Name = "y" }, new AxisMetadata { Name = "x" }]
        };
        var service = new CoordinateTransformService();

        var pixels = service.PhysicalToPixel(
            new PhysicalROI(origin: [-10, 0], size: [10, 10]),
            dataset,
            multiscale,
            arrayShape: [10, 10]);
        var roundTrip = service.PixelToPhysical(pixels, dataset, multiscale);

        Assert.Equal(new long[] { 0, 0 }, pixels.Start);
        Assert.Equal(new long[] { 10, 10 }, pixels.End);
        Assert.Equal(new double[] { -10, 0 }, roundTrip.Origin);
        Assert.Equal(new double[] { 10, 10 }, roundTrip.Size);
    }
}
