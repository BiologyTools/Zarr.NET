using System.Text;
using ZarrNET.Core.Zarr;
using ZarrNET.Core.Zarr.Store;
using Xunit;

namespace ZarrNET.Tests;

public sealed class ZarrV2ToV3ConverterTests
{
    [Fact]
    public async Task ConvertArrayAsync_CopiesDecodedV2ChunksIntoV3()
    {
        using var sourceWorkspace = new TestWorkspace();
        using var destinationWorkspace = new TestWorkspace();
        await using var sourceStore = new LocalFileSystemStore(sourceWorkspace.Path);
        await using var destinationStore = new LocalFileSystemStore(destinationWorkspace.Path);
        await sourceStore.WriteAsync(".zgroup", Encoding.UTF8.GetBytes("{\"zarr_format\":2}"));
        await sourceStore.WriteAsync("0/.zarray", Encoding.UTF8.GetBytes("""
            { "zarr_format": 2, "shape": [2, 3], "chunks": [2, 3],
              "dtype": "|u1", "compressor": null, "fill_value": 0,
              "order": "C", "filters": null, "dimension_separator": "." }
            """));
        await sourceStore.WriteAsync("0/0.0", new byte[] { 0, 1, 2, 3, 4, 5 });

        await ZarrV2ToV3Converter.ConvertArrayAsync(sourceStore, "0", destinationStore, "converted");

        var root = await ZarrGroup.OpenRootAsync(destinationStore);
        var converted = await root.OpenArrayAsync("converted");
        var result = await converted.ReadRegionAsync(regionStart: [0, 0], regionEnd: [2, 3]);

        Assert.Equal(3, converted.Metadata.ZarrVersion);
        Assert.Equal(new byte[] { 0, 1, 2, 3, 4, 5 }, result);
    }
}
