using System.Text;
using ZarrNET.Core.Zarr;
using ZarrNET.Core.Zarr.Store;
using Xunit;

namespace ZarrNET.Tests;

public sealed class ZarrV2CompatibilityTests
{
    [Fact]
    public async Task OpensAndReadsMinimalV2Store()
    {
        using var workspace = new TestWorkspace();
        await using var store = new LocalFileSystemStore(workspace.Path);
        await store.WriteAsync(".zgroup", Encoding.UTF8.GetBytes("{\"zarr_format\":2}"));
        await store.WriteAsync("0/.zarray", Encoding.UTF8.GetBytes("""
            { "zarr_format": 2, "shape": [2, 3], "chunks": [2, 3],
              "dtype": "|u1", "compressor": null, "fill_value": 0,
              "order": "C", "filters": null, "dimension_separator": "." }
            """));
        await store.WriteAsync("0/0.0", new byte[] { 0, 1, 2, 3, 4, 5 });

        var root = await ZarrGroup.OpenRootAsync(store);
        var array = await root.OpenArrayAsync("0");
        var result = await array.ReadRegionAsync(regionStart: [0, 0], regionEnd: [2, 3]);

        Assert.Equal(new byte[] { 0, 1, 2, 3, 4, 5 }, result);
        Assert.Equal(2, array.Metadata.ZarrVersion);
    }
}
