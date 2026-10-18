using System.Net;
using System.Text;
using ZarrNET.Core.Zarr.Store;
using Xunit;

namespace ZarrNET.Tests;

public sealed class HttpConsolidatedMetadataTests
{
    [Fact]
    public async Task ListAsync_ListsV2ConsolidatedMetadataUnderPrefix()
    {
        using var client = new HttpClient(new StaticResponseHandler(new Dictionary<string, string>
        {
            [".zmetadata"] = """
                { "zarr_consolidated_format": 1, "metadata": {
                  "zarr.json": {}, "images/0/.zarray": {}, "images/0/.zattrs": {},
                  "images/1/.zarray": {}, "plate/.zgroup": {} } }
                """
        }));
        await using var store = new HttpZarrStore("https://example.test/v2.zarr", client);

        var keys = await store.ListAsync("images");

        Assert.Equal(new[] { "images/0/.zarray", "images/0/.zattrs", "images/1/.zarray" }, keys);
    }

    [Fact]
    public async Task ListAsync_UsesV3InlineConsolidatedMetadataWhenV2IndexIsAbsent()
    {
        using var client = new HttpClient(new StaticResponseHandler(new Dictionary<string, string>
        {
            ["zarr.json"] = """
                { "zarr_format": 3, "node_type": "group",
                  "consolidated_metadata": { "kind": "inline", "must_understand": false,
                    "metadata": { "labels/zarr.json": {}, "labels/nuclei/zarr.json": {} } } }
                """
        }));
        await using var store = new HttpZarrStore("https://example.test/v3.zarr", client);

        var keys = await store.ListAsync("labels");

        Assert.Equal(new[] { "labels/nuclei/zarr.json", "labels/zarr.json" }, keys);
    }

    private sealed class StaticResponseHandler : HttpMessageHandler
    {
        private readonly IReadOnlyDictionary<string, string> _responses;

        public StaticResponseHandler(IReadOnlyDictionary<string, string> responses) => _responses = responses;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var requestUrl = request.RequestUri?.AbsoluteUri
                ?? throw new InvalidOperationException("The test request has no URL.");
            var found = _responses.FirstOrDefault(pair => requestUrl.EndsWith(pair.Key, StringComparison.Ordinal));
            var response = found.Key is null
                ? new HttpResponseMessage(HttpStatusCode.NotFound)
                : new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(found.Value, Encoding.UTF8, "application/json")
                };
            return Task.FromResult(response);
        }
    }
}
