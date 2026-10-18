using System.Text.Json;
using ZarrNET.Core.Zarr;
using ZarrNET.Core.Zarr.Store;

namespace ZarrNET;

/// <summary>
/// Converts individual Zarr v2 arrays to portable Zarr v3 arrays. Chunks are
/// decoded through the source codec pipeline and written through a v3 bytes
/// pipeline, so conversion does not depend on compressor compatibility.
/// </summary>
public static class ZarrV2ToV3Converter
{
    /// <summary>
    /// Copies a v2 array into a v3 array at <paramref name="destinationArrayPath"/>.
    /// The destination must be an empty store or an existing v3 root group.
    /// Output chunks use little-endian bytes with the source logical shape and
    /// chunk shape. Group hierarchy and arbitrary attributes are intentionally
    /// outside this array-level utility's scope.
    /// </summary>
    public static async Task ConvertArrayAsync(
        IZarrStore sourceStore,
        string sourceArrayPath,
        IZarrStore destinationStore,
        string destinationArrayPath,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(sourceStore);
        ArgumentNullException.ThrowIfNull(destinationStore);
        if (string.IsNullOrWhiteSpace(sourceArrayPath))
            throw new ArgumentException("A non-root source array path is required.", nameof(sourceArrayPath));
        if (string.IsNullOrWhiteSpace(destinationArrayPath))
            throw new ArgumentException("A non-root destination array path is required.", nameof(destinationArrayPath));

        var sourceRoot = await ZarrGroup.OpenRootAsync(sourceStore, ct).ConfigureAwait(false);
        var source = await sourceRoot.OpenArrayAsync(sourceArrayPath, ct).ConfigureAwait(false);
        if (source.Metadata.ZarrVersion != 2)
            throw new ArgumentException("The source array must use Zarr v2 metadata.", nameof(sourceArrayPath));

        await EnsureV3RootGroupAsync(destinationStore, ct).ConfigureAwait(false);
        await WriteV3ArrayMetadataAsync(destinationStore, destinationArrayPath, source.Metadata, ct).ConfigureAwait(false);

        var destinationRoot = await ZarrGroup.OpenRootAsync(destinationStore, ct).ConfigureAwait(false);
        var destination = await destinationRoot.OpenArrayAsync(destinationArrayPath, ct).ConfigureAwait(false);
        await foreach (var chunk in source.EnumerateChunksAsync(ct))
        {
            var decoded = await source.ReadChunkDecodedAsync(chunk, ct).ConfigureAwait(false);
            await destination.WriteChunkDecodedAsync(chunk, decoded, ct).ConfigureAwait(false);
        }
    }

    private static async Task EnsureV3RootGroupAsync(IZarrStore store, CancellationToken ct)
    {
        if (await store.ExistsAsync("zarr.json", ct).ConfigureAwait(false)) return;
        var root = JsonSerializer.SerializeToUtf8Bytes(new { zarr_format = 3, node_type = "group" });
        await store.WriteAsync("zarr.json", root, ct).ConfigureAwait(false);
    }

    private static async Task WriteV3ArrayMetadataAsync(
        IZarrStore store,
        string arrayPath,
        ZarrArrayMetadata source,
        CancellationToken ct)
    {
        var document = new
        {
            zarr_format = 3,
            node_type = "array",
            shape = source.Shape,
            data_type = source.DataType.TypeString,
            chunk_grid = new { name = "regular", configuration = new { chunk_shape = source.ChunkShape } },
            chunk_key_encoding = new { name = "default", configuration = new { separator = "/" } },
            fill_value = 0,
            dimension_names = source.DimensionNames,
            codecs = new[] { new { name = "bytes", configuration = new { endian = "little" } } }
        };
        var path = arrayPath.Trim('/');
        await store.WriteAsync($"{path}/zarr.json", JsonSerializer.SerializeToUtf8Bytes(document), ct).ConfigureAwait(false);
    }
}
