namespace ZarrNET.Core.Zarr.Store;

/// <summary>
/// Pure key/value store abstraction. Keys are forward-slash-separated paths
/// relative to the store root (e.g. "0/0.0.0", "labels/nuclei/zarr.json").
/// No Zarr semantics live here — this is infrastructure only.
/// </summary>
public interface IZarrStore : IAsyncDisposable
{
    /// <summary>Reads raw bytes for a key. Returns null if the key does not exist.</summary>
    Task<byte[]?> ReadAsync(string key, CancellationToken ct = default);

    /// <summary>Reads raw bytes into caller-owned memory. Returns null if the key does not exist.</summary>
    async Task<int?> ReadAsync(string key, Memory<byte> destination, CancellationToken ct = default)
    {
        var data = await ReadAsync(key, ct).ConfigureAwait(false);
        if (data is null)
            return null;

        if (data.Length > destination.Length)
            throw new ArgumentException(
                $"Destination has {destination.Length} bytes, but key '{key}' contains {data.Length} bytes.",
                nameof(destination));

        data.CopyTo(destination);
        return data.Length;
    }

    /// <summary>Writes raw bytes to a key, creating it if it does not exist.</summary>
    Task WriteAsync(string key, byte[] data, CancellationToken ct = default);

    /// <summary>
    /// Writes raw bytes to a key, creating it if it does not exist. Implementations
    /// must consume the memory before the returned task completes.
    /// </summary>
    Task WriteAsync(string key, ReadOnlyMemory<byte> data, CancellationToken ct = default)
        => WriteAsync(key, data.ToArray(), ct);

    /// <summary>Returns true if the key exists in the store.</summary>
    Task<bool> ExistsAsync(string key, CancellationToken ct = default);

    /// <summary>
    /// Lists all keys that begin with the given prefix.
    /// Pass an empty string to list from the root.
    /// </summary>
    Task<IReadOnlyList<string>> ListAsync(string prefix = "", CancellationToken ct = default);

    /// <summary>Deletes a key. No-ops silently if the key does not exist.</summary>
    Task DeleteAsync(string key, CancellationToken ct = default);
}
