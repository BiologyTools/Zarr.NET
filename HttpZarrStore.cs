using System.Net;

namespace OmeZarr.Core.Zarr.Store;

/// <summary>
/// IZarrStore implementation for reading Zarr datasets over HTTP/HTTPS.
/// Supports public S3 buckets, Azure Blob Storage, Google Cloud Storage,
/// and any HTTP server serving Zarr files.
///
/// Note: Listing is limited - requires consolidated metadata (.zmetadata)
/// or works only with explicit path navigation.
/// </summary>
public sealed class HttpZarrStore : IZarrStore
{
    private readonly HttpClient _httpClient;
    private readonly string _baseUrl;
    private readonly bool _ownsHttpClient;
    private bool _disposed;

    // Cache for frequently accessed metadata files
    private readonly Dictionary<string, byte[]?> _metadataCache = new();
    private readonly SemaphoreSlim _cacheLock = new(1, 1);

    public string BaseUrl => _baseUrl;

    /// <summary>
    /// Creates an HTTP Zarr store with a new HttpClient.
    /// The HttpClient will be disposed when the store is disposed.
    /// </summary>
    public HttpZarrStore(string baseUrl)
        : this(baseUrl, new HttpClient(), ownsHttpClient: true)
    {
    }

    /// <summary>
    /// Creates an HTTP Zarr store with a provided HttpClient.
    /// The caller is responsible for disposing the HttpClient.
    /// </summary>
    public HttpZarrStore(string baseUrl, HttpClient httpClient, bool ownsHttpClient = false)
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _httpClient = httpClient;
        _ownsHttpClient = ownsHttpClient;

        // Set reasonable defaults if not already configured
        if (_httpClient.Timeout == TimeSpan.FromSeconds(100))  // Default HttpClient timeout
            _httpClient.Timeout = TimeSpan.FromSeconds(300);  // 5 minutes for large chunks
    }

    // -------------------------------------------------------------------------
    // IZarrStore
    // -------------------------------------------------------------------------

    public async Task<byte[]?> ReadAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        // Check cache for metadata files
        if (IsMetadataKey(key))
        {
            await _cacheLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (_metadataCache.TryGetValue(key, out var cached))
                    return cached;
            }
            finally
            {
                _cacheLock.Release();
            }
        }

        var url = BuildUrl(key);

        try
        {
            var response = await _httpClient.GetAsync(url, ct).ConfigureAwait(false);
            System.Diagnostics.Debug.WriteLine(
    $"[HttpZarrStore] GET {url} → {(int)response.StatusCode} {response.StatusCode}");
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                // Cache negative result for metadata
                if (IsMetadataKey(key))
                    await CacheMetadataAsync(key, null, ct).ConfigureAwait(false);

                return null;
            }

            response.EnsureSuccessStatusCode();

            var data = await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);

            // Cache metadata files
            if (IsMetadataKey(key))
                await CacheMetadataAsync(key, data, ct).ConfigureAwait(false);

            return data;
        }
        catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
        catch (TaskCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (TaskCanceledException ex)
        {
            throw new TimeoutException(
                $"HTTP request timed out while reading key '{key}' from {_baseUrl}", ex);
        }
    }

    public async Task WriteAsync(string key, byte[] data, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        // HTTP stores are typically read-only
        // For write support, would need PUT/POST with authentication
        throw new NotSupportedException(
            "Writing to HTTP Zarr stores is not supported. " +
            "HTTP stores are read-only. Use LocalFileSystemStore for write operations.");
    }

    public async Task<bool> ExistsAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        // Check cache first
        if (IsMetadataKey(key))
        {
            await _cacheLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (_metadataCache.ContainsKey(key))
                    return _metadataCache[key] is not null;
            }
            finally
            {
                _cacheLock.Release();
            }
        }

        var url = BuildUrl(key);

        try
        {
            // Try HEAD first — fast and lightweight
            using var headRequest = new HttpRequestMessage(HttpMethod.Head, url);
            var headResponse = await _httpClient.SendAsync(headRequest, ct).ConfigureAwait(false);

            if (headResponse.IsSuccessStatusCode)
                return true;

            if (headResponse.StatusCode == HttpStatusCode.NotFound)
                return false;

            // HEAD returned a non-success, non-404 status (e.g. 403, 405).
            // Some S3/Ceph implementations return 403 Forbidden for HEAD on
            // objects that are publicly readable via GET. Fall back to a GET
            // with a minimal range to confirm existence without downloading
            // the whole object.
            if (IsMetadataKey(key))
            {
                // For small metadata files, just try a full GET and cache the result.
                // This avoids an extra round-trip when ReadAsync is called next.
                var data = await ReadAsync(key, ct).ConfigureAwait(false);
                return data is not null;
            }

            // For non-metadata keys, try a range GET to avoid large downloads.
            using var rangeRequest = new HttpRequestMessage(HttpMethod.Get, url);
            rangeRequest.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(0, 0);
            var rangeResponse = await _httpClient.SendAsync(rangeRequest, ct).ConfigureAwait(false);

            return rangeResponse.IsSuccessStatusCode
                || rangeResponse.StatusCode == HttpStatusCode.PartialContent;
        }
        catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
        catch (TaskCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (TaskCanceledException ex)
        {
            throw new TimeoutException(
                $"HTTP request timed out while checking existence of '{key}' at {_baseUrl}", ex);
        }
    }

    public async Task<IReadOnlyList<string>> ListAsync(string prefix = "", CancellationToken ct = default)
    {
        ThrowIfDisposed();

        // HTTP stores don't have a standard way to list keys
        // We'd need either:
        // 1. Consolidated metadata (.zmetadata file - Zarr v2)
        // 2. Directory index (not standardized)
        // 3. S3 ListObjects API (requires AWS SDK)

        // For now, return empty list - navigation works via explicit paths
        // Users should use HasChildAsync and explicit path navigation

        throw new NotSupportedException(
            "Listing keys in HTTP Zarr stores is not supported. " +
            "HTTP/S3 stores require explicit path navigation. " +
            "Use HasChildAsync() to check for specific children, or enable consolidated metadata.");
    }

    public Task DeleteAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        throw new NotSupportedException(
            "Deleting from HTTP Zarr stores is not supported. " +
            "HTTP stores are read-only.");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private string BuildUrl(string key)
    {
        // URL-encode the key components to handle special characters
        var parts = key.Split('/');
        var encodedParts = parts.Select(Uri.EscapeDataString);
        var encodedKey = string.Join("/", encodedParts);

        return $"{_baseUrl}/{encodedKey}";
    }

    private static bool IsMetadataKey(string key)
    {
        return key.EndsWith("zarr.json", StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zarray", StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zgroup", StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zattrs", StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zmetadata", StringComparison.OrdinalIgnoreCase);
    }

    private async Task CacheMetadataAsync(string key, byte[]? data, CancellationToken ct)
    {
        await _cacheLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            _metadataCache[key] = data;
        }
        finally
        {
            _cacheLock.Release();
        }
    }

    // -------------------------------------------------------------------------
    // Disposal
    // -------------------------------------------------------------------------

    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return ValueTask.CompletedTask;

        _disposed = true;

        if (_ownsHttpClient)
            _httpClient?.Dispose();

        _cacheLock?.Dispose();

        return ValueTask.CompletedTask;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(HttpZarrStore));
    }
}
