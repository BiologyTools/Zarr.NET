namespace ZarrNET.Core.Zarr.Store;

/// <summary>
/// IZarrStore backed by the local filesystem. Keys map directly to
/// relative paths under the root directory, using the OS path separator.
/// </summary>
public sealed class LocalFileSystemStore : IZarrStore, IDisposable
{
    private static int s_debugCount = 0;

    private readonly string _rootPath;
    private bool _disposed;

    public string RootPath => _rootPath;
    public void Dispose()
    {
        DisposeAsync();
    }
    public LocalFileSystemStore(string rootPath)
    {
        var expanded = Path.GetFullPath(rootPath);

        if (!Directory.Exists(expanded))
            throw new DirectoryNotFoundException($"Zarr store root not found: {expanded}");

        _rootPath = expanded;
    }

    // -------------------------------------------------------------------------
    // IZarrStore
    // -------------------------------------------------------------------------

    public async Task<byte[]?> ReadAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var fullPath = ResolveKey(key);

        if (!File.Exists(fullPath))
            return null;

        var data = await File.ReadAllBytesAsync(fullPath, ct).ConfigureAwait(false);
        if (s_debugCount < 16)
        {
            Log($"[LocalFileSystemStore.ReadAsync] key={key} path={fullPath} len={data.Length} sample={SampleBytes(data)}");
            s_debugCount++;
        }
        return data;
    }

    public async Task<int?> ReadAsync(string key, Memory<byte> destination, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var fullPath = ResolveKey(key);

        if (!File.Exists(fullPath))
            return null;

        var length = checked((int)new FileInfo(fullPath).Length);
        if (length > destination.Length)
            throw new ArgumentException(
                $"Destination has {destination.Length} bytes, but key '{key}' contains {length} bytes.",
                nameof(destination));

        await using var stream = new FileStream(
            fullPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 1024 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);

        var offset = 0;
        while (offset < length)
        {
            var read = await stream.ReadAsync(destination[offset..length], ct).ConfigureAwait(false);
            if (read == 0)
                throw new IOException(
                    $"Short read for '{fullPath}': expected {length} bytes but received {offset} bytes.");

            offset += read;
        }

        return length;
    }

    public async Task WriteAsync(string key, byte[] data, CancellationToken ct = default)
    {
        await WriteAsync(key, data.AsMemory(), ct).ConfigureAwait(false);
    }

    public async Task WriteAsync(string key, ReadOnlyMemory<byte> data, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var fullPath = ResolveKey(key);
        var directory = Path.GetDirectoryName(fullPath)!;

        Directory.CreateDirectory(directory);

        await using (var stream = new FileStream(
            fullPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            bufferSize: 1024 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan))
        {
            await stream.WriteAsync(data, ct).ConfigureAwait(false);
        }

        if (s_debugCount < 16)
        {
            Log($"[LocalFileSystemStore.WriteAsync] key={key} path={fullPath} len={data.Length} sample={SampleBytes(data)}");
            if (key.StartsWith("0/c/", StringComparison.Ordinal))
            {
                try
                {
                    var disk = await File.ReadAllBytesAsync(fullPath, ct).ConfigureAwait(false);
                    Log($"[LocalFileSystemStore.WriteAsync] disk key={key} path={fullPath} len={disk.Length} sample={SampleBytes(disk)}");
                }
                catch (Exception ex)
                {
                    Log($"[LocalFileSystemStore.WriteAsync] disk-read EXCEPTION key={key} path={fullPath} {ex.GetType().Name}: {ex.Message}");
                }
            }
            s_debugCount++;
        }
    }

    public Task<bool> ExistsAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ct.ThrowIfCancellationRequested();

        var fullPath = ResolveKey(key);
        var exists    = File.Exists(fullPath) || Directory.Exists(fullPath);

        return Task.FromResult(exists);
    }

    public Task<IReadOnlyList<string>> ListAsync(string prefix = "", CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ct.ThrowIfCancellationRequested();

        var searchRoot = string.IsNullOrEmpty(prefix)
            ? _rootPath
            : ResolveKey(prefix);

        if (!Directory.Exists(searchRoot))
            return Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());

        var allFiles = Directory
            .EnumerateFiles(searchRoot, "*", SearchOption.AllDirectories)
            .Select(absPath => ToStoreKey(absPath))
            .OrderBy(k => k)
            .ToList();

        return Task.FromResult<IReadOnlyList<string>>(allFiles);
    }

    public Task DeleteAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ct.ThrowIfCancellationRequested();

        var fullPath = ResolveKey(key);

        if (File.Exists(fullPath))
            File.Delete(fullPath);

        return Task.CompletedTask;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Converts a store key (forward-slash separated) to an absolute filesystem path.
    /// Guards against path traversal attacks by ensuring the resolved path stays
    /// within the store root.
    /// </summary>
    private string ResolveKey(string key)
    {
        // Normalise separator and remove any leading slash
        var relativePath = key
            .Replace('/', Path.DirectorySeparatorChar)
            .TrimStart(Path.DirectorySeparatorChar);

        var fullPath = Path.GetFullPath(Path.Combine(_rootPath, relativePath));

        if (!fullPath.StartsWith(_rootPath, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                $"Key '{key}' resolves outside the store root. Possible path traversal.");

        return fullPath;
    }

    /// <summary>Converts an absolute filesystem path back to a forward-slash store key.</summary>
    private string ToStoreKey(string absolutePath)
    {
        var relative = Path.GetRelativePath(_rootPath, absolutePath);
        return relative.Replace(Path.DirectorySeparatorChar, '/');
    }

    // -------------------------------------------------------------------------
    // Disposal
    // -------------------------------------------------------------------------

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(LocalFileSystemStore));
    }

    private static void Log(string message)
    {
        System.Diagnostics.Debug.WriteLine(message);
    }

    private static string SampleBytes(byte[] data, int count = 16)
        => SampleBytes(data.AsSpan(), count);

    private static string SampleBytes(ReadOnlyMemory<byte> data, int count = 16)
        => SampleBytes(data.Span, count);

    private static string SampleBytes(ReadOnlySpan<byte> data, int count = 16)
        => string.Join(",", data[..Math.Min(count, data.Length)].ToArray());
}
