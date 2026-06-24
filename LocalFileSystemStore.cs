namespace ZarrNET.Core.Zarr.Store;

/// <summary>
/// IZarrStore backed by the local filesystem. Keys map directly to
/// relative paths under the root directory, using the OS path separator.
/// </summary>
public sealed class LocalFileSystemStore : IZarrStore, IDisposable
{
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
    }

    public Task WriteManyAsync(
        IEnumerable<ZarrStoreWrite> writes,
        int maxDegreeOfParallelism,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(writes);

        if (maxDegreeOfParallelism < 1)
            throw new ArgumentOutOfRangeException(
                nameof(maxDegreeOfParallelism),
                maxDegreeOfParallelism,
                "Maximum degree of parallelism must be at least 1.");

        var writeList = writes as IReadOnlyList<ZarrStoreWrite> ?? writes.ToArray();
        var plannedWrites = new LocalWrite[writeList.Count];

        for (var i = 0; i < writeList.Count; i++)
        {
            ct.ThrowIfCancellationRequested();

            var write = writeList[i];
            plannedWrites[i] = PlanBatchWrite(write);
        }

        var directories = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < plannedWrites.Length; i++)
        {
            ct.ThrowIfCancellationRequested();
            directories.Add(plannedWrites[i].Directory);
        }

        foreach (var directory in directories)
        {
            ct.ThrowIfCancellationRequested();
            Directory.CreateDirectory(directory);
        }

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism,
            CancellationToken = ct
        };

        Parallel.For(
            0,
            plannedWrites.Length,
            options,
            i =>
            {
                ct.ThrowIfCancellationRequested();
                var write = plannedWrites[i];

                using var stream = new FileStream(
                    write.FullPath,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize: 1024 * 1024,
                    FileOptions.SequentialScan);

                stream.Write(write.Data.Span);
            });

        return Task.CompletedTask;
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

    private LocalWrite PlanBatchWrite(ZarrStoreWrite write)
    {
        if (!IsSafeRelativeStoreKey(write.Key))
        {
            var resolvedPath = ResolveKey(write.Key);
            return new LocalWrite(
                resolvedPath,
                Path.GetDirectoryName(resolvedPath)!,
                write.Data);
        }

        var lastSeparator = write.Key.LastIndexOf('/');
        var directory =
            lastSeparator < 0
                ? _rootPath
                : Path.Join(_rootPath, write.Key[..lastSeparator]);
        var fullPath = Path.Join(_rootPath, write.Key);

        return new LocalWrite(fullPath, directory, write.Data);
    }

    private static bool IsSafeRelativeStoreKey(string key)
    {
        if (string.IsNullOrEmpty(key))
            return false;

        if (key[0] == '/' || key[0] == '\\' || Path.IsPathRooted(key))
            return false;

        var segmentStart = 0;
        for (var i = 0; i < key.Length; i++)
        {
            var c = key[i];
            if (c == '\\' || c == ':')
                return false;

            if (c == '/')
            {
                if (IsUnsafePathSegment(key, segmentStart, i))
                    return false;

                segmentStart = i + 1;
            }
        }

        return !IsUnsafePathSegment(key, segmentStart, key.Length);
    }

    private static bool IsUnsafePathSegment(string key, int start, int end)
    {
        var length = end - start;
        return length == 0 ||
               length == 1 && key[start] == '.' ||
               length == 2 && key[start] == '.' && key[start + 1] == '.';
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

    private readonly record struct LocalWrite(
        string FullPath,
        string Directory,
        ReadOnlyMemory<byte> Data);
}
