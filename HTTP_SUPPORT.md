# HTTP/Remote Storage Support - Implementation Summary

## What Was Added

HTTP/HTTPS remote storage support for reading OME-Zarr datasets from cloud storage and web servers.

### New Files

1. **HttpZarrStore.cs** - IZarrStore implementation for HTTP/HTTPS access
   - Uses standard HttpClient for GET requests
   - Metadata caching to reduce network requests
   - Configurable timeouts and authentication
   - 404 handling (returns null for missing keys)

### Modified Files

1. **OmeZarrReader.cs** - Auto-detects URLs and creates appropriate store
   - `OpenAsync(pathOrUrl)` now accepts both file paths and URLs
   - `OpenAsync(IZarrStore)` overload for custom stores

2. **ZarrGroup.cs** - Gracefully handles stores without listing support
   - `ListChildNamesAsync()` returns empty list for HTTP stores (listing not supported)
   - Users must use explicit path navigation with HTTP datasets

3. **README.md** - Updated documentation
   - Added remote access examples
   - Updated features, limitations, and roadmap

### Usage

```csharp
// Automatic detection - works with URLs or file paths
await using var reader = await OmeZarrReader.OpenAsync("https://example.com/data.zarr");

// Custom HttpClient configuration
var httpClient = new HttpClient();
httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer TOKEN");
var store = new HttpZarrStore("https://example.com/data.zarr", httpClient);
await using var reader = await OmeZarrReader.OpenAsync(store);

// Everything else works identically to local files
var image = reader.AsMultiscaleImage();
var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);
```

## Supported Remote Storage

✅ **HTTP/HTTPS** - Any web server serving Zarr files  
✅ **Amazon S3** - Public buckets or presigned URLs  
✅ **Azure Blob Storage** - Public containers or SAS URLs  
✅ **Google Cloud Storage** - Public buckets  
✅ **IDR** - Image Data Resource (https://idr.openmicroscopy.org/)  

## How It Works

### Store Detection
`OmeZarrReader.OpenAsync()` checks if the path is a URL:
- `http://` or `https://` → creates HttpZarrStore
- `file://` → creates LocalFileSystemStore
- Everything else → assumes local path, creates LocalFileSystemStore

### HTTP Requests
- **Read** - GET request, returns null on 404
- **Exists** - HEAD request for efficiency
- **List** - Not supported (throws NotSupportedException)
  - HTTP doesn't have standard directory listing
  - Future: could support consolidated metadata (.zmetadata)
- **Write/Delete** - Not supported (read-only)

### Metadata Caching
HttpZarrStore caches metadata files (.zarray, .zattrs, zarr.json) to avoid repeated requests:
- First access fetches and caches
- Subsequent accesses use cache
- Reduces network overhead significantly

### Performance Considerations

**Good:**
- Low-resolution levels load quickly
- Small ROIs are efficient
- Metadata cached after first access

**Less Efficient:**
- Large full-resolution reads over network
- Non-chunk-aligned ROIs (multiple HTTP requests)
- No parallel chunk downloading (yet)

**Best Practices:**
1. Use lower resolution levels for preview/navigation
2. Read chunk-aligned regions when possible
3. Cache frequently accessed data locally
4. Use custom HttpClient with appropriate timeouts

## Examples Provided

**HttpExamples.cs** contains 12 real-world scenarios:

1. Read from public HTTP URL
2. Read from S3 public bucket
3. Read from Azure Blob Storage
4. Read from Google Cloud Storage
5. Custom HttpClient configuration (auth, timeouts)
6. IDR public datasets
7. Progressive loading (low res → high res)
8. Download and cache locally
9. Compare local vs remote performance
10. Handle network errors gracefully
11. Mixed local and remote
12. Real-world: Find brightest region in IDR dataset

## Testing

Test with real public datasets:

```csharp
// IDR dataset (always available)
var url = "https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.4/idr0062A/6001240.zarr";
await using var reader = await OmeZarrReader.OpenAsync(url);

var image = reader.AsMultiscaleImage();
Console.WriteLine($"Loaded remote image: {image.Multiscales[0].Name}");
```

## Limitations

1. **No listing support** - Must navigate via explicit child paths
   - Can't use `ListChildNamesAsync()` to discover children
   - Use `HasChildAsync("childName")` to check specific paths
   - Future: .zmetadata consolidated metadata could enable listing

2. **Read-only** - No write support for HTTP stores
   - Write operations throw NotSupportedException
   - Use LocalFileSystemStore for writing

3. **No parallel downloads** - Chunks read sequentially
   - Future enhancement: parallel chunk fetching

4. **No range requests** - Entire chunk files downloaded
   - Future enhancement: HTTP range requests for partial chunks

5. **Authentication** - Requires custom HttpClient setup
   - No built-in OAuth/AWS credentials handling
   - Users must configure HttpClient.DefaultRequestHeaders

## Future Enhancements

### Planned
- [ ] Consolidated metadata (.zmetadata) support for listing
- [ ] Parallel chunk downloading
- [ ] HTTP range requests for partial chunks
- [ ] AWS SDK integration (proper credentials, ListObjects)
- [ ] Retry logic with exponential backoff
- [ ] Progress reporting for large downloads

### Possible
- [ ] Local cache layer (disk-backed)
- [ ] Prefetch hinting for known access patterns
- [ ] Compression over the wire (Accept-Encoding)
- [ ] CloudFront/CDN optimization
- [ ] WebSocket streaming for real-time data

## Architecture Notes

The implementation maintains clean separation:

```
OmeZarrReader
    ↓ (detects URL vs path)
HttpZarrStore / LocalFileSystemStore
    ↓ (both implement IZarrStore)
ZarrGroup, ZarrArray
    ↓ (no knowledge of HTTP vs local)
OME-Zarr Nodes
```

The HTTP layer is completely isolated in HttpZarrStore. All upper layers (ZarrGroup, ZarrArray, OME nodes) work identically whether the store is local or remote. This makes the code testable and allows easy addition of new store types (S3 SDK, Azure SDK, etc.) without touching the rest of the codebase.

## Dependencies

No new dependencies required:
- Uses built-in `HttpClient` (.NET runtime)
- No AWS, Azure, or GCP SDKs needed for basic functionality
- Optional: Users can bring their own HttpClient with SDK authentication

## Breaking Changes

None. Fully backward compatible:
- Existing code using file paths works unchanged
- New URL support is opt-in
- LocalFileSystemStore unchanged
