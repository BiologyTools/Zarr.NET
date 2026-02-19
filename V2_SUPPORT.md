# Zarr v2 Support Added

## What Changed

Your Fiji-exported OME-Zarr is **Zarr v2**, which uses separate metadata files instead of a single zarr.json.

### v2 File Structure
```
dataset.zarr/
├── .zgroup          ← Group metadata (just format version)
├── .zattrs          ← OME-Zarr attributes (multiscales, axes, etc.)
├── 0/               ← Resolution level 0
│   ├── .zarray      ← Array metadata (shape, chunks, dtype, compressor)
│   ├── 0.0.0        ← Chunk files (dot-separated coordinates)
│   ├── 0.0.1
│   └── ...
├── 1/               ← Resolution level 1
│   ├── .zarray
│   └── ...
└── labels/
    └── cells/
        ├── .zattrs
        ├── 0/
        │   └── .zarray
        └── ...
```

### v3 File Structure (for reference)
```
dataset.zarr/
├── zarr.json        ← All metadata in one file
├── 0/
│   ├── zarr.json
│   └── c/           ← Chunks under c/ subdirectory
│       ├── 0/0/0
│       └── ...
└── ...
```

## Files Modified

1. **ZarrV2Document.cs** (new)
   - Parses `.zarray`, `.zgroup`, `.zattrs` files
   - v2 compressor object → codec pipeline

2. **NumpyDtypeParser.cs** (new)
   - Converts numpy dtype strings like `"<u2"` → ZarrDataType
   - Handles byte order prefix: `<` (little), `>` (big), `|` (n/a)

3. **ZarrNodeMetadata.cs**
   - Added `FromV2Document` factory methods
   - v2 compressor → synthesized codec pipeline (bytes + gzip/zstd)

4. **ZarrGroup.cs**
   - Auto-detects v2 vs v3 (looks for zarr.json vs .zgroup)
   - `OpenArrayAsync` / `OpenGroupAsync` / `OpenRootAsync` all try v3 first, fall back to v2
   - `ListChildNamesAsync` finds both zarr.json and .zarray/.zgroup
   - `HasChildAsync` checks for all metadata file types

5. **ZarrArray.cs**
   - `BuildChunkKey` handles v2 dot-separated keys: `"0.1.2"` vs v3 nested: `"c/0/1/2"`

## Usage (no changes needed)

Your existing usage code works identically. The reader auto-detects version:

```csharp
await using var reader = await OmeZarrReader.OpenAsync("/path/to/fiji-export.zarr");
// ↑ This now works with both v2 and v3

var image = reader.AsMultiscaleImage();
var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);
var roi   = new PhysicalROI(origin: [0, 0, 0, 0, 0], size: [1, 1, 1, 100, 100]);
var data  = await level.ReadRegionAsync(roi);
```

## v2 Specifics Worth Knowing

1. **Compressor instead of codec pipeline**
   - v2: `"compressor": {"id": "gzip", "level": 5}`
   - We synthesize a v3-style pipeline: `[bytes, gzip]`

2. **Numpy dtypes**
   - v2: `"dtype": "<u2"` → little-endian uint16
   - v3: `"data_type": "uint16"` + `"endian": "little"` in bytes codec

3. **Chunk keys**
   - v2: `0.1.2` (dimension_separator: ".")
   - v3: `c/0/1/2` (chunk_key_encoding with separator: "/")

4. **No dimension_names**
   - v2 doesn't have dimension names in .zarray
   - Use the OME-Zarr axes metadata instead (in .zattrs)

5. **Blosc still not supported**
   - If your Fiji export uses Blosc, you'll get an error
   - Re-export with gzip compression if needed

## Testing Your Fiji Data

```csharp
var zarrPath = @"C:\path\to\fiji-export.zarr";

await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
Console.WriteLine($"Detected: {reader.RootNodeType}");

var image = reader.AsMultiscaleImage();
Console.WriteLine($"Multiscale: {image.Multiscales[0].Name}");
Console.WriteLine($"Axes: {string.Join(", ", image.Multiscales[0].Axes.Select(a => a.Name))}");

var level = await image.OpenResolutionLevelAsync(0);
Console.WriteLine($"Shape: [{string.Join(", ", level.Shape)}]");
Console.WriteLine($"DataType: {level.DataType}");

// Read a small region to verify
var testRoi = new PhysicalROI(
    origin: new double[level.Rank],  // all zeros
    size:   Enumerable.Repeat(10.0, level.Rank).ToArray()  // 10 units per axis
);

var result = await level.ReadRegionAsync(testRoi);
Console.WriteLine($"Read: {result}");
```

This will tell you immediately if the reader works with your data and what dtype/shape you're dealing with.
