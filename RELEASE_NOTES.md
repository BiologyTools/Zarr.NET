# ZarrNET 0.8.2

Changes since `Release-0.8.1`, through commit `66d5ecd`.

This release adds support for reading Blosc-compressed chunks that use
bit-shuffle filtering, expanding compatibility with existing Zarr datasets.

## Added

- **Blosc bit-shuffle decoding:** `BloscCodec.DecodeAsync` now reverses the
  bit-shuffle filter after decompressing each block, instead of rejecting
  bit-shuffled frames with `NotSupportedException`. This includes single-byte
  elements as well as multi-byte elements.

## Tests

- Added regression tests that decode LZ4-compressed, bit-shuffled frames and
  compare the output with expected byte and UInt16 data.

## Current limits

- This change adds bit-shuffle decoding only; bit-shuffle encoding is not
  implemented.
- Bit-shuffled blocks must contain a multiple of eight elements; other element
  counts raise `InvalidDataException`.
- BloscLZ and Snappy inner codecs remain unsupported.

[Full comparison](https://github.com/BiologyTools/Zarr.NET/compare/Release-0.8.1...66d5ecdb0f92641de1f80f2e6c67f0ba2b7f6a40)
