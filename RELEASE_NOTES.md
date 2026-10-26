# ZarrNET 0.8.0

Changes since `Release-0.7.0`, through commit `e5f4ed1`.

This release adds OME-Zarr 0.6 coordinate transformations and image resampling,
Zarr v3 transpose encoding, HTTP hierarchy discovery from consolidated metadata,
and a utility for converting individual Zarr v2 arrays to v3.

## Added

- **OME-Zarr 0.6 metadata:** Parse coordinate systems, transform input/output
  references, nested transformations, and scene metadata. Resolve image axes
  from coordinate-system metadata.
- **Point transformations:** Evaluate `identity`, `scale`, `translation`,
  `mapAxis`, `projectAxis`, `affine`, `rotation`, `sequence`, `byDimension`, and
  `bijection` transforms, with inverse evaluation where supported. Load affine
  and rotation matrices stored in separate Zarr arrays using
  `TransformParameterResolver`.
- **Vector-field transformations:** Resolve `coordinates` and `displacements`
  fields with nearest, linear, or cubic B-spline interpolation.
- **Physical-grid resampling:** Use `ResamplePhysicalRegionAsync` to sample an
  image onto a requested physical region and output shape with nearest, linear,
  or cubic B-spline interpolation. The overload accepting a root group resolves
  array-backed matrices and direct vector fields; vector fields use a numerical
  inverse solve.
- **Scene navigation:** Use `CoordinateTransformationGraph` to find transform
  paths and map points between coordinate systems across image groups.
- **Zarr v3 transpose codec:** Encode and decode chunk-axis permutations,
  including transpose codecs in shard inner pipelines.
- **HTTP hierarchy discovery:** List metadata keys and discover children using
  v2 `.zmetadata` or v3 inline consolidated metadata. Repeated listings reuse
  the fetched metadata index.
- **Zarr v2 to v3 conversion:** Use `ZarrV2ToV3Converter.ConvertArrayAsync` to
  copy an individual non-root array through decoded chunks into an uncompressed,
  little-endian v3 bytes pipeline, retaining array shape and chunk shape.

## Changed

- `OmeZarrWriter` now emits OME-Zarr **0.6** metadata, replacing its previous
  0.5 output. Generated metadata includes a named physical coordinate system,
  explicit transform endpoints, and a discrete channel axis. Check downstream
  reader support when consuming newly written datasets.
- Physical ROI conversion now maps every corner to compute enclosing bounds
  for rotated or sheared regions. Pixel-size calculation uses transformed unit
  steps to account for rotation and affine transforms.

## Tests and documentation

- Added automated coverage for writer/reader round trips, physical ROI reads,
  affine inversion, transform composition, rotated ROI bounds, physical-grid
  resampling, transpose encoding, HTTP consolidated metadata listing, v2 reads,
  and v2-to-v3 conversion.
- Expanded the README and HTTP documentation with APIs, examples, and support
  limits.

## Current limits

- Resampling supports real numeric data and rank-preserving transforms. Nested
  vector-field compositions and rank-changing projections remain unsupported.
- `ReadRegionAsync(PhysicalROI)` returns an enclosing source box; use
  `ResamplePhysicalRegionAsync` for an output grid in physical coordinates.
- HTTP listing requires consolidated metadata; stores without it still need
  explicit path navigation.
- Conversion handles one non-root array at a time. Recursive hierarchy,
  group-metadata, and arbitrary-attribute migration remain outside its scope.

[Full comparison](https://github.com/BiologyTools/Zarr.NET/compare/Release-0.7.0...e5f4ed1ee9b987ab99472da3f06cc1d085c6ad88)
