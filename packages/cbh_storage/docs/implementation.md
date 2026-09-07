# cbh_storage implementation

`cbh_storage` implements the persistence behavior specified by the
[`cargo-bench-history` design](../../cargo-bench-history/docs/DESIGN.md). Its place in the
application is defined by the
[`cargo-bench-history` implementation guide](../../cargo-bench-history/docs/implementation.md)
and the workspace rules for [implementation documentation](../../../docs/implementation.md).

The crate owns the persistence port, local and Azure adapters, read-through caching, storage-facing
validation of keys presented to backends, cache-control key construction, and cache invalidation.
Persisted object-key layout, construction, and parsing belong to `cbh_model`. Backend adapters
preserve one storage contract; callers and caching logic depend on the port rather than
backend-specific APIs. Configuration selection is supplied by `cbh_config`, stored values use
`cbh_model`, and byte encoding belongs to `cbh_codec`.

All backends return one operation-level aggregate. Private conditions add backend and operation
context while retaining filesystem, codec, configuration, or service causes. The aggregate exposes
only the narrow decisions required across the crate boundary: object absence and write-once
collision. Other distinctions remain private under the workspace
[error-handling guide](../../../docs/error-handling.md).

Local writes compress data and publish it atomically. Azure operations retain SDK diagnostics,
while conditional creates persist an opaque request identity so a collision caused by an automatic
retry after a committed upload is recognized as success. Read-through caching uses per-project
invalidation markers after remote overwrites and deletions.
