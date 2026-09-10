# cbh_diag implementation

`cbh_diag` supports the diagnostic behavior specified by the
[`cargo-bench-history` design](../../cargo-bench-history/docs/DESIGN.md). Its place in the
application is defined by the
[`cargo-bench-history` implementation guide](../../cargo-bench-history/docs/implementation.md)
and the workspace rules for [implementation documentation](../../../docs/implementation.md).

The crate owns the shared diagnostic-reporting abstraction and deterministic diagnostic text
helpers. Producers report through that abstraction without owning a process stream, while the
shell supplies the production sink. Private test support records the same reporting operations so
orchestration can be verified without writing to a terminal.
Tests that do not inspect verbose diagnostics use a quiet recorder: announcements remain
observable, but per-object diagnostic formatting and stage recording stay disabled.
