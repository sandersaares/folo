# Implementation

Pure marker and message composition is synchronous. Lifecycle orchestration is generic over the
`GitHub` port; unit tests use an in-memory fake and `futures::executor::block_on`, with no runtime,
network or real-time delay. The production `RestGitHub` adapter is the only HTTP boundary.

The port exposes semantic GitHub operations—list, create, update, close, delete, compare and read
the pull-request head—rather than a raw HTTP passthrough. Idempotent operations retry transient
failures in the adapter. Creates never retry blindly: orchestration reads by marker after an
error and treats a matching artifact as the successful result of the ambiguous request.

The binary is a thin Clap and Tokio entry point. `lib.rs` and `main.rs` contain only crate-level
documentation, attributes, re-exports and entry-point wiring.

