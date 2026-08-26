# Packaging

This chapter covers what a published crate archive contains: the `include`
allow-list every publishable package declares, and the inputs Cargo adds on its
own.

## Only what a consumer compiles ships

A published archive exists so a consumer can build the crate and read its
rendered API documentation. Every publishable package therefore declares an
`include` allow-list rather than an `exclude` deny-list, so a file added later
stays out of the archive until someone lists it deliberately.

The default allow-list is the source tree alone, declared once in
`[workspace.package]` and inherited by each publishable package:

```toml
# Workspace Cargo.toml
[workspace.package]
include = [
    "src/**/*",
]
```

```toml
# Package Cargo.toml
include.workspace = true
```

A `publish = false` package inherits nothing here, because it is never packaged.

Tests, benchmarks, examples, books, and `AGENTS.md` stay in git. They are inputs
to developing the crate, not to consuming it.

Design and implementation documents stay in git as well. They are maintainer
documents, not user-facing documentation (see [design.md](design.md) and
[implementation.md](implementation.md)), and they link to repository-root
chapters that no archive carries, so a packaged copy could not be followed to a
complete picture anyway.

## Compile-time inputs are the exception

A file outside `src/` ships when the crate embeds it at compile time, because
the packaged crate cannot build without it. Such a package declares its own
`include` instead of inheriting, because inheritance replaces the workspace
value rather than extending it, so the list repeats `src/**/*` alongside the
extra pattern. Add the narrowest pattern that covers those files, and say in a
comment why they ship.

The cases in this workspace are the Mermaid diagrams embedded into rendered
documentation with `simple_mermaid::mermaid!`, and the fixture files
`cbh_engines` embeds with `include_str!` under its `private-test-util` feature.
A published feature that cannot compile is a defect, so the files its code
embeds ship with it.

## What Cargo adds regardless

Cargo packs `Cargo.toml`, a generated `Cargo.lock`, and the README it detects in
the package directory whether or not `include` names them. Listing the README
would only imply that the list controls it. For the full set of inputs Cargo
adds outside ordinary package rules, see
[`packages/cargo-release-plan/docs/design.md`](../packages/cargo-release-plan/docs/design.md),
"Where Cargo adds content".

## Changing an allow-list is a released-content change

Editing `include` changes which files the archive carries, so it changes
released content and requires a version increment like any other such change.
See [git-workflow.md](git-workflow.md) and the `increment-versions` skill.
