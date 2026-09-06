# Feature flags

This chapter covers conditional compilation in the workspace: gating test-only
code with `#[cfg(test)]`, gating optional functionality behind Cargo features,
and how the two interact in `test` builds.

## Test-only code requires cfg(test)

If there are functions that are only used in tests, mark them (and their `use`
statements) with `#[cfg(test)]`. Do not just suppress "dead code" warnings.

## Feature-gated code should also be enabled by `test` build

If code is feature-gated, it should always also be enabled in test builds:
`#[cfg(any(test, feature = "foo"))]`

When a feature gate controls a dependency (e.g. `dep:futures-core` behind
`futures-stream`), ensure the dependency is also listed as a dev-dependency so it
is available in test builds without requiring the feature to be explicitly
activated.
