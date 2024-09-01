set windows-shell := ["pwsh.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command"]
use_pwsh := if os() == "windows" { "pwsh.exe" } else { "/usr/bin/env pwsh" }

_default:
    @just --list --unsorted

bench:
    cargo bench

build PROFILE='dev':
    cargo build --workspace --profile {{ PROFILE }} --all-features --all-targets

clean:
    cargo clean

docs-api:
    cargo doc --workspace --no-deps --all-features

docs-open:
    cargo doc --workspace --no-deps --all-features --open

docs-test:
    cargo test --workspace --all-features --doc

# format rust code (workspace if run from root; otherwise, the current folder hierarchy)
[no-cd]
format:
    cargo fmt --verbose --all

# check code format (workspace if run from root; otherwise, the current folder hierarchy)
[no-cd]
format-check:
    cargo fmt --verbose --all --check

# format the Justfile itself (use before committing Justfile changes)
format-self:
    just --fmt --unstable

# cargo check all possible combinations of crate features
features-check:
    cargo hack check --workspace --feature-powerset --locked

# run machete to remove unused dependencies in the workspace
machete:
    cargo machete --skip-target-dir

test:
    cargo nextest --workspace --all-targets --all-features
