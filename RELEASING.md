# Guide to releasing a new version

Publishing to crates.io and shipping `cargo-binstall` prebuilt binaries is automated
by `.github/workflows/release.yml` on every push to `main`. Pull requests that change
released content carry the version increments; merge publishes those versions.
See [docs/release-versioning.md](docs/release-versioning.md) for how versions are
decided and [docs/release-automation.md](docs/release-automation.md) for the publish
design.

`main` is behind a merge queue whose only required status check is `required-checks`.
See [Required GitHub configuration](#required-github-configuration) below.

1. Validate everything via `just validate` on Windows (will automatically invoke Linux validation).
1. If you feel like it, also perform extra validation via `just validate-extra`.
1. On merge to `main`, `release.yml` publishes any version crates.io does not yet have
   (via crates.io Trusted Publishing — no stored token) and uploads prebuilt binaries
   for the binary crates. If anything fails it opens a `ci-failure` issue for that run.

## First publish of a new crate

crates.io does not allow Trusted Publishing for a crate that has never been published,
so a brand-new crate's first version must be published manually:

1. `cargo publish -p <crate>` (with a crates.io token login).
1. Configure Trusted Publishing for the crate on crates.io (owner `folo-rs`, repo
   `folo`, workflow `release.yml`).
1. Re-run `release.yml`. For a binary crate, the workflow creates the missing tag
   and GitHub release at the package's version anchor before uploading its prebuilt binaries.
   Subsequent releases then go through `release.yml` automatically.

The `increment-versions` skill runs `just check-never-published` as an early,
workspace-wide advisory. Before applying an approved plan,
`just check-increment-published` fails unless every **publishable** package the
plan reaches has already reached crates.io. Version-alignment targets with
publication disabled do not require a first-publication handoff. The gate cannot
verify Trusted Publisher configuration or the release-workflow follow-up, so
complete those remaining steps explicitly before retrying the increment.

## Emergency manual publish

If the CI publish path is broken, publish by hand with `cargo publish -p <crate>` (in
dependency order). For a binary crate, re-run `release.yml` (or push a version bump)
afterwards so the prebuilt binaries are produced.

## Required GitHub configuration

Branch protection, the merge queue, and the required-status-check ruleset are GitHub
settings rather than files in this repository, so they are configured once by a
repository admin and are prerequisites of the process above:

* `main` is protected.
* The merge queue is enabled on `main`.
* The ruleset requires only the status check named `required-checks`.
* Individual Validation matrix job names are not required — a skipped leg never
  posts a check and would block the queue forever.

`cargo-release-plan` also needs a one-time first `cargo publish` (and Trusted
Publishing configured afterwards) before later versions can go through `release.yml`.
