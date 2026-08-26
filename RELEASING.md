# Guide to releasing a new version

Publishing to crates.io and shipping `cargo-binstall` prebuilt binaries is automated
by `.github/workflows/release.yml` on every push to `main`. Pull requests that change
released content carry the version increments; merge publishes those versions.
See [docs/release-versioning.md](docs/release-versioning.md) for how versions are
decided and [docs/release-automation.md](docs/release-automation.md) for the publish
design.

Once the remaining GitHub settings below are in place, `main` is behind a merge
queue whose only required status check is `required-checks`.

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
1. Subsequent releases then go through `release.yml` automatically.

The `increment-versions` skill's preflight (`just check-never-published`) stops if a
crate in the increment set has never been published, so first-publish is not folded
into `apply`.

## Emergency manual publish

If the CI publish path is broken, publish by hand with `cargo publish -p <crate>` (in
dependency order). For a binary crate, re-run `release.yml` (or push a version bump)
afterwards so the prebuilt binaries are produced.

## Remaining GitHub settings

Branch protection, the merge queue, and the required-status-check ruleset are GitHub
settings, not files in this repository. A human with repository admin access must
apply:

* Protect `main`.
* Enable the merge queue on `main`.
* Require only the status check named `required-checks`.
* Do not require individual Validation matrix job names — skipped legs never post a
  check and would block the queue.

`cargo-release-plan` also needs a one-time first `cargo publish` (and Trusted
Publishing configured afterwards) before later versions can go through `release.yml`.
