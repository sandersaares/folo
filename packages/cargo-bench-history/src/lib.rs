#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![expect(
    clippy::exhaustive_enums,
    clippy::exhaustive_structs,
    reason = "this crate's `pub` items form a handoff boundary between lib.rs and \
              the in-crate binary plus integration tests, used only inside this \
              workspace rather than as a stable public API, so exhaustive \
              construction and matching by those in-workspace consumers is intended"
)]

//! Maintain a long-lived history of benchmark results and analyze it for trends
//! that snapshot-only tooling cannot see.
//!
//! Most benchmark tooling only reports the current run, or at best compares
//! against the previous local run. `cargo-bench-history` instead stores *every*
//! run as an immutable result set — on the local filesystem or in an Azure Blob
//! container — and reconstructs each benchmark's series
//! in git first-parent commit order, so historical trends become analyzable
//! ("benchmark X has been getting incrementally slower over the past 12 months",
//! "scenario Y regressed after commit Z, visible only in hindsight against noisy
//! data").
//!
//! # Quick start
//!
//! ```text
//! cargo bench-history install   # write a starter .cargo/bench_history.toml
//! cargo bench-history collect --local=./bench-history    # bench the current commit and store the results
//! cargo bench-history backfill --local=./bench-history <from> <to>   # bootstrap history from past commits
//! cargo bench-history analyze --local=./bench-history    # report regressions and drift over history
//! ```
//!
//! `collect` benches the current commit and stores the results in the selected
//! storage; `backfill` benches a range of past commits so there is a trend to
//! analyze (a single `collect` on its own has nothing to compare against); `analyze`
//! reads the accumulated history back and reports what changed. Run these from the
//! repository whose benchmarks you are tracking.
//!
//! Storage is selected at run time: `--local=<path>` (or a bare `--local`, taking
//! the path from `CARGO_BENCH_HISTORY_STORAGE`) selects local filesystem storage,
//! while omitting `--local` uses the optional Azure backend configured in
//! `.cargo/bench_history.toml`. `install` writes that starter config; a local path
//! is machine-dependent, so it is supplied per command rather than stored in the
//! shared file.
//!
//! # How results are stored
//!
//! Each run is stored as one immutable result set per benchmark engine. The
//! storage is partitioned *only* by the factors that make two results
//! fundamentally incomparable — the **discriminant set**:
//!
//! * **engine** — the benchmark system (`callgrind`, `criterion`, `alloc_tracker`,
//!   `all_the_time`), since their numbers are not comparable to each other.
//! * **target triple** — for example `x86_64-unknown-linux-gnu`.
//! * **machine key** — a hardware fingerprint of the host the benchmark ran on.
//!   Every engine is partitioned by it (Callgrind instruction counts and
//!   `alloc_tracker` allocations included), because even those figures vary with
//!   the hardware in practice — libraries dispatch to different code paths on
//!   different microarchitectures — so results from different machines are never
//!   mixed.
//!
//! Everything else — toolchain version, OS, commit, CI provider — is recorded as
//! metadata rather than forking history, so its effect stays *visible* as a step
//! in the timeline. Results are keyed by commit: a **clean** run (committed tree)
//! writes one deterministic object per commit, while a **dirty** run (uncommitted
//! changes present) is stored separately as an ephemeral snapshot that analysis
//! only admits on the branch you are working on.
//!
//! # Commands
//!
//! ## `collect`
//!
//! Executes the workspace benches with `cargo bench`, harvests every supported
//! engine's machine-readable output, and stores one result set per engine. There
//! is nothing to configure about engines: the run enables the combined
//! environment the engines need and detects each engine from the output
//! it produces (off Linux the Callgrind benches compile to no-ops, so only the
//! host-runnable engines are stored). Re-running the same clean commit is refused
//! as a duplicate unless `--overwrite` replaces the stored result; `--no-store`
//! harvests and reports without writing. `--best-of N` reruns the whole suite `N`
//! times and keeps the minimum value per metric — a noise-reduction pass for
//! jittery runners, where interference only ever makes a case slower. A run is
//! positioned on the timeline by where its commit sits in git history
//! (first-parent topology), resolved live at analyze time — never by when the
//! benchmarks happened to execute.
//!
//! ## `install`
//!
//! Generates a starter `.cargo/bench_history.toml` if absent, printing its path
//! and next steps (including how to `backfill` history for an existing
//! repository); an existing file is never overwritten.
//!
//! ## `backfill`
//!
//! Replays `collect` across the inclusive commit range `<from> <to>`, bootstrapping
//! history for a repository that adopted the tool late. Each commit is checked out
//! in a dedicated git **worktree** (the primary checkout is never touched, so a
//! dirty working tree is fine) and benched there, taking its timeline position
//! from where the commit sits in git history. The range `<from> <to>` only needs
//! to form a first-parent chain; it does not have to lie on the current branch.
//! The range is processed newest commit first, so a run cut short has filled the
//! most recent — and most comparison-relevant — gaps. Each commit is built by the
//! toolchain its own checkout selects, so a `rust-toolchain.toml` in history
//! governs its own build and a `cargo +nightly run … backfill` does not impose
//! `nightly` on it. Commits that already have a stored result for this host's
//! target triple and machine key are skipped (so backfill is resumable and cheap
//! to re-run); `--overwrite` re-benches the whole range. A commit that fails to
//! build or bench stops the run unless `--ignore-errors` continues past it.
//!
//! ## `analyze`
//!
//! Reconstructs a timeline from git history and reports notable patterns. It
//! requires a repository. Commits up to the merge-base with the base branch
//! contribute clean runs only, while commits unique to the analyzed branch also
//! contribute dirty snapshots unless `--no-dirty` is given. Pass one or more
//! benchmark-id prefixes (matched against the qualified
//! `<package>/<group>/<case>/<value>` identity) to scope the analysis to a subset
//! of benchmarks. Findings are *advisory*: the exit code reflects only whether the
//! analysis ran, never what it found. Downstream automation reads the
//! machine-readable signal from the `json` report — `mode`, the named `outcome`,
//! the backward-compatible boolean `notable` (any finding survived), each finding's
//! `direction`, and the full per-finding `series` for charting. `--outcome <path>`
//! writes just the stable outcome wire name for lightweight automation. See
//! [Analyze modes](#analyze-modes) below.
//!
//! ## `list`
//!
//! Previews, without analyzing, the data a matching `analyze` would consume. The
//! subject is a bare word:
//!
//! * `list runs` — the run, series, and per-commit counts of the selected runs,
//!   per discriminant set.
//! * `list discriminants` — the discriminant sets present in storage (no
//!   repository required), for discovering which engines, triples, and machine
//!   keys have data before scoping an analysis. This is a discovery catalog, so it
//!   lists *every* stored partition regardless of the current machine; pass a discriminant
//!   to narrow it.
//! * `list blessings` — the blessings recorded at the current commit, or — with
//!   `--all` — the most recent blessing of every benchmark across the window.
//!
//! ## `prune`
//!
//! Deletes a chosen portion of the stored data, using the same selection pipeline
//! as `analyze`/`list`. An action is required: `--dirty` removes ephemeral
//! uncommitted-tree snapshots, `--clean` removes clean runs, and `--all` removes
//! both. `--include-blessings` additionally deletes blessing sidecars in the
//! selected range — including orphans on commits with no recorded run — and may be
//! given on its own; pruning runs otherwise never removes a blessing. Pruning
//! preserves base-branch history — only
//! the context branch's own commits (those after the merge-base with `--base`) are
//! removed. When the context resolves onto the base branch itself, the whole
//! selection *is* base-branch history, so the deletion is refused unless
//! `--prune-base` confirms it. Narrow the selection with a discriminant, a `<commit>`
//! argument, or `--since`. `--dry-run` previews without deleting.
//!
//! ## `examine`
//!
//! A drill-down sibling of `list runs` over the same `analyze`/`list` selection: it
//! pivots the small chart `analyze` draws into a per-commit listing. Given the two
//! required options `--benchmark <qualified-id>` and `--metric <name>` — the one
//! command that names a metric, meant to be pasted from an `analyze` finding — it
//! lists every commit from the earliest one carrying that series in any matching
//! discriminant set up to the analyzed tip, in git first-parent order and once per
//! set, pairing the value with the short commit id and the first 50 characters of the
//! commit's title (JSON keeps the full title and full-precision value). Every set
//! shares that range, so their listings cover the same commits; a commit with no data
//! point reads `n/a` (null in JSON), and one with several observations contributes a
//! row per observation (clean before dirty, each flagged). It requires a repository
//! and runs no detection or re-baselining. An unknown metric name is rejected. An
//! empty pivot is explained by one of two hints: when no run enters the selection at
//! all, the same "matched no runs" hint `analyze` gives; when runs enter but none
//! carry the named `(benchmark, metric)` pair, a distinct hint pointing at the
//! unmatched benchmark id or metric name.
//!
//! ## `bless` / `unbless`
//!
//! `bless` accepts an intentional performance change on the base branch so history
//! analysis stops re-flagging it. Pass one or more benchmark-id prefixes to accept
//! (matched against the qualified `<package>/<group>/<case>/<value>` identity, e.g.
//! `bless all_the_time/read_cell`), or `--all` to accept every benchmark recorded
//! at the commit. A blessing re-baselines the benchmark's history from the blessed
//! commit forward, so the accepted step is no longer reported while earlier points
//! stay on the chart for context. Blessing prefers the base branch and an existing
//! recorded run at the blessed commit, but neither is required: blessing off the base
//! branch warns (the blessing only takes effect once the commit joins the base's
//! first-parent history), and blessing a commit with no recorded run warns and
//! synthesizes the target discriminant sets from the resolved discriminant filters, so a change can
//! be accepted *before* its data is captured. By default `bless`/`unbless` act on
//! `HEAD`; `--context <ref>` blesses or unblesses another commit instead. `unbless`
//! removes the blessings recorded at that commit — note that any blessings defined
//! at *later* commits remain in force, so the timeline may stay blessed past the
//! unblessed commit.
//!
//! ## `machine-key`
//!
//! Prints this machine's hardware fingerprint — the machine key every engine
//! partitions its history by — to standard output as a single clean line, and exits.
//! It probes only the host's hardware: no repository, git, config, or storage is
//! touched. This is the key `collect` stamps every result with, so CI captures it and
//! threads the exact keys a collection produced into the matching `analyze` selection
//! (see the per-push and per-PR workflows). Under `--verbose` the individual factors
//! behind the fingerprint (processor count, memory regions, processor models, and the
//! factor-set version tag) are written to standard error, so a change in the key can be
//! traced to the specific factor that moved.
//!
//! # Selecting data: options shared by the query commands
//!
//! `analyze`, `list`, `prune`, and `examine` share one selection model, organized
//! in `--help` into named groups:
//!
//! * **Discriminant selection** (`--engine`, `--target-triple`, `--machine-key`) —
//!   chooses which discriminant sets to operate on. Each discriminant is repeatable
//!   (union of values) and defaults to the current machine's value when omitted
//!   (`--engine` defaults to every engine; `list discriminants` is a catalog and
//!   defaults to every partition). The literal `all` removes the filter
//!   for that dimension, e.g. `--machine-key all` spans every machine.
//! * **Commit selection** (`--context`, `--base`, `--since`) — `--context` is the
//!   ref whose history is analyzed (default `HEAD`); `--base` is the ref it branched
//!   from (default: the configured or detected default branch), which determines the
//!   merge-base split. Because `--context` already anchors the newest edge of the
//!   timeline, `--since` is a one-sided cutoff: it bounds only the oldest commit to
//!   include by that commit's **committer date** (read from git history) and accepts
//!   an RFC 3339 timestamp, a `YYYY-MM-DD` date, or a relative duration such as
//!   `6 months ago`.
//! * **Data filtering** (`--no-dirty`) — exclude dirty snapshots.
//!
//! # Analyze modes
//!
//! `analyze` runs in one of two modes, chosen automatically from the git topology
//! (there is no flag to force a mode):
//!
//! * **history** — long-range trend watch over the base branch (selected when you
//!   analyze a clean checkout of the base branch). It detects sustained
//!   change-points and slow drifts, defaults `--since` to the last six months, and
//!   reports only **regressions** (steady improvement over time is expected).
//! * **branch** — "how does my feature compare" (selected for a feature branch, or
//!   a dirty base checkout). It judges the branch by its **tip commit** versus the
//!   base — the intermediate commits are ignored, since only the tip lands in the
//!   base on merge — reporting both regressions and improvements.
//!
//! # Configuration
//!
//! `install` writes a starter `.cargo/bench_history.toml`. It carries an optional
//! `[project]` id (used to namespace stored data; defaults to the workspace
//! directory name) and an optional `[storage.<kind>]` **cloud** backend
//! (`[storage.azure]` today). Local filesystem storage is **not** configured here:
//! a local path is machine-dependent and this file is shared, so local storage is
//! selected per command with `--local`:
//!
//! ```toml
//! # [project]
//! # id = "my-project"
//! # default_branch = "main"   # base branch for `analyze`; auto-detected by default
//!
//! # Optional: store results in an Azure Blob container. Omit entirely to use
//! # local storage via --local instead.
//! # [storage.azure]
//! # account = "mystorageaccount"
//! # container = "bench-history"
//! ```
//!
//! Storage is resolved per command: `--local=<path>` selects local filesystem
//! storage (a bare `--local` takes the path from `CARGO_BENCH_HISTORY_STORAGE`),
//! otherwise the configured cloud backend is used; with neither, a storage-backed
//! command errors (except `collect --no-store`, which stores nothing). Every command
//! also accepts `--config PATH` to point at a non-default file, `--repo
//! PATH` to resolve git state from another directory, and `--verbose` to emit a
//! step-by-step diagnostic trail to standard error.
//!
//! ## Azure Blob storage
//!
//! The Azure backend authenticates with **Microsoft Entra ID** (OAuth): it stores
//! no secret, and every request carries a short-lived token over an HTTPS endpoint
//! (the default `https://<account>.blob.core.windows.net`). The account may — and
//! should — have shared-key access disabled entirely.
//!
//! To stand up an Entra-ID-backed store:
//!
//! 1. **Deploy a Storage account** reachable over HTTPS. Entra-only accounts
//!    (shared-key access disabled) are supported and preferred — there is then no
//!    account key to leak. The `bench-history` container does not need to pre-exist;
//!    `collect` creates it on first use.
//! 2. **Grant the identity that runs the tool the `Storage Blob Data Contributor`
//!    role** on the account. This data-plane role covers both the blob read/write
//!    the tool performs and the container creation `collect` does on first use; the
//!    broader `Storage Blob Data Owner` is not needed for a flat blob container.
//!    Locally, that identity is your `az login` user; in CI it is the federated
//!    managed identity below.
//! 3. **For CI, authenticate with GitHub OIDC workload identity federation** instead
//!    of a stored secret: create a user-assigned managed identity, add a federated
//!    credential whose subject matches the workflow's OIDC token, and whose audience
//!    is `api://AzureADTokenExchange`, then grant it the role from step 2. The subject
//!    must match **each event that runs the tool**, so register one credential per
//!    event: a run on the default branch presents
//!    `repo:<owner>/<repo>:ref:refs/heads/main`, while a pull-request-triggered run
//!    (for example a workflow that benchmarks a PR) presents
//!    `repo:<owner>/<repo>:pull_request` and needs its own credential with that
//!    subject. Only **same-repo** pull requests can federate — a fork's run cannot
//!    mint a token whose subject names your repository, so fork PRs cannot reach the
//!    store and such workflows must skip them. Run the tool from a job that has
//!    `permissions: { id-token: write }`, with the managed identity's client ID and
//!    your Entra tenant ID exported as the `AZURE_CLIENT_ID` and `AZURE_TENANT_ID`
//!    environment variables. The tool then mints a fresh OIDC assertion straight from
//!    GitHub's per-job token endpoint for each Entra token exchange, so it stays
//!    authenticated even across the hourly access-token refresh of a multi-hour
//!    benchmark run — no `azure/login` step and no stored secret are involved. (When
//!    those variables are absent — locally, or in a short job that runs `azure/login`
//!    — the tool instead picks up the ambient Azure CLI session.)
//!
//! Worked, runnable examples of all of the above live in the folo repository as Bicep
//! templates with PowerShell deploy wrappers: the long-lived store with its own
//! dedicated managed identity at
//! <https://github.com/folo-rs/folo/tree/main/infra/azure-bench-history-prod> and a
//! separate test account/identity at
//! <https://github.com/folo-rs/folo/tree/main/infra/azure-bench-history-test>, with the
//! account name baked into the committed `.cargo/bench_history.toml`, the per-push
//! consumer at <https://github.com/folo-rs/folo/blob/main/.github/workflows/bench-history.yml>,
//! and the per-pull-request consumer (which collects and compares a PR against `main`,
//! and relies on the identity's `pull_request` federated credential) at
//! <https://github.com/folo-rs/folo/blob/main/.github/workflows/pr-bench-history.yml>.

mod commands;
mod config_writer;
mod dispatch;
mod errors;
mod outcome;
mod output;

pub use cbh_analyze::{AnalysisOutcome, AutoDiscriminants};
pub use cbh_cli::{Cli, EarlyExit};
pub use cbh_command::{
    AnalyzeOptions, BackfillOptions, BlessOptions, CacheSelection, CollectOptions, Command,
    ExamineOptions, ImportOptions, InstallOptions, ListOptions, ListSubject, LocalStorageSelection,
    MachineKeyOptions, PruneOptions, UnblessOptions,
};
pub use cbh_config::default_template;
pub(crate) use cbh_model as model;
pub use cbh_storage::StorageOverride;
pub(crate) use cbh_storage::finish_with_flush;
pub use dispatch::{Overrides, run, run_with_overrides};
pub(crate) use errors::*;
pub use model::{
    BenchmarkId, BenchmarkIdPrefix, BenchmarkResult, EnvironmentInfo, EnvironmentProvider, GitInfo,
    Metric, MetricKind, Run, RunContext, SCHEMA_VERSION, ToolchainInfo,
};
pub use outcome::RunOutcome;
