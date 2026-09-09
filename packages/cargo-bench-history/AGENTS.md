# Agent notes for cargo-bench-history

An async-by-default Tokio application. **All pure logic** (parsing, mapping, comparability,
key derivation, analysis, message formatting) stays **synchronous**; async is pushed only to
the IO edges. The code is a **shell crate** (this CLI + wiring) plus a family of small,
doc-hidden `cbh_*` implementation crates — the pure, I/O-free data model, statistics,
analysis, codec, and rendering, plus the extracted IO adapters (git, storage, probes,
engines) — released in lockstep with the shell. Several expose a `private-test-util`
feature carrying in-workspace test/bench utilities.

> **This file is agent instructions, not a design or implementation guide.** User-visible
> behavior lives in [`docs/DESIGN.md`](docs/DESIGN.md). Package ownership and internal tenets
> live in [`docs/implementation.md`](docs/implementation.md), with the `analyze` data flow in
> [`docs/analyze.md`](docs/analyze.md). Keep the owning document in sync when changing behavior,
> package boundaries, storage format, or the analysis pipeline. Do not restate that content here.

## Ports and fakes

Every IO edge is a small **port trait** (RPITIT — `impl Future` return, no `async_trait`)
with a real Tokio adapter and an in-`#[cfg(test)]` in-memory fake, so orchestration runs
against fakes under `block_on` (Miri-safe, no runtime). The ports:

* `process::BenchRunner` — runs the bench argv directly (no shell).
* `probe::EnvironmentProbe` — shells `git`/`rustc`, reads the hardware profile.
* `bench_output::BenchOutputSource` — harvests each engine's output tree, `mtime`-scoped.
* `storage::Storage` — `LocalStorage`/`AzureBlobStorage`, selected by `build_storage` into a
  `StorageFacade`; `MemoryStorage` fake. `put` is **write-once** (`AlreadyExists`);
  `put_overwrite`/`delete` are the escape hatches and arm the cache-invalidation marker.
* `config_writer::ConfigWriter` — `install`'s config-file writer (`create_new`).
* `git_history::GitHistory` — read-only commit topology (`resolve`, `default_branch`,
  `merge_base`, `first_parent`, `committer_time`, `is_dirty`). `first_parent` pairs each commit ID
  with its committer date and subject; `FakeGitHistory` models a canned graph.

**When you add an IO edge, follow the same pattern.** Never call `SystemTime::now()` in
orchestration — time comes from the injected `tick::Clock` (`new_tokio` in production,
`new_frozen_at` in tests). Never insert real-time delays in tests.

## Compute fan-out

Do **not** spawn ad-hoc threads (`std::thread::scope`, `map_parallel`-style helpers) for
compute. Route every parallel pass through the injected `anyspawn::Spawner` that
`analyze_with` threads through: production injects the Tokio blocking pool, tests inject
`cbh_detect::testing::synchronous_spawner` (exposed by the `private-test-util`
feature), so the work stays runtime-agnostic and Miri-safe.
New per-series logic must be side-effect-free. Flow and rationale: [`docs/analyze.md`](docs/analyze.md).

## Selection lockstep (analyze / list / prune / examine)

`analyze`, `list`, `prune`, and `examine` share one **data-set-selection pipeline** in
`analyze/mod.rs` (`Selection` + `resolve_discriminants` +
`discriminant_filtered_candidates` + `resolve_history`/`select_dataset`), and all four live
inside the `analyze` module tree (`list.rs`, `prune.rs`, `examine.rs`, each
`pub(crate) mod`; `bless`/`unbless` in `bless.rs` reuse the same discriminant-filter
selection). **A selection parameter added to one must be added to all four** unless
genuinely inapplicable. The analyze-only condensed `--markdown-summary` output is **not**
part of the lockstep — only `analyze` detects; `list`/`prune`/`examine` reuse the selection
but never analyze.
The always-on **ghost filter** likewise changes only *which reconstructed series are detected
on* (it drops benchmarks absent at the context commit), not which runs are *selected*,
so `list runs` may report more series than `analyze` now analyzes. Each is
generic over the `GitHistory` + `Storage` ports so tests drive it with fakes + `block_on`.
Semantics and per-command behaviour: DESIGN §7–§8.

`analyze` needs a resolvable repository (topology at query time); `list discriminants` is the
one query view that does **not** (a pure index over storage keys). `examine` is the one
command that names a `--metric` (its input is an `analyze` finding, which prints the metric).

## Hidden `import` command

`import` is a hidden, unsupported sibling of `collect` — the store path with the `cargo bench`
run removed — reusing `collect`'s finalize-and-store helper. **Keep the two on one code path**:
a store rule added to `collect` (keying, overwrite/skip-existing, dirty coexistence) must hold
for `import` for free. Its invariants are easy to break: the harvest is **ungated** (freshness
`None`), so `--target-dir` is **required** and must never fall back to `<repo>/target`; the three overrides (`--target-triple` / `--commit` / `--dirty`) attribute imported data without manual machine key selection; `import` assumes nothing about the data being synthetic (real output must import identically).
`--commit` is git-resolved (unknown = hard error) and never checks the commit out. Semantics:
DESIGN §7.9.

## CLI (clap derive)

The CLI uses **clap** derive (chosen over `argh` for `help_heading` grouping — the surface is
wide). Flags live in shared `#[command(flatten)]` arg-group structs so a group looks identical
everywhere and the selection lockstep holds automatically. Do not assume the other workspace
cargo tools share these grouped conventions. Full group/flag map: DESIGN §7.

## Verbose diagnostics (`report::Reporter`)

`--verbose` threads a `&dyn report::Reporter` through each pipeline. Emit notes **only**
through the guarded `ReporterExt` surface — `note_with(|| format!(…))` for one lazy line,
`if_enabled(|notes| …)` for a block; the unconditional `Sink` is sealed, so there is no
`enabled()`/`note()` guard to forget. Stage timings are a separate channel
(`ReporterExt::timing`). A third channel, `ReporterExt::announce`, is **always on** (not
verbose-gated): use it only for the one-line effective-selection / effective-partition
summaries that must appear on every run, never for per-object chatter. Production
`StderrReporter` writes `[bench-history] …` to **stderr** (notes/timings only when verbose;
announcements always) — never stdout, so machine-readable output stays clean. The reporter is
`&dyn` (not `+ Sync`) so run futures stay `!Send` / Miri-driven. Notes must be **explanatory,
not conclusion-only** (see `docs/standalone-binaries.md`). Tests use the `#[cfg(test)]`
`RecordingReporter` (`notes()`/`contains()` for notes, `announcements()`/`announced()` for the
always-on channel).

## Storage & engines (essentials)

* Object bodies are **gzip** at the `Storage` boundary via `cbh_codec`;
  callers hand/receive plain JSON. `MemoryStorage` deliberately stays **plaintext** (keeps the
  Miri `analyze` suite fast) — do not compress it.
* The backend is chosen at run time by `--local` / configured cloud (never a local path in the
  committed config); read commands add a `--cache` read-through mirror. Details: DESIGN §4, §6.
* Four engines are parsed in `bench/`. None is deterministic; they differ by whether each
  point carries a confidence interval (`criterion`, `all_the_time`, and `alloc_tracker` all
  record one on every operation) or is a single value (`callgrind`, plus any legacy mean-only
  file the adapter still tolerates). An interval is only ever an extra veto that can suppress
  a candidate finding, never create one. `collect`/`backfill` invoke `cargo bench` (once, or
  `--best-of N` times keeping the per-metric minimum) and harvest whatever ran; `--engine` is
  an `analyze` discriminant filter, not a collect flag. Details: DESIGN §1, §7.1.

## Testing

**Miri.** Pure logic and fake-driven orchestration tests are plain `#[test]` +
`futures::executor::block_on` + a frozen clock (no runtime, no IO) so they run under Miri. Any
test touching the real filesystem/process/runtime/wall-clock or the network emulator must be
`#[tokio::test]` (or `#[test]`) **and** `#[cfg_attr(miri, ignore = "…")]` with a reason.

**Faker (test-support engine).** End-to-end tests launch the standalone
**`cargo-bench-history-faker` lib+bin package** — a stand-in benchmark engine. It is a
published-but-unsupported crate (`#![cfg_attr(docsrs, doc(hidden))]`, no stable API/CLI); the workspace consumes
its `binary_path()` locator behind the `private-test-util` feature, so `cargo install
cargo-bench-history` still places only the one real binary on PATH. Tests resolve the binary via
`cargo_bench_history_faker::binary_path()`; every `just` recipe that runs the suite under nextest
pre-builds it once and passes `CBH_FAKER` (avoids per-process `cargo build` races). It writes
per-engine fixture output on demand via repeatable flags (`--callgrind`/`--criterion`/
`--alloc-tracker`/`--all-the-time`) and `--fail-if-exists` to simulate a failing commit. The
value cores behind those writers live in the faker's public `writers` module, so unit and
fidelity tests can build the exact same documents without spawning a process.

**CWD & target root.** Almost every test — including the real-adapter e2e — pins its own
tempdir workspace and target root through the `run_with_overrides` entry (`workspace_dir` +
`target_root`, the latter injected as `CARGO_TARGET_DIR` for the engine), so it neither reads
the ambient environment nor mutates the process CWD. **Do not** reintroduce a
`set_var("CARGO_TARGET_DIR", …)` guard or a CWD-changing `#[serial]` e2e: an ambient
`CARGO_TARGET_DIR` (a shared or per-worktree target dir) would point the harvest at foreign
engine output. `tests/cbh_real_engine.rs` is the one true e2e (real `cargo bench` + Criterion).
Production `resolve_target_root` is covered by its unit tests in `commands::collect`. The one
deliberate exception to the pin rule is the mock
`collect_harvests_output_when_the_engine_runs_in_a_package_directory` regression, which drives
the no-override path (`Workspace::drive_resolving_target_root`) to prove `collect` calls
`resolve_target_root` and injects an *absolute* `CARGO_TARGET_DIR`; it is therefore the sole
integration coverage of that wiring — keep it, and keep the harvest hermetic there by other
means than a target-root override.

**Fixtures.** Parser fixtures live in `cbh_engines` (`tests/fixtures/**`, exposed here as
`cbh_engines::testing` behind `private-test-util`). Criterion and Callgrind files are **real**
committed output of those external tools, doubling as schema-drift canaries — do not
hand-edit to make a test pass; regenerate from a real run. The in-workspace engines
(`alloc_tracker`, `all_the_time`) are guarded differently: `cbh_engines`'s
`schema_roundtrip` test drives the **real** producer and feeds its output straight through
the adapter, so producer/consumer drift fails the build automatically. Their fixture files
are therefore representative samples of the current schema for the value-asserting unit tests,
not the drift canary — keep them in sync with the producer's shape, but the round-trip test is
authoritative.
Use the minimal synthetic documents exported by `cbh_engines::testing` for orchestration
scenarios that do not assert producer schema compatibility. Seed existing storage directly
when a test only needs to exercise a subsequent collection, rather than executing collection
again as fixture setup.

**Backfill coverage.** Planning/skip/overwrite/error logic is exhaustively covered by
fake-driven unit tests in `commands/backfill.rs`; keep the real-git integration drives a small
smoke subset (each spawns many git subprocesses and dominates mutation cost) — add new
coverage at the fake level.

**Mutation.** SDK-delegating Azure IO methods carry `#[cfg_attr(test, mutants::skip)]` (only
Azurite covers them); their pure logic stays under mutation. A test with no mutation signal
opts out with `#[cfg_attr(mutants, ignore = "<why>")]` — `just mutants` sets `--cfg mutants`
(registered in `unexpected_cfgs`).

**Azure.** The Azure backend is always compiled in (Entra-OAuth only). Network tests
**self-skip** when no emulator/account is reachable. Run them with `just test-azurite` (local
Azurite; sets `BENCH_HISTORY_REQUIRE_AZURITE=1`) or `just test-azure` (real account; sets
`ENABLE_AZURE`, reads the account from `constants.env`, needs `az login`). Auth/seam design:
DESIGN + `storage/azure.rs`.

## Stress harness

`cargo-bench-history-stress` (sibling package) is an on-demand scaling experiment for
`analyze` with **zero production-code coupling**: it writes objects in the exact storage key
layout + gzip body (via `cbh_codec`) and reads them back through the public
`run_with_overrides` entry — so if you change the on-disk key layout or the JSON report's
top-level fields, update its `seed.rs`/`report.rs` in lockstep. Run via
`just bench-history-stress[-azure]`; dataset shape is in its `README.md`.
