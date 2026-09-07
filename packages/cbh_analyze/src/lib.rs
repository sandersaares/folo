#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(hidden))]
#![expect(
    clippy::exhaustive_structs,
    reason = "this crate's `pub` items form an internal handoff boundary between the \
              cargo-bench-history sub-crates rather than a stable public API, so \
              exhaustive construction of its value types by those in-workspace \
              consumers is intended"
)]

//! Implementation crate for [`cargo-bench-history`]; do not depend on this directly.
//!
//! The `analyze`-family orchestration: resolve which stored runs make up each
//! benchmark's history from live git topology, reconstruct the series, and report the
//! notable changes, plus the sibling `list`, `prune`, `examine`, and `bless`/`unbless`
//! commands that share the same loading and selection machinery.
//!
//! Unlike a snapshot tool, `analyze` orders a series by *git history* rather than by
//! ingest time (see the `analyze` command in `DESIGN.md`): it resolves the target ref's
//! first-parent ancestry, splits it at the merge-base with a base branch, and admits
//! dirty (uncommitted-tree) snapshots only on the target side of that split. The pure
//! logic (selection, series reconstruction, finding detection, report rendering) stays
//! sync and Miri-safe; only the git queries and object loads touch async ports. The
//! command entry points wire the real adapters; the `analyze_with`-style orchestrators
//! they call are storage- and git-generic so the in-memory tests can drive them. Split
//! out of the `cargo-bench-history` shell so this orchestration layer is cheap to
//! mutation-test in isolation.
//!
//! Every command entry point is re-exported flat from the crate root, so the shell
//! writes `cbh_analyze::analyze` rather than reaching into a submodule.
//!
//! [`cargo-bench-history`]: https://github.com/folo-rs/folo

mod announce;
mod bless;
mod comparison_base;
mod dataset;
mod discriminants;
mod error;
mod examine;
mod history;
mod list;
mod load;
mod pipeline;
mod prune;
mod report;
mod selection;
mod window;

pub use bless::{bless, unbless};
pub(crate) use cbh_detect::{Series, SeriesFilter, apply_blessings};
pub use cbh_render::AnalysisOutcome;
pub(crate) use cbh_render::{ReportFormat, chart_series, format_value};
pub(crate) use dataset::{empty_history_hint, select_dataset};
pub use discriminants::AutoDiscriminants;
pub(crate) use discriminants::resolve_discriminants;
pub use error::AnalyzeError;
pub(crate) use error::{
    BaseBranchUnavailableError, BlessBaseRequiredError, BlessDiscriminantsRequiredError,
    BlessSelectionRequiredError, CommitterTimeFailedError, DefaultBranchProbeFailedError,
    EmptyBenchmarkError, FirstParentWalkFailedError, InvalidBlessingError, InvalidResultSetError,
    InvalidStoredUtf8Error, InvalidWindowValueError, ListAllUnsupportedError, MergeBaseFailedError,
    MergeBaseUnavailableError, NoOutputSelectedError, PruneBaseConfirmationRequiredError,
    PruneSelectionRequiredError, ResolveRefFailedError, ToolchainProbeFailedError,
    UnknownEngineError, UnknownMetricError, UnresolvedRefError, WindowOutOfRangeError,
    WorkingTreeProbeFailedError,
};
pub use examine::execute as examine;
pub(crate) use history::{
    DirtyTipPolicy, ResolvedHistory, dirty_base_exception_warning, resolve_history,
};
pub use list::execute as list;
pub(crate) use load::{RunIndex, discriminant_filtered_candidates};
pub use pipeline::execute as analyze;
pub(crate) use pipeline::{resolve_auto_discriminants, resolve_now};
pub use prune::execute as prune;
pub use report::RenderedReports;
pub(crate) use report::ReportRequest;
pub(crate) use selection::Selection;
pub use window::auto_mode;
pub(crate) use window::{before_since_cutoff, parse_since};
