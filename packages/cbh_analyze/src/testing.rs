//! Small stored documents for fake-driven query orchestration.

#![cfg_attr(coverage_nightly, coverage(off))]

use cbh_git::FakeGitHistory;
use cbh_model::Run;
use cbh_storage::{MemoryStorage, Storage};
use futures::executor::block_on;

/// Serializes the result projection consumed by the real query loader.
///
/// Query policy reads context from storage keys and repository history. Most fixtures
/// therefore need only result data, not repeated serialization of discarded provenance.
/// Full persisted-schema compatibility is covered by the model and `RunPoints` tests;
/// tests specifically contrasting provenance with topology retain complete run documents.
pub(crate) fn run_points_json(run: &Run) -> String {
    format!(
        r#"{{"results":{}}}"#,
        serde_json::to_string(&run.results).unwrap()
    )
}

/// Seeds the real loader's minimal result projection without formatting a complete run.
pub(crate) fn store_run(storage: &MemoryStorage, key: &str, run: &Run) {
    block_on(storage.put(key, run_points_json(run).as_bytes())).unwrap();
}

/// A measured ancestor and a context commit suffice for presence and selection assertions.
pub(crate) fn two_commit_history(first: &str, tip: &str) -> FakeGitHistory {
    let mut git = FakeGitHistory::new();
    git.commit(first, None)
        .commit(tip, Some(first))
        .branch("master", tip)
        .head("master")
        .mark_default("master");
    git
}
