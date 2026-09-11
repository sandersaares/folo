// Native protocol coverage for lifecycle contracts.
use serde_json::{Value, json};

use crate::protocol::execute;
use crate::tests::support::*;

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed scope comparison retains Miri coverage"
)]
fn operation_only_actionable_scope_changes_advance_required_scope() {
    let mut input = problem_update();
    input["incoming"]["diagnosis"]["category"] = json!("code");
    input["incoming"]["diagnosis"]["repair_disposition"] = json!("actionable");
    input["incoming"]["diagnosis"]["scope"][0]["platform"] = Value::Null;
    input["incoming"]["diagnosis"]["scope"][0]["operation"] =
        json!("Run the failing target with its original execution qualifier");
    let original: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    input["existing"] = original.clone();
    input["incoming"]["operation_id"] = json!("expanded-operation");
    input["incoming"]["diagnosis"]["scope"][0]["operation"] =
        json!("Run the failing target with an additional execution qualifier");
    let expanded: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(expanded["scope_revision"], 2);
    assert_eq!(expanded["generation"], original["generation"]);
    assert_eq!(expanded["evidence"][0], original["evidence"][0]);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn problem_identity_and_operation_replay_preserve_scope_and_occurrence() {
    let mut input = problem_update();
    let problem: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(problem["issue_number"], 99);
    assert_eq!(
        problem["evidence"][0]["diagnosis"]["scope"],
        diagnosis()["scope"]
    );
    input["existing"] = problem.clone();
    let repeated: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(repeated, problem);
    input["incoming"]["diagnosis"]["cause"] = json!("conflicting operation payload");
    _ = execute(&input.to_string()).unwrap_err();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn closed_without_resolution_and_new_runs_without_ancestry_do_not_recur() {
    let mut input = problem_update();
    let problem: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    input["existing"] = problem;
    input["existing"]["status"] = json!("needs-human");
    input["incoming"]["operation_id"] = json!("new-publication");
    input["incoming"]["relation"] = json!("recurrence");
    input["incoming"]["source_relation"] = json!("descendant");
    advance_update(&mut input, 900, 1, "2026-09-09T03:00:00Z");
    _ = execute(&input.to_string()).unwrap_err();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn duplicate_evidence_does_not_authorize_a_new_unproved_recurrence() {
    let mut input = problem_update();
    let original: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    input["existing"] = original.clone();
    input["incoming"]["operation_id"] = json!("new-disposition");
    input["incoming"]["relation"] = json!("recurrence");
    for status in ["open", "needs-human"] {
        input["existing"]["status"] = json!(status);
        _ = execute(&input.to_string()).unwrap_err();
    }
    input["incoming"]["relation"] = json!("repeat");
    let repeated: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(repeated["evidence"], original["evidence"]);
    assert_eq!(repeated["status"], "needs-human");
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn confirmed_recurrence_and_late_historical_evidence_have_independent_occurrences() {
    let mut input = problem_update();
    input["existing"] = resolved_problem();
    advance_update(&mut input, 900, 1, "2026-09-09T03:00:00Z");
    input["incoming"]["operation_id"] = json!("recurrence");
    input["incoming"]["relation"] = json!("recurrence");
    let recurring: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(recurring["generation"], 2);
    assert_eq!(recurring["scope_revision"], 1);
    assert_eq!(recurring["status"], "open");
    assert_eq!(
        recurring["resolved_occurrences"].as_array().unwrap().len(),
        1
    );
    input["existing"] = recurring.clone();
    let replayed: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(replayed, recurring);

    let mut late = problem_update();
    late["existing"] = recurring.clone();
    late["incoming"]["operation_id"] = json!("late-history");
    late["incoming"]["relation"] = json!("historical");
    late["incoming"]["diagnosis"]["summary"] = json!("Additional description of the older symptom");
    late["incoming"]["diagnosis"]["scope"][0]["operation"] =
        json!("Additional required operation in the resolved occurrence");
    let updated: Value = serde_json::from_str(&execute(&late.to_string()).unwrap()).unwrap();
    assert_eq!(updated["generation"], 2);
    assert_eq!(updated["scope_revision"], recurring["scope_revision"]);
    assert_eq!(updated["observation"], recurring["observation"]);
    assert_eq!(updated["diagnosis"], recurring["diagnosis"]);
    assert_eq!(
        updated["evidence"].as_array().unwrap().last().unwrap()["generation"],
        1
    );
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn historical_current_occurrence_evidence_advances_only_growing_scope() {
    let mut input = problem_update();
    let original: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    input["existing"] = original.clone();
    advance_update(&mut input, 788, 1, "2026-09-09T00:50:00Z");
    input["incoming"]["operation_id"] = json!("late-current-scope");
    input["incoming"]["relation"] = json!("historical");
    let same_scope: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(same_scope["scope_revision"], 1);
    input["existing"] = same_scope;
    input["incoming"]["operation_id"] = json!("late-expanded-scope");
    input["incoming"]["diagnosis"]["scope"][0]["operation"] =
        json!("Additional required operation in the current occurrence");
    let expanded: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(expanded["scope_revision"], 2);
    assert_eq!(expanded["generation"], original["generation"]);
    assert_eq!(expanded["diagnosis"], original["diagnosis"]);
    assert_eq!(expanded["observation"], original["observation"]);
    assert_eq!(expanded["status"], original["status"]);
    assert_eq!(expanded["evidence"][0], original["evidence"][0]);
    input["existing"] = expanded.clone();
    let replayed: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(replayed, expanded);
    input["incoming"]["operation_id"] = json!("duplicate-late-scope");
    let duplicate: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(duplicate, expanded);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn recurrence_rejects_old_or_unrelated_execution_and_exhausted_generations() {
    for relation in ["unrelated", "unknown"] {
        let mut input = problem_update();
        input["existing"] = resolved_problem();
        advance_update(&mut input, 900, 1, "2026-09-09T03:00:00Z");
        input["incoming"]["operation_id"] = json!("new");
        input["incoming"]["relation"] = json!("recurrence");
        input["incoming"]["source_relation"] = json!(relation);
        _ = execute(&input.to_string()).unwrap_err();
    }
    let mut input = problem_update();
    input["existing"] = resolved_problem();
    input["incoming"]["operation_id"] = json!("old");
    input["incoming"]["relation"] = json!("recurrence");
    input["incoming"]["diagnosis"]["summary"] = json!("A distinct older observation");
    _ = execute(&input.to_string()).unwrap_err();
    advance_update(&mut input, 900, 1, "2026-09-09T03:00:00Z");
    input["existing"]["generation"] = json!(u64::MAX);
    input["existing"]["resolution"]["generation"] = json!(u64::MAX);
    input["incoming"]["target_generation"] = json!(u64::MAX);
    _ = execute(&input.to_string()).unwrap_err();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn repeats_preserve_complete_scope_and_refine_same_revision_diagnosis() {
    let mut input = problem_update();
    let original: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    input["existing"] = original.clone();
    input["incoming"]["operation_id"] = json!("refined");
    input["incoming"]["diagnosis"]["cause"] =
        json!("More precise description of the same source failure");
    let refined: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(
        refined["diagnosis"]["cause"],
        input["incoming"]["diagnosis"]["cause"]
    );
    assert_eq!(refined["scope_revision"], 1);
    input["existing"] = refined;
    advance_update(&mut input, 900, 1, "2026-09-09T03:00:00Z");
    input["incoming"]["operation_id"] = json!("additional-scope");
    input["incoming"]["diagnosis"]["scope"][0]["package"] = json!("affected-package");
    let broader: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(broader["scope_revision"], 2);
    assert_eq!(broader["generation"], 1);
    assert_eq!(
        broader["evidence"][0]["diagnosis"]["scope"],
        original["evidence"][0]["diagnosis"]["scope"]
    );
    input["existing"]["scope_revision"] = json!(u64::MAX);
    _ = execute(&input.to_string()).unwrap_err();

    input["existing"] = original.clone();
    input["existing"]["evidence"] = json!([]);
    input["existing"]["legacy"] = json!({
        "finding_id":"retained", "generation":1,
        "scope":original["diagnosis"]["scope"], "record":{"registered":"retained"}
    });
    input["incoming"]["diagnosis"]["scope"] = original["diagnosis"]["scope"].clone();
    let imported: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(imported["scope_revision"], 1);
    assert_eq!(imported["legacy"]["scope"], original["diagnosis"]["scope"]);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn problem_updates_reject_foreign_identity_future_occurrences_and_new_unpublished_history() {
    let original: Value =
        serde_json::from_str(&execute(&problem_update().to_string()).unwrap()).unwrap();
    for (pointer, value) in [
        ("/incoming/operation_id", json!("")),
        ("/incoming/observation/source_sha", json!("b".repeat(40))),
        ("/incoming/observation/source_sha", json!("invalid")),
        ("/incoming/target_generation", json!(2)),
        ("/existing/repository_id", json!(124)),
    ] {
        let mut input = problem_update();
        input["existing"] = original.clone();
        input["incoming"]["operation_id"] = json!("different");
        input["incoming"]["diagnosis"]["summary"] = json!("Distinct evidence description");
        *input.pointer_mut(pointer).unwrap() = value;
        _ = execute(&input.to_string()).unwrap_err();
    }
    let mut input = problem_update();
    input["incoming"]["target_generation"] = json!(2);
    _ = execute(&input.to_string()).unwrap_err();
    let mut input = problem_update();
    input["existing"] = original.clone();
    input["incoming"]["operation_id"] = json!("new-operation-same-evidence");
    let unchanged: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(unchanged, original);
    input["existing"] = resolved_problem();
    advance_update(&mut input, 900, 1, "2026-09-09T03:00:00Z");
    _ = execute(&input.to_string()).unwrap_err();
    input["incoming"]["relation"] = json!("historical");
    _ = execute(&input.to_string()).unwrap_err();
    input["incoming"]["source_relation"] = json!("unrelated");
    _ = execute(&input.to_string()).unwrap();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn stale_occurrences_and_inapplicable_repeats_cannot_replace_current_execution() {
    let mut input = problem_update();
    let original: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    input["existing"] = original.clone();
    input["existing"]["generation"] = json!(2);
    input["incoming"]["operation_id"] = json!("changed");
    input["incoming"]["diagnosis"]["summary"] = json!("Different description");
    _ = execute(&input.to_string()).unwrap_err();
    input["incoming"]["relation"] = json!("historical");
    _ = execute(&input.to_string()).unwrap_err();

    input["existing"] = original.clone();
    input["incoming"]["relation"] = json!("repeat");
    advance_update(&mut input, 900, 1, "2026-09-09T03:00:00Z");
    input["incoming"]["source_relation"] = json!("unrelated");
    let repeated: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(repeated["observation"], original["observation"]);
    let mut older = problem_update();
    older["existing"] = repeated;
    older["existing"]["observation"]["started_at"] = json!("2026-09-09T04:00:00Z");
    older["existing"]["observation"]["run_id"] = json!(950);
    older["incoming"]["operation_id"] = json!("older");
    older["incoming"]["diagnosis"]["summary"] = json!("Older additional evidence");
    _ = execute(&older.to_string()).unwrap();
}
