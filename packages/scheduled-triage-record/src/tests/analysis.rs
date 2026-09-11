// Native protocol coverage for analysis contracts.
use serde_json::{Value, json};

use crate::protocol::{execute, shared};
use crate::tests::support::*;

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn proved_empty_workflow_failure_or_cancellation_is_not_an_unavailable_inventory() {
    for conclusion in ["cancelled", "failure"] {
        let input = empty_workflow_request(conclusion);
        let output: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
        assert_eq!(output["status"], "complete");
        assert!(output["jobs"].as_array().unwrap().is_empty());
        assert!(output["problems"].as_array().unwrap().is_empty());
    }
    let mut input = empty_workflow_request("cancelled");
    input["analysis"]["workflow"] = Value::Null;
    _ = execute(&input.to_string()).unwrap_err();
    input["analysis"]["status"] = json!("blocked");
    input["analysis"]["reason"] = json!("Cancellation needs explicit accounting");
    _ = execute(&input.to_string()).unwrap();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn empty_success_unknown_conclusions_and_inconsistent_totals_cannot_complete_failure_analysis() {
    for conclusion in ["success", "unknown"] {
        _ = execute(&empty_workflow_request(conclusion).to_string()).unwrap_err();
    }
    let mut input = empty_workflow_request("cancelled");
    input["basis"]["api_evidence"]["total_count"] = json!(1);
    input["basis"]["api_digest"] = shared(&json!({
        "op":"fingerprint","value":input["basis"]["api_evidence"]
    }))
    .unwrap()["digest"]
        .clone();
    _ = execute(&input.to_string()).unwrap_err();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn serializes_complete_setup_analysis_without_manufacturing_a_code_repair() {
    let output: Value = serde_json::from_str(&execute(&request().to_string()).unwrap()).unwrap();
    assert_eq!(output["status"], "complete");
    assert_eq!(
        output["problems"][0]["diagnosis"]["repair_disposition"],
        "operator-recovery"
    );
    assert!(output["problems"][0]["diagnosis"]["scope"][0]["replay"].is_null());
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and canonical hashing exceed the interpreter budget; typed support tests retain Miri coverage"
)]
fn actionable_diagnoses_need_actionable_links_not_only_consequences_or_new_duplicates() {
    for kind in ["infrastructure", "blocked", "cancelled", "duplicate"] {
        let mut input = actionable_request();
        input["analysis"]["jobs"][0]["disposition"]["kind"] = json!(kind);
        input["analysis"]["jobs"][0]["steps"][0]["disposition"]["kind"] = json!(kind);
        input["analysis"]["results"][0]["disposition"]["kind"] = json!(kind);
        _ = execute(&input.to_string()).unwrap_err();
        input["analysis"]["results"][0]["disposition"]["kind"] = json!("actionable");
        _ = execute(&input.to_string()).unwrap();
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and canonical hashing exceed the interpreter budget; typed support tests retain Miri coverage"
)]
fn duplicate_only_existing_actionability_needs_full_record_cause_and_occurrence_scope_proof() {
    let input = duplicate_actionable_request();
    _ = execute(&input.to_string()).unwrap();
    for (pointer, value) in [
        ("/index/entries/0/prior_support", Value::Null),
        ("/index/entries/0/full_read_digest", Value::Null),
        (
            "/index/entries/0/prior_support/full_read_digest",
            json!("stale"),
        ),
        (
            "/index/entries/0/prior_support/current_diagnosis/repair_disposition",
            json!("needs-human"),
        ),
        (
            "/index/entries/0/prior_support/current_diagnosis/cause",
            json!("A different cause"),
        ),
        (
            "/index/entries/0/prior_support/occurrences/0/generation",
            json!(2),
        ),
        (
            "/index/entries/0/prior_support/occurrences/0/diagnosis/repair_disposition",
            json!("unresolved"),
        ),
        (
            "/analysis/problems/0/diagnosis/scope/0/operation",
            json!("An additional execution qualifier"),
        ),
        (
            "/analysis/problems/0/diagnosis/cause",
            json!("A newly asserted source defect"),
        ),
        (
            "/analysis/results/0/disposition/kind",
            json!("infrastructure"),
        ),
    ] {
        let mut invalid = input.clone();
        *invalid.pointer_mut(pointer).unwrap() = value;
        _ = execute(&invalid.to_string()).unwrap_err();
    }
    let mut human = input;
    human["index"]["entries"][0]["prior_support"]["current_diagnosis"]["repair_disposition"] =
        json!("needs-human");
    human["analysis"]["problems"][0]["diagnosis"]["repair_disposition"] = json!("needs-human");
    _ = execute(&human.to_string()).unwrap();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and canonical hashing exceed the interpreter budget; typed support tests retain Miri coverage"
)]
fn current_actionable_evidence_can_establish_expanded_scope_without_borrowing_prior_support() {
    let mut input = duplicate_actionable_request();
    input["analysis"]["problems"][0]["diagnosis"]["scope"][0]["operation"] =
        json!("An independently evidenced additional execution qualifier");
    _ = execute(&input.to_string()).unwrap_err();
    input["analysis"]["jobs"][0]["steps"][0]["disposition"]["kind"] = json!("actionable");
    _ = execute(&input.to_string()).unwrap();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn every_failed_job_step_and_result_requires_a_disposition() {
    for pointer in [
        "/analysis/jobs",
        "/analysis/jobs/0/steps",
        "/analysis/results",
    ] {
        let mut input = request();
        *input.pointer_mut(pointer).unwrap() = json!([]);
        _ = execute(&input.to_string()).unwrap_err();
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn rejects_changed_revision_unknown_links_and_missing_citations() {
    for (pointer, value) in [
        ("/analysis/revision/digest", json!("foreign")),
        (
            "/analysis/jobs/0/disposition/problem_keys",
            json!(["unknown"]),
        ),
        (
            "/analysis/problems/0/diagnosis/citations",
            json!(["/absent"]),
        ),
        ("/analysis/problems/0/diagnosis/scope", json!([])),
        (
            "/analysis/problems/0/diagnosis/repair_disposition",
            json!("actionable"),
        ),
    ] {
        let mut input = request();
        *input.pointer_mut(pointer).unwrap() = value;
        _ = execute(&input.to_string()).unwrap_err();
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn unfinished_analysis_keeps_reason_and_cannot_impersonate_completion() {
    let mut input = request();
    input["analysis"]["status"] = json!("blocked");
    input["analysis"]["reason"] = json!("More diagnostics are required");
    input["analysis"]["jobs"][0]["disposition"]["kind"] = json!("unresolved");
    _ = execute(&input.to_string()).unwrap();
    input["analysis"]["status"] = json!("complete");
    _ = execute(&input.to_string()).unwrap_err();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn one_job_can_explain_independent_problems_without_hash_based_grouping() {
    let mut input = request();
    let mut independent = input["analysis"]["problems"][0].clone();
    independent["key"] = json!("independent");
    independent["diagnosis"]["cause"] = json!("A separately evidenced unavailable dependency");
    input["analysis"]["problems"]
        .as_array_mut()
        .unwrap()
        .push(independent);
    input["analysis"]["jobs"][0]["disposition"]["problem_keys"] =
        json!(["download", "independent"]);
    let output: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(output["problems"].as_array().unwrap().len(), 2);
}

#[test]
fn does_not_accept_process_repair_or_unknown_protocol_actions() {
    for input in [
        json!({"op":"create_pr"}),
        json!({"op":"edit_source","path":"source.rs"}),
        json!({"op":"confirm_repair","generation":1}),
        json!({"op":"validate_analysis","unknown":true}),
    ] {
        _ = execute(&input.to_string()).unwrap_err();
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn malformed_analysis_decisions_fail_without_partial_acceptance() {
    for (pointer, value) in [
        ("/analysis/schema_version", json!(2)),
        ("/analysis/analysis_id", json!("")),
        ("/analysis/revision/repository_id", json!(124)),
        ("/analysis/status", json!("blocked")),
        ("/analysis/problems/0/key", json!("")),
        ("/analysis/problems/0/diagnosis/title", json!("")),
        (
            "/analysis/problems/0/diagnosis/scope/0/operation",
            json!(""),
        ),
        ("/analysis/jobs/0/disposition/explanation", json!("")),
        ("/analysis/jobs/0/job_id", json!(102)),
        ("/analysis/jobs/0/steps/0/number", json!(2)),
        ("/analysis/results/0/index", json!(9)),
        ("/analysis/index_digest", json!("changed")),
        ("/index/complete", json!(false)),
        ("/analysis/problems/0/matching/reason", json!("")),
    ] {
        let mut input = request();
        *input.pointer_mut(pointer).unwrap() = value;
        _ = execute(&input.to_string()).unwrap_err();
    }
    for pointer in [
        "/analysis/jobs",
        "/analysis/jobs/0/steps",
        "/analysis/results",
        "/analysis/problems",
    ] {
        let mut input = request();
        let items = input.pointer_mut(pointer).unwrap().as_array_mut().unwrap();
        items.push(items.first().unwrap().clone());
        _ = execute(&input.to_string()).unwrap_err();
    }
    let mut input = request();
    let mut unlinked = input["analysis"]["problems"][0].clone();
    unlinked["key"] = json!("unlinked");
    input["analysis"]["problems"]
        .as_array_mut()
        .unwrap()
        .push(unlinked);
    _ = execute(&input.to_string()).unwrap_err();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn successful_or_skipped_jobs_do_not_hide_a_failed_step() {
    for conclusion in ["success", "skipped"] {
        let mut input = request();
        input["evidence"]["attempt"]["jobs"][0]["conclusion"] = json!(conclusion);
        let prepared = shared(&json!({"op":"prepare","evidence":input["evidence"]})).unwrap();
        input["evidence"] = prepared["evidence"].clone();
        input["analysis"]["revision"]["digest"] = prepared["digest"].clone();
        input["basis"] = basis(&prepared);
        _ = execute(&input.to_string()).unwrap();
        input["analysis"]["jobs"] = json!([]);
        _ = execute(&input.to_string()).unwrap_err();
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn empty_workflows_reject_source_repair_claims_and_non_workflow_dispositions() {
    let mut input = empty_workflow_request("failure");
    input["analysis"]["workflow"]["kind"] = json!("actionable");
    _ = execute(&input.to_string()).unwrap_err();
    input["analysis"]["workflow"]["kind"] = json!("blocked");
    let mut problem = request()["analysis"]["problems"][0].clone();
    problem["diagnosis"]["category"] = json!("code");
    problem["diagnosis"]["repair_disposition"] = json!("actionable");
    problem["diagnosis"]["citations"] = json!(["/api_evidence/workflow_conclusion"]);
    problem["diagnosis"]["scope"][0]["citations"] = json!(["/api_evidence/jobs"]);
    input["analysis"]["problems"] = json!([problem]);
    input["analysis"]["workflow"]["problem_keys"] = json!(["download"]);
    _ = execute(&input.to_string()).unwrap_err();
    input["analysis"]["problems"][0]["diagnosis"]["repair_disposition"] = json!("needs-human");
    _ = execute(&input.to_string()).unwrap();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn problem_dispositions_need_links_and_replays_remain_structured() {
    let mut input = request();
    input["analysis"]["jobs"][0]["disposition"]["problem_keys"] = json!([]);
    _ = execute(&input.to_string()).unwrap_err();
    let mut input = request();
    input["analysis"]["problems"][0]["diagnosis"]["scope"][0]["replay"] =
        json!("not typed replay data");
    _ = execute(&input.to_string()).unwrap_err();
}
