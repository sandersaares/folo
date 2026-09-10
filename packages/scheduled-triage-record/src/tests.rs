#![allow(
    clippy::indexing_slicing,
    reason = "fixed synthetic JSON fixtures make missing protocol fields test failures"
)]

use serde_json::{Value, json};

use crate::protocol::{execute, shared};

fn source() -> Value {
    // IDs distinguish synthetic repository/run/job roles; diagnostics are supplied decisions,
    // not a claim that deterministic tests establish an actual model's diagnosis quality.
    shared(&json!({
        "op":"prepare",
        "evidence":{
            "repository":{"id":123,"name":"owner/repository"},
            "workflow":{"id":456,"name":"Full deep validation","path":".github/workflows/full-deep-validation.yml"},
            "run_id":789,
            "attempt":{
                "run_attempt":1,"run_number":42,
                "created_at":"2026-09-09T01:00:00Z","started_at":"2026-09-09T01:00:01Z",
                "completed_at":"2026-09-09T01:00:02Z",
                "workflow_conclusion":"failure","run_sha":"a".repeat(40),"controller_sha":"a".repeat(40),
                "manifest":{"source_sha":"a".repeat(40)},"plan":{"run":true},
                "results":[{"check_id":"setup","outcome":"execution-error"}],
                "jobs":[{
                    "id":101,"name":"Setup","status":"completed","conclusion":"failure",
                    "steps":[{"number":1,"name":"Download","status":"completed","conclusion":"failure"}],
                    "log":{"url":"https://example.invalid/log","excerpt":"Dependency download failed","bytes":26}
                }],"evidence_gaps":[]
            }
        }
    })).unwrap()
}

fn diagnosis() -> Value {
    json!({
        "title":"Dependency download failure","summary":"Setup could not download dependencies",
        "cause":"The dependency source rejected the download","category":"infrastructure",
        "repair_disposition":"operator-recovery","repair_reason":"Restore dependency source access",
        "citations":["/attempt/jobs/0/log/excerpt"],
        "scope":[{
            "operation":"dependency download","package":null,"check_id":null,"platform":"linux",
            "replay":null,"citations":["/attempt/jobs/0/steps/0"]
        }]
    })
}

fn basis(source: &Value) -> Value {
    let mut jobs = source["evidence"]["attempt"]["jobs"].clone();
    for job in jobs.as_array_mut().unwrap() {
        job["run_id"] = json!(789);
        job["run_attempt"] = json!(1);
        job["head_sha"] = json!("a".repeat(40));
    }
    let api = json!({
        "repository_id":123,"workflow_id":456,"run_id":789,"run_attempt":1,
        "controller_sha":"a".repeat(40),"source_sha":"a".repeat(40),
        "started_at":"2026-09-09T01:00:01Z","created_at":"2026-09-09T01:00:00Z",
        "total_count":jobs.as_array().unwrap().len(),"jobs":jobs
    });
    let fingerprint = shared(&json!({"op":"fingerprint","value":api})).unwrap();
    json!({"schema_version":1,"api_digest":fingerprint["digest"],"api_evidence":api,"supporting_revisions":[]})
}

fn request() -> Value {
    let source = source();
    let disposition = json!({
        "kind":"infrastructure","explanation":"Execution was blocked by the failed download",
        "citations":["/attempt/jobs/0/log/excerpt"],"problem_keys":["download"]
    });
    json!({
        "op":"validate_analysis",
        "evidence":source["evidence"],
        "basis":basis(&source),
        "index":{"digest":"index","complete":true,"entries":[]},
        "analysis":{
            "schema_version":1,"analysis_id":"analysis","checkpoint":1,
            "revision":{"repository_id":123,"workflow_id":456,"run_id":789,"run_attempt":1,
                "digest":source["digest"],"issue_number":20},
            "status":"complete","considered_issues":[],"index_digest":"index","reason":"",
            "jobs":[{"job_id":101,"disposition":disposition,"steps":[{"number":1,"disposition":disposition}]}],
            "results":[{"index":0,"disposition":disposition}],"gaps":[],
            "problems":[{"key":"download","diagnosis":diagnosis(),
                "matching":{"kind":"new","reason":"No existing scheduled problem accounts for this failure",
                    "closest_candidates":[]}}]
        }
    })
}

#[test]
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
fn comparison_requires_the_complete_index_and_real_full_read_receipt() {
    let mut input = request();
    input["index"]["entries"] = json!([{
        "issue_number":99,"generation":1,"scope_revision":1,"record_digest":"record",
        "summary":{"cause":"dependency source"},"full_read_digest":"full"
    }]);
    _ = execute(&input.to_string()).unwrap_err();
    input["analysis"]["considered_issues"] = json!([99]);
    input["analysis"]["problems"][0]["matching"] = json!({
        "kind":"existing","issue_number":99,"expected_generation":1,"expected_scope_revision":1,
        "target_generation":1,"record_digest":"record","full_read_digest":"full",
        "relation":"repeat","reason":"The same dependency download operation and rejection recur"
    });
    _ = execute(&input.to_string()).unwrap();
    input["index"]["entries"][0]["full_read_digest"] = Value::Null;
    _ = execute(&input.to_string()).unwrap_err();
}

#[test]
fn ambiguous_matches_remain_blocked_without_a_competing_new_issue() {
    let mut input = request();
    input["index"]["entries"] = json!([{
        "issue_number":99,"generation":1,"scope_revision":1,"record_digest":"record",
        "summary":{},"full_read_digest":"full"
    }]);
    input["analysis"]["considered_issues"] = json!([99]);
    input["analysis"]["problems"][0]["matching"] = json!({
        "kind":"ambiguous","reason":"The failing remote operation is not yet established",
        "candidates":[{"issue_number":99,"full_read_digest":"full","reason":"Similar download symptom"}]
    });
    _ = execute(&input.to_string()).unwrap_err();
    input["analysis"]["status"] = json!("blocked");
    input["analysis"]["reason"] = json!("Additional logs are required");
    _ = execute(&input.to_string()).unwrap();
}

#[test]
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

fn problem_update() -> Value {
    let request = request();
    let analysis = request["analysis"].clone();
    json!({
        "op":"update_problem","existing":null,
        "incoming":{
            "issue_number":99,"target_generation":1,"revision":analysis["revision"],
            "diagnosis":diagnosis(),"relation":"repeat","source_relation":"identical",
            "operation_id":"publication",
            "primary_evidence":request["evidence"],"basis":request["basis"],
            "observation":{"source_sha":"a".repeat(40),"run_id":789,"run_attempt":1,
                "started_at":"2026-09-09T01:00:01Z","created_at":"2026-09-09T01:00:00Z"}
        }
    })
}

#[test]
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
fn closed_without_resolution_and_new_runs_without_ancestry_do_not_recur() {
    let mut input = problem_update();
    let problem: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    input["existing"] = problem;
    input["existing"]["status"] = json!("needs-human");
    input["incoming"]["operation_id"] = json!("new-publication");
    input["incoming"]["relation"] = json!("recurrence");
    input["incoming"]["source_relation"] = json!("descendant");
    input["incoming"]["revision"]["run_attempt"] = json!(2);
    input["incoming"]["observation"]["run_attempt"] = json!(2);
    _ = execute(&input.to_string()).unwrap_err();
}
