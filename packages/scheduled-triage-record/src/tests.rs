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
    let evidence = &source["evidence"];
    let mut jobs = source["evidence"]["attempt"]["jobs"].clone();
    for job in jobs.as_array_mut().unwrap() {
        job["run_id"] = evidence["run_id"].clone();
        job["run_attempt"] = evidence["attempt"]["run_attempt"].clone();
        job["head_sha"] = evidence["attempt"]["controller_sha"].clone();
    }
    let api = json!({
        "repository_id":evidence["repository"]["id"],"workflow_id":evidence["workflow"]["id"],
        "run_id":evidence["run_id"],"run_attempt":evidence["attempt"]["run_attempt"],
        "controller_sha":evidence["attempt"]["controller_sha"],
        "source_sha":evidence.pointer("/attempt/manifest/source_sha"),
        "started_at":evidence["attempt"]["started_at"],"created_at":evidence["attempt"]["created_at"],
        "workflow_conclusion":source["evidence"]["attempt"]["workflow_conclusion"],
        "total_count":jobs.as_array().unwrap().len(),"jobs":jobs
    });
    let fingerprint = shared(&json!({"op":"fingerprint","value":api})).unwrap();
    json!({"schema_version":1,"api_digest":fingerprint["digest"],"api_evidence":api,"supporting_revisions":[]})
}

fn empty_workflow_request(conclusion: &str) -> Value {
    let mut input = request();
    let mut evidence = input["evidence"].clone();
    evidence["attempt"]["jobs"] = json!([]);
    evidence["attempt"]["results"] = json!([]);
    evidence["attempt"]["manifest"] = Value::Null;
    evidence["attempt"]["plan"] = Value::Null;
    evidence["attempt"]["workflow_conclusion"] = json!(conclusion);
    let prepared = shared(&json!({"op":"prepare","evidence":evidence})).unwrap();
    input["evidence"] = prepared["evidence"].clone();
    input["basis"] = basis(&prepared);
    input["analysis"]["revision"]["digest"] = prepared["digest"].clone();
    input["analysis"]["jobs"] = json!([]);
    input["analysis"]["results"] = json!([]);
    input["analysis"]["problems"] = json!([]);
    let disposition = json!({
        "kind":if conclusion == "cancelled" {"cancelled"} else {"blocked"},
        "explanation":"The workflow stopped before any job was instantiated; no checker or source failure is inferred",
        "citations":["/api_evidence/workflow_conclusion","/api_evidence/jobs"],
        "problem_keys":[]
    });
    input["analysis"]["workflow"] = disposition.clone();
    input["analysis"]["gaps"] = Value::Array(
        prepared["evidence"]["attempt"]["evidence_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|gap| json!({"gap":gap,"disposition":disposition}))
            .collect(),
    );
    input
}

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

fn supported_request() -> Value {
    let mut input = request();
    let full = source();
    let mut partial = full["evidence"].clone();
    partial["attempt"]["jobs"] = json!([]);
    partial["attempt"]["evidence_gaps"] =
        json!(["The collector could not retrieve the entire job inventory"]);
    let partial = shared(&json!({"op":"prepare","evidence":partial})).unwrap();
    input["evidence"] = partial["evidence"].clone();
    input["analysis"]["revision"]["digest"] = partial["digest"].clone();
    input["basis"]["supporting_revisions"] =
        json!([{"digest":full["digest"],"evidence":full["evidence"]}]);
    let citation = "/supporting_revisions/0/evidence/attempt/jobs/0/log/excerpt";
    for pointer in [
        "/analysis/jobs/0/disposition/citations",
        "/analysis/jobs/0/steps/0/disposition/citations",
        "/analysis/results/0/disposition/citations",
        "/analysis/problems/0/diagnosis/citations",
        "/analysis/problems/0/diagnosis/scope/0/citations",
    ] {
        *input.pointer_mut(pointer).unwrap() = json!([citation]);
    }
    let disposition = input["analysis"]["jobs"][0]["disposition"].clone();
    input["analysis"]["results"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "index":0,"source_digest":full["digest"],"disposition":disposition
        }));
    input["analysis"]["gaps"] = Value::Array(
        partial["evidence"]["attempt"]["evidence_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|gap| json!({"gap":gap,"disposition":disposition}))
            .collect(),
    );
    input["analysis"]["support_dispositions"] =
        json!({full["digest"].as_str().unwrap():disposition});
    input
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn fuller_committed_same_attempt_support_preserves_the_primary_claim() {
    let input = supported_request();
    let output: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    assert_eq!(
        output["revision"]["digest"],
        input["analysis"]["revision"]["digest"]
    );
    assert_ne!(
        output["revision"]["digest"],
        input["basis"]["supporting_revisions"][0]["digest"]
    );
    assert_eq!(output["jobs"][0]["job_id"], 101);
    let mut missing_reason = input;
    missing_reason["analysis"]["support_dispositions"] = json!({});
    _ = execute(&missing_reason.to_string()).unwrap_err();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn supporting_attempt_source_and_status_conflicts_are_not_overwritten() {
    for (pointer, value) in [
        ("/attempt/run_attempt", json!(2)),
        ("/attempt/manifest/source_sha", json!("b".repeat(40))),
        ("/attempt/jobs/0/conclusion", json!("success")),
    ] {
        let mut input = supported_request();
        let old_digest = input["basis"]["supporting_revisions"][0]["digest"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut support = input["basis"]["supporting_revisions"][0]["evidence"].clone();
        *support.pointer_mut(pointer).unwrap() = value;
        let prepared = shared(&json!({"op":"prepare","evidence":support})).unwrap();
        input["basis"]["supporting_revisions"][0] =
            json!({"digest":prepared["digest"],"evidence":prepared["evidence"]});
        let disposition = input["analysis"]["support_dispositions"][&old_digest].clone();
        input["analysis"]["support_dispositions"] =
            json!({prepared["digest"].as_str().unwrap():disposition});
        input["analysis"]["results"][1]["source_digest"] = prepared["digest"].clone();
        _ = execute(&input.to_string()).unwrap_err();
    }
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
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
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

fn advance_update(input: &mut Value, run_id: u64, attempt: u64, stamp: &str) {
    let mut evidence = input["incoming"]["primary_evidence"].clone();
    evidence["run_id"] = json!(run_id);
    evidence["attempt"]["run_attempt"] = json!(attempt);
    evidence["attempt"]["created_at"] = json!(stamp);
    evidence["attempt"]["started_at"] = json!(stamp);
    evidence["attempt"]["completed_at"] = json!(stamp);
    let prepared = shared(&json!({"op":"prepare","evidence":evidence})).unwrap();
    let api_basis = basis(&prepared);
    input["incoming"]["primary_evidence"] = prepared["evidence"].clone();
    input["incoming"]["revision"]["digest"] = prepared["digest"].clone();
    input["incoming"]["revision"]["run_id"] = json!(run_id);
    input["incoming"]["revision"]["run_attempt"] = json!(attempt);
    input["incoming"]["revision"]["issue_number"] = json!(run_id);
    input["incoming"]["basis"] = api_basis.clone();
    input["incoming"]["observation"] = json!({
        "source_sha":api_basis["api_evidence"]["source_sha"],
        "run_id":run_id,"run_attempt":attempt,"created_at":stamp,"started_at":stamp
    });
}

fn resolved_problem() -> Value {
    let mut input = problem_update();
    let mut problem: Value = serde_json::from_str(&execute(&input.to_string()).unwrap()).unwrap();
    advance_update(&mut input, 800, 1, "2026-09-09T02:00:00Z");
    problem["status"] = json!("resolved");
    problem["resolution"] = json!({
        "generation":1,"observation":input["incoming"]["observation"],
        "evidence":[{
            "kind":"registered-repair","issue_number":99,"generation":1,
            "finding_id":"f".repeat(64),"pr_number":11,"merge_commit_sha":"a".repeat(40),
            "reporter_record_digest":"b".repeat(64)
        }],
        "explanation":"The registered repair and applicable hosted confirmation establish resolution"
    });
    problem["observation"] = input["incoming"]["observation"].clone();
    problem
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

    let mut late = problem_update();
    late["existing"] = recurring.clone();
    late["incoming"]["operation_id"] = json!("late-history");
    late["incoming"]["relation"] = json!("historical");
    late["incoming"]["diagnosis"]["summary"] = json!("Additional description of the older symptom");
    let updated: Value = serde_json::from_str(&execute(&late.to_string()).unwrap()).unwrap();
    assert_eq!(updated["generation"], 2);
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
fn every_original_gap_and_support_disposition_remains_required() {
    let mut input = request();
    input["analysis"]["results"][0]["source_digest"] = json!("absent");
    _ = execute(&input.to_string()).unwrap_err();
    let mut input = supported_request();
    input["analysis"]["gaps"][0]["source_digest"] = json!("absent");
    _ = execute(&input.to_string()).unwrap_err();
    let mut input = supported_request();
    input["analysis"]["gaps"].as_array_mut().unwrap().pop();
    _ = execute(&input.to_string()).unwrap_err();
    let mut input = supported_request();
    let duplicate = input["analysis"]["gaps"][0].clone();
    input["analysis"]["gaps"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    _ = execute(&input.to_string()).unwrap_err();
    let mut input = supported_request();
    input["analysis"]["gaps"][0]["gap"] = json!("not an observed gap");
    _ = execute(&input.to_string()).unwrap_err();
    let mut input = supported_request();
    let disposition = input["analysis"]["jobs"][0]["disposition"].clone();
    input["analysis"]["support_dispositions"]["foreign"] = disposition;
    _ = execute(&input.to_string()).unwrap_err();
}

fn indexed_request() -> Value {
    let mut input = request();
    input["index"]["entries"] = json!([{
        "issue_number":99,"generation":2,"scope_revision":1,"record_digest":"record",
        "summary":{},"full_read_digest":"full"
    }]);
    input["analysis"]["considered_issues"] = json!([99]);
    input["analysis"]["problems"][0]["matching"] = json!({
        "kind":"existing","issue_number":99,"expected_generation":2,"expected_scope_revision":1,
        "target_generation":2,"record_digest":"record","full_read_digest":"full",
        "relation":"repeat","reason":"The evidence establishes the same cause"
    });
    input
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn candidate_matching_preserves_generation_and_full_index_requirements() {
    for (pointer, value) in [
        (
            "/analysis/problems/0/matching/expected_generation",
            json!(1),
        ),
        (
            "/analysis/problems/0/matching/expected_scope_revision",
            json!(2),
        ),
        ("/analysis/problems/0/matching/target_generation", json!(1)),
        ("/analysis/problems/0/matching/issue_number", json!(100)),
        ("/analysis/problems/0/matching/reason", json!("")),
    ] {
        let mut input = indexed_request();
        *input.pointer_mut(pointer).unwrap() = value;
        _ = execute(&input.to_string()).unwrap_err();
    }
    let mut input = indexed_request();
    let mut duplicate = input["analysis"]["problems"][0].clone();
    duplicate["key"] = json!("another-proposal");
    input["analysis"]["problems"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    _ = execute(&input.to_string()).unwrap_err();
    let mut input = indexed_request();
    input["analysis"]["problems"][0]["matching"] = json!({
        "kind":"new","reason":"Independent archive corruption",
        "closest_candidates":[{"issue_number":99,"full_read_digest":"full","reason":"Access failure is a different cause"}]
    });
    _ = execute(&input.to_string()).unwrap();
    input["analysis"]["problems"][0]["matching"]["closest_candidates"][0]["reason"] = json!("");
    _ = execute(&input.to_string()).unwrap_err();
    input["analysis"]["problems"][0]["matching"]["closest_candidates"] = json!([]);
    _ = execute(&input.to_string()).unwrap_err();
    input["analysis"]["problems"][0]["matching"] =
        json!({"kind":"ambiguous","reason":"","candidates":[]});
    _ = execute(&input.to_string()).unwrap_err();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn completion_basis_rejects_changed_snapshots_foreign_jobs_and_duplicate_steps() {
    for (pointer, value) in [
        ("/basis/schema_version", json!(2)),
        ("/basis/api_evidence/repository_id", json!(124)),
        ("/basis/api_evidence/source_sha", json!("b".repeat(40))),
        ("/basis/api_evidence/jobs/0/run_id", json!(900)),
        ("/basis/api_evidence/jobs/0/steps/0/number", json!(0)),
        ("/basis/api_evidence/workflow_conclusion", json!("success")),
    ] {
        let mut input = request();
        *input.pointer_mut(pointer).unwrap() = value;
        input["basis"]["api_digest"] = shared(&json!({
            "op":"fingerprint","value":input["basis"]["api_evidence"]
        }))
        .unwrap()["digest"]
            .clone();
        _ = execute(&input.to_string()).unwrap_err();
    }
    let mut input = request();
    input["basis"]["api_digest"] = json!("changed");
    _ = execute(&input.to_string()).unwrap_err();
    let mut input = request();
    input["evidence"]["attempt"]
        .as_object_mut()
        .unwrap()
        .remove("validated_no_work");
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

#[test]
#[cfg_attr(
    miri,
    ignore = "full producer snapshots and repeated canonical hashing exceed the interpreter budget; typed guards retain Miri coverage"
)]
fn incomplete_primary_metadata_is_retained_alongside_the_complete_api_snapshot() {
    for pointer in [
        "/attempt/jobs/0/status",
        "/attempt/jobs/0/steps",
        "/attempt/workflow_conclusion",
    ] {
        let mut input = request();
        let mut evidence = input["evidence"].clone();
        *evidence.pointer_mut(pointer).unwrap() = Value::Null;
        let prepared = shared(&json!({"op":"prepare","evidence":evidence})).unwrap();
        input["evidence"] = prepared["evidence"].clone();
        input["analysis"]["revision"]["digest"] = prepared["digest"].clone();
        input["analysis"]["status"] = json!("in-progress");
        input["analysis"]["reason"] = json!("Account for original collection gaps");
        input["analysis"]["problems"][0]["diagnosis"]["scope"][0]["citations"] =
            json!(["/api_evidence/jobs/0/steps/0"]);
        _ = execute(&input.to_string()).unwrap();
    }
    let mut input = supported_request();
    let old = input["basis"]["supporting_revisions"][0]["digest"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut support = input["basis"]["supporting_revisions"][0]["evidence"].clone();
    support["attempt"]["manifest"] = Value::Null;
    let prepared = shared(&json!({"op":"prepare","evidence":support})).unwrap();
    input["basis"]["supporting_revisions"][0] =
        json!({"digest":prepared["digest"],"evidence":prepared["evidence"]});
    let disposition = input["analysis"]["support_dispositions"][&old].clone();
    input["analysis"]["support_dispositions"] =
        json!({prepared["digest"].as_str().unwrap():disposition});
    input["analysis"]["results"][1]["source_digest"] = prepared["digest"].clone();
    input["analysis"]["status"] = json!("in-progress");
    input["analysis"]["reason"] = json!("Supporting collection is incomplete");
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
