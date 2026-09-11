// Shared producer-shaped JSON builders for the native protocol test modules.
use serde_json::{Value, json};

use crate::protocol::{execute, shared};

pub(crate) fn source() -> Value {
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

pub(crate) fn diagnosis() -> Value {
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

pub(crate) fn basis(source: &Value) -> Value {
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

pub(crate) fn empty_workflow_request(conclusion: &str) -> Value {
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

pub(crate) fn supported_request() -> Value {
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

pub(crate) fn request() -> Value {
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

pub(crate) fn actionable_request() -> Value {
    let mut input = request();
    input["analysis"]["problems"][0]["diagnosis"]["category"] = json!("code");
    input["analysis"]["problems"][0]["diagnosis"]["repair_disposition"] = json!("actionable");
    input
}

pub(crate) fn duplicate_actionable_request() -> Value {
    let mut input = actionable_request();
    let diagnosis = input["analysis"]["problems"][0]["diagnosis"].clone();
    input["analysis"]["results"][0]["disposition"]["kind"] = json!("duplicate");
    input["index"]["entries"] = json!([{
        "issue_number":99,"generation":1,"scope_revision":1,"record_digest":"full",
        "summary":diagnosis,"full_read_digest":"full",
        "prior_support":{
            "full_read_digest":"full","current_diagnosis":diagnosis,
            "occurrences":[{"generation":1,"diagnosis":diagnosis}]
        }
    }]);
    input["analysis"]["considered_issues"] = json!([99]);
    input["analysis"]["problems"][0]["matching"] = json!({
        "kind":"existing","issue_number":99,"expected_generation":1,"expected_scope_revision":1,
        "target_generation":1,"record_digest":"full","full_read_digest":"full",
        "relation":"repeat","reason":"The full prior record establishes the same cause and scope"
    });
    input
}

pub(crate) fn problem_update() -> Value {
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

pub(crate) fn advance_update(input: &mut Value, run_id: u64, attempt: u64, stamp: &str) {
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

pub(crate) fn resolved_problem() -> Value {
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

pub(crate) fn indexed_request() -> Value {
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
