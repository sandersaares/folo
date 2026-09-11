// Native protocol coverage for basis contracts.
use serde_json::{Value, json};

use crate::protocol::{execute, shared};
use crate::tests::support::*;

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
