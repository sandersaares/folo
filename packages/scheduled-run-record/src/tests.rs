#![allow(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "synthetic fixture sizes and paths are fixed; direct indexing keeps assertions readable"
)]

use std::io;
use std::num::NonZero;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};

use crate::canonical::{canonicalize, digest};
use crate::evidence::{Evidence, Identity, InvalidRecordError};
use crate::pages::{BODY_LIMIT, Comment, PAGE_BYTES, decode, paginate, prepare_pages};
use crate::record::{PageReference, Record, Revision, merge, render, restore};
use crate::{ReadInputError, WriteOutputError, run};

pub(crate) fn fixture() -> Value {
    // Synthetic successful observation; IDs and commit names only distinguish fixture roles.
    json!({
        "repository": {"id": 123, "name": "owner/repository"},
        "workflow": {
            "id": 456,
            "name": "Scheduled validation",
            "path": ".github/workflows/scheduled.yml"
        },
        "run_id": 789,
        "attempt": {
            "run_attempt": 1,
            "run_number": 42,
            "created_at": "2026-09-09T01:00:00Z",
            "started_at": "2026-09-09T01:00:01Z",
            "completed_at": "2026-09-09T01:00:02Z",
            "workflow_conclusion": "success",
            "run_sha": "a".repeat(40),
            "controller_sha": "b".repeat(40),
            "manifest": {"validated": true},
            "plan": {"entries": ["unit"]},
            "validated_no_work": false,
            "results": [{"outcome": "passed", "checker": "unit", "extra": ["b", "a"]}],
            "jobs": [{
                "id": 101, "name": "Check", "status": "completed", "conclusion": "success",
                "steps": [{
                    "number": 1, "name": "Execute", "status": "completed", "conclusion": "success"
                }]
            }],
            "evidence_gaps": []
        }
    })
}

pub(crate) fn evidence(value: Value) -> Evidence {
    serde_json::from_value(value).unwrap()
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "test requests are ephemeral JSON fixtures consumed by this assertion helper"
)]
fn call(request: Value) -> Value {
    let mut output = Vec::new();
    run(request.to_string().as_bytes(), &mut output).unwrap();
    serde_json::from_slice(&output).unwrap()
}

pub(crate) fn comments(value: Value, first_id: u64) -> (Identity, Vec<Comment>) {
    let evidence = evidence(value).validate().unwrap();
    let comments = prepare_pages(&evidence)
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(offset, page)| Comment {
            id: NonZero::new(first_id + u64::try_from(offset).unwrap()).unwrap(),
            body: page.body,
        })
        .collect();
    (evidence.identity, comments)
}

pub(crate) fn record(value: Value, first_id: u64) -> Record {
    let evidence = evidence(value).validate().unwrap();
    let pages = prepare_pages(&evidence)
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(offset, page)| PageReference {
            operation_id: page.operation_id,
            id: NonZero::new(first_id + u64::try_from(offset).unwrap()).unwrap(),
        })
        .collect();
    Record {
        identity: evidence.identity,
        revisions: vec![Revision {
            digest: evidence.digest,
            should_report: evidence.should_report,
            evidence: evidence.evidence,
            pages,
        }],
    }
}

#[test]
fn no_plan_setup_failure_remains_reportable() {
    let mut value = fixture();
    value["attempt"]["manifest"] = Value::Null;
    value["attempt"]["plan"] = Value::Null;
    value["attempt"]["results"] = json!([]);
    value["attempt"]["workflow_conclusion"] = json!("failure");
    value["attempt"]["jobs"][0]["conclusion"] = json!("failure");
    value["attempt"]["jobs"][0]["steps"][0]["conclusion"] = json!("failure");
    let response = call(json!({"op": "prepare", "evidence": value}));
    assert_eq!(response["should_report"], true);
    assert!(
        !response["evidence"]["attempt"]["evidence_gaps"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        response["evidence"]["attempt"]["jobs"][0]["steps"][0]["name"],
        "Execute"
    );
    assert_eq!(
        response["identity"],
        json!({"repository_id": 123, "workflow_id": 456, "run_id": 789})
    );
}

#[test]
fn prepare_normalizes_only_explicit_collector_gaps() {
    let mut value = fixture();
    let diagnostic = r"cannot read C:\report\random\check.log";
    value["attempt"]["evidence_gaps"] = json!([diagnostic]);
    value["attempt"]["results"][0]["diagnostic"] = json!(diagnostic);
    let response = call(json!({
        "op": "prepare",
        "evidence": value,
        "transient_path_prefixes": [r"C:\report\random"]
    }));
    assert_eq!(
        response["evidence"]["attempt"]["evidence_gaps"],
        json!([r"cannot read <reporter-local>\check.log"])
    );
    assert_eq!(
        response["evidence"]["attempt"]["results"][0]["diagnostic"],
        diagnostic
    );
    assert_eq!(response["should_report"], true);
    assert!(
        response["evidence"]
            .get("transient_path_prefixes")
            .is_none()
    );
}

#[test]
fn failed_jobs_require_intake() {
    unsuccessful_observations("job");
}

#[test]
fn failed_steps_require_intake() {
    unsuccessful_observations("step");
}

#[test]
fn failed_results_require_intake() {
    unsuccessful_observations("result");
}

fn unsuccessful_observations(field: &str) {
    let cases = if cfg!(miri) {
        // Miri covers each status branch; native tests expand the malformed-outcome variants.
        vec![json!("failure")]
    } else {
        vec![json!("failure"), json!("unknown"), Value::Null]
    };
    for changed in cases {
        let mut value = fixture();
        match field {
            "job" => value["attempt"]["jobs"][0]["conclusion"] = changed,
            "step" => value["attempt"]["jobs"][0]["steps"][0]["conclusion"] = changed,
            "result" => value["attempt"]["results"][0]["outcome"] = changed,
            _ => unreachable!(),
        }
        assert!(evidence(value).validate().unwrap().should_report);
    }
}

#[test]
fn mixed_jobs_retain_logs_references_and_all_results() {
    let mut value = fixture();
    let mut failed = value["attempt"]["jobs"][0].clone();
    failed["id"] = json!(102);
    failed["conclusion"] = json!("failure");
    failed["log"] = json!({
        "url": "https://api.github.com/repos/owner/repository/actions/jobs/102/logs",
        "excerpt": "non-ASCII evidence 🧪\n", "bytes": 99999,
        "truncated": true, "excerpt_truncated": true,
        "artifact_refs": [{"id": 12}, {"id": 11}]
    });
    value["attempt"]["jobs"]
        .as_array_mut()
        .unwrap()
        .push(failed);
    value["attempt"]["results"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "outcome": "findings", "checker": "miri", "diagnostics": ["problem", "problem"]
        }));
    let evidence = evidence(value).validate().unwrap();
    let normalized: Value = serde_json::from_str(&evidence.canonical).unwrap();
    let attempt = &normalized["attempt"];
    assert!(evidence.should_report);
    assert_eq!(attempt["jobs"].as_array().unwrap().len(), 2);
    assert_eq!(attempt["results"].as_array().unwrap().len(), 2);
    let log = attempt["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|job| job.get("log").filter(|log| !log.is_null()))
        .unwrap();
    assert_eq!(log["excerpt"], "non-ASCII evidence 🧪\n");
    assert_eq!(log["truncated"], true);
    assert_eq!(log["artifact_refs"].as_array().unwrap().len(), 2);
}

#[test]
fn normalization_is_deterministic_and_retains_duplicates() {
    let mut one = fixture();
    one["attempt"]["observations"] = json!([
        {"b": [3, 1, 1], "a": "same"},
        {"b": [3, 1, 1], "a": "same"},
        {"a": "other"}
    ]);
    let mut other = one.clone();
    other["attempt"]["observations"] = json!([
        {"a": "same", "b": [3, 1, 1]},
        {"a": "same", "b": [3, 1, 1]},
        {"a": "other"}
    ]);
    let one = evidence(one).validate().unwrap();
    let other = evidence(other).validate().unwrap();
    assert_eq!(one.digest, other.digest);
    assert_eq!(one.canonical, other.canonical);
    let normalized: Value = serde_json::from_str(&one.canonical).unwrap();
    assert_eq!(
        normalized["attempt"]["observations"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(canonicalize(normalized.clone()), normalized);
    assert_eq!(canonicalize(json!([2, 1, 1])), json!([2, 1, 1]));
}

#[test]
fn job_pagination_order_does_not_change_the_digest() {
    let mut first = fixture();
    first["attempt"]["jobs"][0]["id"] = json!(10);
    let mut job = first["attempt"]["jobs"][0].clone();
    job["id"] = json!(2);
    first["attempt"]["jobs"].as_array_mut().unwrap().push(job);
    let mut second = first.clone();
    second["attempt"]["jobs"].as_array_mut().unwrap().reverse();
    let first = evidence(first).validate().unwrap();
    let second = evidence(second).validate().unwrap();
    assert_eq!(first.digest, second.digest);
    let normalized: Value = serde_json::from_str(&first.canonical).unwrap();
    assert_eq!(normalized["attempt"]["jobs"][0]["id"], 2);
}

#[test]
fn steps_results_and_gaps_sort_only_their_explicit_inventories() {
    let mut first = fixture();
    first["attempt"]["jobs"][0]["steps"][0]["number"] = json!(10);
    let mut step = first["attempt"]["jobs"][0]["steps"][0].clone();
    step["number"] = json!(2);
    first["attempt"]["jobs"][0]["steps"]
        .as_array_mut()
        .unwrap()
        .push(step);
    first["attempt"]["results"][0]["check_id"] = json!("check-z");
    let mut result = first["attempt"]["results"][0].clone();
    result["check_id"] = json!("check-a");
    first["attempt"]["results"]
        .as_array_mut()
        .unwrap()
        .push(result);
    first["attempt"]["evidence_gaps"] = json!(["z", "a", "a"]);

    let first = evidence(first).validate().unwrap();
    let normalized: Value = serde_json::from_str(&first.canonical).unwrap();
    assert_eq!(normalized["attempt"]["jobs"][0]["steps"][0]["number"], 2);
    assert_eq!(normalized["attempt"]["results"][0]["check_id"], "check-a");
    assert_eq!(
        normalized["attempt"]["results"][0]["extra"],
        json!(["b", "a"])
    );
    assert_eq!(
        normalized["attempt"]["evidence_gaps"],
        json!(["a", "a", "z"])
    );
}

#[test]
#[cfg_attr(
    miri,
    ignore = "multi-stage JSON publication roundtrip; nested-array and inventory primitives cover Miri"
)]
fn replay_argument_order_survives_prepare_restore_and_changes_the_digest() {
    let mut original = fixture();
    let argv = json!([
        "cargo",
        "test",
        "--package",
        "example",
        "--",
        "--exact",
        "test_name"
    ]);
    original["attempt"]["results"][0]["replay"] = json!({
        "argv": argv,
        "flags": ["--release", "--locked"],
        "extension": {"phases": ["second", "first"]}
    });
    let prepared = call(json!({"op": "prepare", "evidence": original}));
    assert_eq!(
        prepared["evidence"]["attempt"]["results"][0]["replay"]["argv"],
        argv
    );
    let comments: Vec<_> = prepared["pages"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(index, page)| json!({"id": index + 1, "body": page["body"]}))
        .collect();
    let restored = call(json!({
        "op": "restore", "identity": prepared["identity"], "comments": comments
    }));
    let replay = &restored["record"]["revisions"][0]["evidence"]["attempt"]["results"][0]["replay"];
    assert_eq!(replay["argv"], argv);
    assert_eq!(replay["flags"], json!(["--release", "--locked"]));
    assert_eq!(replay["extension"]["phases"], json!(["second", "first"]));
    let mut reordered = prepared["evidence"].clone();
    reordered["attempt"]["results"][0]["replay"]["argv"]
        .as_array_mut()
        .unwrap()
        .swap(0, 1);
    let reordered = call(json!({"op": "prepare", "evidence": reordered}));
    assert_ne!(prepared["digest"], reordered["digest"]);
}

#[test]
fn digest_uses_standard_sha256_without_a_newline() {
    // Standard SHA-256 known-answer vector pins the interoperable algorithm and hex encoding.
    assert_eq!(
        digest("abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn same_evidence_is_idempotent_and_lost_response_duplicates_reconcile() {
    let (identity, mut first) = comments(fixture(), 900);
    let mut second = first.clone();
    for comment in &mut second {
        comment.id = NonZero::new(comment.id.get() - 800).unwrap();
    }
    first.extend(second);
    first.reverse();
    let result = restore(identity, first).unwrap();
    assert!(result.incomplete_revisions.is_empty());
    assert_eq!(result.record.revisions.len(), 1);
    assert_eq!(result.record.revisions[0].pages[0].id.get(), 100);
}

#[test]
fn merging_identical_revisions_is_idempotent() {
    let record = record(fixture(), 100);
    let merged = merge(record.clone(), record.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(&merged).unwrap(),
        serde_json::to_value(&record).unwrap()
    );
}

#[test]
#[cfg_attr(
    miri,
    ignore = "multi-revision end-to-end publication and merging; single revisions cover Miri paths"
)]
fn changed_same_attempt_appends_revision_and_green_rerun_preserves_failures() {
    let mut failed = fixture();
    failed["attempt"]["workflow_conclusion"] = json!("failure");
    let mut changed = failed.clone();
    changed["attempt"]["evidence_gaps"] = json!(["late log unavailable"]);
    let mut green = fixture();
    green["attempt"]["run_attempt"] = json!(2);
    let latest = record(green, 300);
    let latest = merge(latest, record(changed, 200)).unwrap();
    let latest = merge(latest, record(failed, 100)).unwrap();
    assert_eq!(latest.revisions.len(), 3);
    assert_eq!(
        latest
            .revisions
            .last()
            .unwrap()
            .evidence
            .attempt
            .run_attempt
            .get(),
        2
    );
    assert_eq!(
        latest
            .revisions
            .iter()
            .filter(|revision| revision.should_report)
            .count(),
        2
    );
    let rendered = serde_json::to_value(render(latest).unwrap()).unwrap();
    assert!(
        rendered["body"]
            .as_str()
            .unwrap()
            .contains("Recorded attempts: 2")
    );
    assert!(
        rendered["body"]
            .as_str()
            .unwrap()
            .contains("Complete evidence revisions: 3")
    );
}

#[test]
fn validated_no_work_does_not_create_issue_but_cannot_hide_failures() {
    let mut value = no_work();
    assert!(!evidence(value.clone()).validate().unwrap().should_report);
    value["attempt"]["plan"] = Value::Null;
    let error = evidence(value).validate().unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

fn no_work() -> Value {
    let mut value = fixture();
    value["attempt"]["validated_no_work"] = json!(true);
    value["attempt"]["plan"] = json!({"entries": []});
    value["attempt"]["results"] = json!([]);
    value["attempt"]["workflow_conclusion"] = json!("skipped");
    value["attempt"]["jobs"][0]["conclusion"] = json!("skipped");
    value["attempt"]["jobs"][0]["steps"] = json!([]);
    value
}

#[test]
fn validated_no_work_cannot_hide_failures_or_gaps() {
    let mut value = no_work();
    value["attempt"]["evidence_gaps"] = json!(["missing API page"]);
    assert!(evidence(value.clone()).validate().unwrap().should_report);
    value["attempt"]["evidence_gaps"] = json!([]);
    value["attempt"]["jobs"][0]["conclusion"] = json!("failure");
    assert!(evidence(value.clone()).validate().unwrap().should_report);
}

#[test]
fn malformed_identity_is_rejected_without_output() {
    let cases = [
        ("/repository/id", json!(0)),
        ("/repository/name", json!("invalid")),
        ("/workflow/id", json!(-1)),
        ("/workflow/path", json!("../scheduled.yml")),
        ("/run_id", json!("789")),
        ("/attempt/run_attempt", json!(0)),
        ("/attempt/run_sha", json!("branch")),
        ("/attempt/controller_sha", json!("abcd")),
        ("/attempt/created_at", json!("not a date")),
        ("/attempt/manifest", json!("not a manifest")),
    ];
    // Parsing every malformed representation repeatedly is native coverage; Miri retains a
    // typed numeric identity rejection and the workflow-path validation branch.
    let cases: Vec<_> = if cfg!(miri) {
        cases
            .into_iter()
            .filter(|(pointer, _)| matches!(*pointer, "/repository/id" | "/workflow/path"))
            .collect()
    } else {
        cases.into()
    };
    for (pointer, invalid) in cases {
        let mut value = fixture();
        *value.pointer_mut(pointer).unwrap() = invalid;
        let mut output = Vec::new();
        _ = run(
            json!({"op":"prepare", "evidence": value})
                .to_string()
                .as_bytes(),
            &mut output,
        )
        .unwrap_err();
        assert!(output.is_empty());
    }
}

#[test]
fn missing_metadata_is_reportable_not_silently_accepted_as_green() {
    let pointers = [
        "/attempt/completed_at",
        "/attempt/started_at",
        "/attempt/workflow_conclusion",
        "/attempt/jobs/0/id",
        "/attempt/jobs/0/name",
        "/attempt/jobs/0/status",
        "/attempt/jobs/0/steps",
        "/attempt/jobs/0/steps/0/number",
    ];
    let pointers = if cfg!(miri) {
        // One absent timestamp and empty steps retain interpreter coverage without repeating
        // the entire evidence normalization for every API property permutation.
        &pointers[..1]
    } else {
        &pointers
    };
    for pointer in pointers {
        let mut value = fixture();
        *value.pointer_mut(pointer).unwrap() = Value::Null;
        assert!(evidence(value).validate().unwrap().should_report);
    }
    let mut value = fixture();
    value["attempt"]["jobs"][0]["steps"] = json!([]);
    assert!(evidence(value).validate().unwrap().should_report);
}

#[test]
fn small_pages_roundtrip_unicode_and_marker_like_log_text() {
    let canonical = json!({"log": "🧪<!-- scheduled-run-evidence:v1 -->\n```"}).to_string();
    let identity = evidence(fixture()).identity();
    // A tiny fragment forces splits inside Unicode without a production-sized Miri workload.
    let pages = paginate(
        &identity,
        NonZero::new(1).unwrap(),
        &digest(&canonical),
        &canonical,
        7,
    )
    .unwrap();
    let mut restored = Vec::new();
    for (index, page) in pages.iter().enumerate() {
        let decoded = decode(&page.body).unwrap().unwrap();
        assert_eq!(decoded.header.page.get(), index + 1);
        assert_eq!(decoded.header.page_count.get(), pages.len());
        assert!(page.body.len() <= BODY_LIMIT);
        restored.extend(decoded.bytes);
    }
    assert_eq!(restored, canonical.as_bytes());
}

#[test]
#[cfg_attr(
    miri,
    ignore = "production-sized GitHub pagination; small Unicode pagination is covered"
)]
fn production_pages_publish_only_complete_lossless_revisions() {
    let mut value = fixture();
    value["attempt"]["large_observation"] = json!("🧪".repeat(PAGE_BYTES));
    let (identity, all) = comments(value, 100);
    assert!(all.len() > 1);
    let mut partial = all.clone();
    _ = partial.pop().unwrap();
    let partial = restore(identity.clone(), partial).unwrap();
    assert!(partial.record.revisions.is_empty());
    assert_eq!(partial.incomplete_revisions.len(), 1);
    let restored = restore(identity, all).unwrap();
    assert!(restored.incomplete_revisions.is_empty());
    assert_eq!(restored.record.revisions.len(), 1);
    assert_eq!(
        restored.record.revisions[0].evidence.attempt.extra["large_observation"],
        "🧪".repeat(PAGE_BYTES)
    );
}

#[test]
fn unrelated_triage_comments_are_ignored_and_wrong_run_pages_are_rejected() {
    let (identity, mut pages) = comments(fixture(), 100);
    pages.push(Comment {
        id: NonZero::new(999).unwrap(),
        body: "[Copilot speaking]\n<!-- scheduled-run-triage:v1 {} -->".to_owned(),
    });
    assert_eq!(
        restore(identity.clone(), pages.clone())
            .unwrap()
            .record
            .revisions
            .len(),
        1
    );
    let wrong = Identity {
        run_id: NonZero::new(999).unwrap(),
        ..identity
    };
    let error = restore(wrong, pages).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn unsupported_page_schema_is_rejected() {
    let (_, pages) = comments(fixture(), 100);
    let body = pages[0]
        .body
        .replace("\"schema_version\":1", "\"schema_version\":2");
    let error = decode(&body).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn conflicting_duplicate_delivery_cannot_replace_evidence() {
    let (identity, mut pages) = comments(fixture(), 100);
    let mut duplicate = pages[0].clone();
    duplicate.body.push(' ');
    pages.push(duplicate);
    let error = restore(identity, pages).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn changed_payload_cannot_keep_the_previous_digest() {
    let (identity, mut pages) = comments(fixture(), 100);
    let original = decode(&pages[0].body).unwrap().unwrap().bytes;
    let changed = String::from_utf8(original.clone())
        .unwrap()
        .replace("success", "failure");
    pages[0].body = pages[0]
        .body
        .replace(&STANDARD.encode(original), &STANDARD.encode(changed));
    let error = restore(identity, pages).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn record_validation_rejects_missing_and_misbound_references() {
    let valid = record(fixture(), 100);
    let mut invalid = valid.clone();
    invalid.revisions[0].pages.clear();
    _ = invalid.validate().unwrap_err();
    if !cfg!(miri) {
        // Native validation expands malformed-reference permutations after the representative
        // missing-reference check; each permutation otherwise rehashes the same full fixture.
        let mut invalid = valid.clone();
        invalid.revisions[0].pages[0].operation_id.push('x');
        _ = invalid.validate().unwrap_err();
        let mut invalid = valid;
        invalid.revisions[0].digest.push('0');
        _ = invalid.validate().unwrap_err();
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "repeated JSON protocol roundtrips; individual prepare, restore, merge and render tests cover Miri"
)]
fn operation_protocol_roundtrips_record_and_bounded_root_body() {
    let prepared = call(json!({"op": "prepare", "evidence": fixture()}));
    let comments: Vec<_> = prepared["pages"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(index, page)| json!({"id": index + 1, "body": page["body"]}))
        .collect();
    let restored =
        call(json!({"op": "restore", "identity": prepared["identity"], "comments": comments}));
    let validated = call(json!({"op": "validate", "record": restored["record"]}));
    let merged = call(json!({"op": "merge", "record": validated, "incoming": restored["record"]}));
    assert_eq!(merged, restored["record"]);
    let rendered = call(json!({"op": "render", "record": merged}));
    assert_eq!(rendered["title"], "Scheduled validation failed");
    assert_eq!(rendered["issue_marker"], prepared["issue_marker"]);
    assert!(rendered["body"].as_str().unwrap().len() < BODY_LIMIT);
}

#[test]
fn empty_record_renders_bounded_pending_root() {
    let record = Record {
        identity: evidence(fixture()).identity(),
        revisions: Vec::new(),
    };
    let rendered = serde_json::to_value(render(record).unwrap()).unwrap();
    assert_eq!(rendered["title"], "Scheduled validation failed");
    assert_eq!(
        rendered["issue_marker"],
        "<!-- scheduled-run:v1 {\"repository_id\":123,\"run_id\":789,\"schema_version\":1,\"workflow_id\":456} -->"
    );
    assert!(
        rendered["body"]
            .as_str()
            .unwrap()
            .contains("publication is pending")
    );
    assert!(rendered["body"].as_str().unwrap().len() < BODY_LIMIT);
}

#[test]
fn io_errors_do_not_emit_success_shaped_json() {
    let mut output = Vec::new();
    let error = run([0xff].as_slice(), &mut output).unwrap_err();
    _ = error.find_source::<ReadInputError>().unwrap();
    _ = error.find_source::<io::Error>().unwrap();
    assert!(output.is_empty());
    let request = json!({"op": "prepare", "evidence": fixture()}).to_string();
    let error = run(request.as_bytes(), [].as_mut_slice()).unwrap_err();
    _ = error.find_source::<WriteOutputError>().unwrap();
    _ = error.find_source::<io::Error>().unwrap();
}
