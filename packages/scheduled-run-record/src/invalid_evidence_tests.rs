#![allow(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "fixed synthetic JSON paths and sizes make direct test assertions clearer"
)]

use std::num::NonZero;
use std::string::FromUtf8Error;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};

use crate::evidence::InvalidRecordError;
use crate::pages::{BODY_LIMIT, Comment, PAGE_BYTES, decode, paginate, validate_body_size};
use crate::record::{Record, merge, render, restore};
use crate::tests::{comments, evidence, fixture, record};

#[test]
fn empty_job_inventory_is_reportable() {
    let mut value = fixture();
    value["attempt"]["jobs"] = json!([]);
    let actual = evidence(value).validate().unwrap();
    assert!(actual.should_report);
    assert!(!actual.evidence.attempt.evidence_gaps.is_empty());
}

#[test]
fn unavailable_job_log_is_retained_as_a_gap() {
    let mut value = fixture();
    value["attempt"]["jobs"][0]["log"] =
        json!({"url":"https://example.invalid/log","unavailable":true});
    let actual = evidence(value).validate().unwrap();
    assert!(actual.should_report);
    assert!(!actual.evidence.attempt.evidence_gaps.is_empty());
}

#[test]
fn incomplete_log_capture_is_reportable() {
    for log in [
        json!({"url":"https://example.invalid/log","excerpt":"diagnostic"}),
        json!({"url":"https://example.invalid/log","bytes":12}),
    ] {
        let mut value = fixture();
        value["attempt"]["jobs"][0]["log"] = log;
        let actual = evidence(value).validate().unwrap();
        assert!(actual.should_report);
        assert!(!actual.evidence.attempt.evidence_gaps.is_empty());
    }
}

#[test]
fn non_https_log_reference_is_rejected() {
    let mut value = fixture();
    value["attempt"]["jobs"][0]["log"] =
        json!({"url":"file:///local-log","excerpt":"diagnostic","bytes":12});
    let error = evidence(value).validate().unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn pagination_rejects_invalid_boundaries() {
    let identity = evidence(fixture()).identity();
    for (text, size) in [("{}", 0), ("{}", PAGE_BYTES + 1), ("", PAGE_BYTES)] {
        let error = paginate(
            &identity,
            NonZero::new(1).unwrap(),
            &"a".repeat(64),
            text,
            size,
        )
        .unwrap_err();
        _ = error.find_source::<InvalidRecordError>().unwrap();
    }
}

#[test]
fn malformed_page_delimiters_are_rejected() {
    let (_, pages) = comments(fixture(), 100);
    let body = &pages[0].body;
    for damaged in [
        body.replace(" -->\n", "\n"),
        body.replace("\n```base64\n", "\n"),
        body.strip_suffix("\n```\n").unwrap().to_owned(),
    ] {
        _ = decode(&damaged).unwrap_err();
    }
}

#[test]
fn altered_page_prose_cannot_keep_a_valid_payload() {
    let (_, mut pages) = comments(fixture(), 100);
    pages[0].body = pages[0]
        .body
        .replace("Hosted evidence only", "Unverified evidence");
    let error = decode(&pages[0].body).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn github_body_size_is_enforced_at_the_shared_boundary() {
    // The same guard protects generated page/index bodies and stored GitHub comments.
    validate_body_size(&"x".repeat(BODY_LIMIT)).unwrap();
    let error = validate_body_size(&"x".repeat(BODY_LIMIT + 1)).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn record_rejects_a_revision_from_another_run() {
    let mut invalid = record(fixture(), 100);
    invalid.identity.run_id = NonZero::new(999).unwrap();
    let error = invalid.validate().unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn record_rejects_a_forged_reporting_verdict() {
    let mut invalid = record(fixture(), 100);
    invalid.revisions[0].should_report = true;
    let error = invalid.validate().unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn record_rejects_duplicate_revisions() {
    let mut invalid = record(fixture(), 100);
    invalid.revisions.push(invalid.revisions[0].clone());
    let error = invalid.validate().unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn record_rejects_reused_comment_ids() {
    let mut invalid = record(reference_fixture(), 100);
    let mut next = reference_fixture();
    next["attempt"]["run_attempt"] = json!(2);
    invalid.revisions.extend(record(next, 100).revisions);
    let error = invalid.validate().unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

fn reference_fixture() -> Value {
    // Reference ownership does not depend on a full checker result or step inventory.
    // Keep a valid partial-evidence revision small enough for repeated Miri validation.
    let mut value = fixture();
    value["attempt"]["jobs"] = json!([]);
    value["attempt"]["results"] = json!([]);
    value["attempt"]["plan"] = json!({});
    value["attempt"]["manifest"] = json!({});
    value["attempt"]["validated_no_work"] = json!(true);
    value
}

#[test]
fn restoration_rejects_inconsistent_page_counts() {
    let validated = evidence(fixture()).validate().unwrap();
    let (identity, mut pages) = comments(fixture(), 100);
    let alternative = paginate(
        &identity,
        validated.run_attempt,
        &validated.digest,
        &validated.canonical,
        validated.canonical.len().div_ceil(2),
    )
    .unwrap();
    pages.push(Comment {
        id: NonZero::new(101).unwrap(),
        body: alternative[0].body.clone(),
    });
    let error = restore(identity, pages).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

fn replace_payload(comment: &mut Comment, replacement: &[u8]) {
    let original = decode(&comment.body).unwrap().unwrap();
    comment.body = comment.body.replace(
        &STANDARD.encode(original.bytes),
        &STANDARD.encode(replacement),
    );
}

#[test]
fn duplicate_page_coordinates_cannot_name_different_bytes() {
    let (identity, mut pages) = comments(fixture(), 100);
    let mut duplicate = pages[0].clone();
    duplicate.id = NonZero::new(101).unwrap();
    replace_payload(&mut duplicate, b"different bytes");
    pages.push(duplicate);
    let error = restore(identity, pages).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn empty_evidence_fragment_is_rejected() {
    let (_, mut pages) = comments(fixture(), 100);
    replace_payload(&mut pages[0], &[]);
    let error = decode(&pages[0].body).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "production base64 page-size boundary; the empty-fragment guard is covered under Miri"
)]
fn oversized_evidence_fragment_is_rejected() {
    let (_, mut pages) = comments(fixture(), 100);
    replace_payload(&mut pages[0], &vec![b'x'; PAGE_BYTES + 1]);
    let error = decode(&pages[0].body).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn malformed_page_header_and_encoding_preserve_parse_errors() {
    let (_, pages) = comments(fixture(), 100);
    let (_, remainder) = pages[0].body.split_once(" -->\n").unwrap();
    let bad_header =
        format!("[Copilot speaking]\n<!-- scheduled-run-evidence:v1 {{}} -->\n{remainder}");
    let error = decode(&bad_header).unwrap_err();
    _ = error.find_source::<serde_json::Error>().unwrap();
    let original = decode(&pages[0].body).unwrap().unwrap().bytes;
    let bad_encoding = pages[0]
        .body
        .replace(&STANDARD.encode(original), "not base64!");
    let error = decode(&bad_encoding).unwrap_err();
    _ = error.find_source::<base64::DecodeError>().unwrap();
}

#[test]
fn restoration_rejects_noncanonical_fragment_boundaries() {
    let validated = evidence(fixture()).validate().unwrap();
    let pages = paginate(
        &validated.identity,
        validated.run_attempt,
        &validated.digest,
        &validated.canonical,
        validated.canonical.len().div_ceil(2),
    )
    .unwrap();
    let comments = pages
        .into_iter()
        .zip(1..)
        .map(|(page, id)| Comment {
            id: NonZero::new(id).unwrap(),
            body: page.body,
        })
        .collect();
    let error = restore(validated.identity, comments).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn restoration_preserves_invalid_utf8_and_json_errors() {
    let (identity, mut pages) = comments(fixture(), 100);
    replace_payload(&mut pages[0], &[0xff]);
    let error = restore(identity, pages).unwrap_err();
    _ = error.find_source::<FromUtf8Error>().unwrap();
    let (identity, mut pages) = comments(fixture(), 100);
    replace_payload(&mut pages[0], b"{");
    let error = restore(identity, pages).unwrap_err();
    _ = error.find_source::<serde_json::Error>().unwrap();
}

#[test]
fn merge_rejects_different_runs() {
    let first = Record {
        identity: evidence(fixture()).identity(),
        revisions: Vec::new(),
    };
    let mut second = first.clone();
    second.identity.run_id = NonZero::new(999).unwrap();
    let error = merge(first, second).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn merge_rejects_comment_ids_reused_between_revisions() {
    let first = record(reference_fixture(), 100);
    let mut other = reference_fixture();
    other["attempt"]["run_attempt"] = json!(2);
    let error = merge(first, record(other, 100)).unwrap_err();
    _ = error.find_source::<InvalidRecordError>().unwrap();
}

#[test]
fn publication_checkpoint_binds_comment_references_not_just_run_identity() {
    let first = record(reference_fixture(), 100);
    let mut second = first.clone();
    second.revisions[0].pages[0].id = NonZero::new(200).unwrap();
    let first: Value = serde_json::to_value(render(first).unwrap()).unwrap();
    let second: Value = serde_json::to_value(render(second).unwrap()).unwrap();
    assert_eq!(first["issue_marker"], second["issue_marker"]);
    assert_ne!(first["index_digest"], second["index_digest"]);
}
