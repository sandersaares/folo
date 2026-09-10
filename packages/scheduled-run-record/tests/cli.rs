//! Tests the JSON protocol through the actual utility executable and its standard streams.

#![allow(
    clippy::indexing_slicing,
    reason = "tests assert fixed JSON protocol fields; missing fields are test failures"
)]

use std::io::Write;
use std::process::{Command, Output, Stdio};

use serde_json::{Value, json};
use testing::with_watchdog;

fn invoke(input: Vec<u8>) -> Output {
    with_watchdog(move || {
        let mut child = Command::new(env!("CARGO_BIN_EXE_scheduled-run-record"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&input).unwrap();
        child.wait_with_output().unwrap()
    })
}

fn request(value: &Value) -> Value {
    let output = invoke(value.to_string().into_bytes());
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
#[cfg_attr(miri, ignore = "exercises the real utility process and OS pipes")]
fn prepares_restores_and_renders_a_setup_failure() {
    // A setup failure can lack all checker artifacts and timestamps; those omissions must
    // remain explicit evidence rather than prevent durable intake.
    let prepared = request(&json!({
        "op":"prepare",
        "evidence":{
            "repository":{"id":123,"name":"owner/repository"},
            "workflow":{"id":456,"name":"Full deep validation","path":".github/workflows/full-deep-validation.yml"},
            "run_id":789,
            "attempt":{
                "run_attempt":1,"run_number":42,"workflow_conclusion":"failure",
                "run_sha":"a".repeat(40),"controller_sha":"b".repeat(40),
                "results":[],"jobs":[],"evidence_gaps":[]
            }
        }
    }));
    assert_eq!(prepared["should_report"], true);
    let comments: Vec<_> = prepared["pages"]
        .as_array()
        .unwrap()
        .iter()
        .zip(1_u64..)
        .map(|(page, id)| json!({"id":id,"body":page["body"]}))
        .collect();
    let restored = request(&json!({
        "op":"restore","identity":prepared["identity"],"comments":comments
    }));
    assert!(
        restored["incomplete_revisions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let revisions = restored["record"]["revisions"].as_array().unwrap();
    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions.first().unwrap()["digest"], prepared["digest"]);
    let rendered = request(&json!({"op":"render","record":restored["record"]}));
    assert_eq!(rendered["title"], "Deep validation failed");
    assert!(
        rendered["body"]
            .as_str()
            .unwrap()
            .starts_with("[Copilot speaking]")
    );
}

#[test]
#[cfg_attr(miri, ignore = "exercises the real utility process and OS pipes")]
fn rejects_invalid_requests_without_success_output() {
    for input in [b"{".to_vec(), br#"{"op":"unknown"}"#.to_vec(), vec![0xff]] {
        let output = invoke(input);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}
