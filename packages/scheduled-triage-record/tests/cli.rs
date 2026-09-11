//! Exercises the actual utility's JSON and failure streams without native App or GitHub access.

use std::io::Write;
use std::process::{Command, Output, Stdio};

use serde_json::{Value, json};
use testing::with_watchdog;

fn invoke(input: Vec<u8>) -> Output {
    with_watchdog(move || {
        let mut child = Command::new(env!("CARGO_BIN_EXE_scheduled-triage-record"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&input).unwrap();
        child.wait_with_output().unwrap()
    })
}

#[test]
#[cfg_attr(miri, ignore = "exercises the actual utility process and OS pipes")]
fn inspects_incomplete_evidence_without_losing_its_gaps() {
    let input = json!({
        "op":"inspect",
        "evidence":{
            "repository":{"id":123,"name":"owner/repository"},
            "workflow":{"id":456,"name":"Full deep validation","path":".github/workflows/full-deep-validation.yml"},
            "run_id":789,
            "attempt":{
                "run_attempt":1,"run_number":42,"workflow_conclusion":"failure",
                "run_sha":"a".repeat(40),"controller_sha":"a".repeat(40),
                "results":[],"jobs":[],"evidence_gaps":[]
            }
        }
    });
    let output = invoke(input.to_string().into_bytes());
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response.get("should_report"), Some(&Value::Bool(true)));
    assert!(
        !response
            .pointer("/evidence/attempt/evidence_gaps")
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
#[cfg_attr(miri, ignore = "exercises the actual utility process and OS pipes")]
fn invalid_or_unauthorized_operations_never_emit_success_json() {
    for input in [
        b"{".to_vec(),
        br#"{"op":"create_pr"}"#.to_vec(),
        br#"{"op":"confirm_repair"}"#.to_vec(),
        vec![0xff],
    ] {
        let output = invoke(input);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}
