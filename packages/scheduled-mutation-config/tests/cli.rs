//! Exercises the actual controller process, including argument selection and JSON-only output.

#![allow(
    clippy::indexing_slicing,
    reason = "tests assert fixed JSON protocol fields; missing fields are test failures"
)]

use std::io::Write;
use std::process::{Command, Output, Stdio};

use serde_json::{Value, json};
use testing::with_watchdog;

fn invoke(arguments: &[&str], input: Vec<u8>) -> Output {
    let arguments: Vec<_> = arguments.iter().map(ToString::to_string).collect();
    with_watchdog(move || {
        let mut child = Command::new(env!("CARGO_BIN_EXE_scheduled-mutation-config"))
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        // Closing the finite input before collecting output lets the utility observe EOF.
        child.stdin.take().unwrap().write_all(&input).unwrap();
        child.wait_with_output().unwrap()
    })
}

#[test]
#[cfg_attr(miri, ignore = "exercises the real utility process and OS pipes")]
fn decodes_configuration_on_the_default_command_path() {
    let output = invoke(&[], b"test_tool = 'nextest'\nall_features = true".to_vec());
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual["test_tool"], "nextest");
    assert_eq!(actual["all_features"], true);
}

#[test]
#[cfg_attr(miri, ignore = "exercises the real utility process and OS pipes")]
fn selects_dependency_attestation_instead_of_toml_decoding() {
    let metadata = json!({
        "packages": [{
            "id":"helper", "name":"scheduled-mutation-config", "version":"0.0.0",
            "source":null, "dependencies":[]
        }],
        "workspace_members":["helper"],
        "resolve":{"nodes":[{"id":"helper","deps":[],"features":[]}]}
    });
    let output = invoke(
        &["--dependency-contract"],
        metadata.to_string().into_bytes(),
    );
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual, json!({"requirements":[],"packages":{}}));
}

#[test]
#[cfg_attr(miri, ignore = "exercises the real utility process and OS pipes")]
fn rejects_unknown_or_extra_arguments_without_success_output() {
    for arguments in [
        vec!["--unknown"],
        vec!["--dependency-contract", "unexpected"],
    ] {
        let output = invoke(&arguments, Vec::new());
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}

#[test]
#[cfg_attr(miri, ignore = "exercises the real utility process and OS pipes")]
fn reports_input_errors_without_emitting_partial_json() {
    for input in [b"features = [".to_vec(), vec![0xff]] {
        let output = invoke(&[], input);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
    let output = invoke(&["--dependency-contract"], b"{}".to_vec());
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}
