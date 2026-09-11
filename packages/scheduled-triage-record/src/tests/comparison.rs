// Native protocol coverage for comparison contracts.
use serde_json::{Value, json};

use crate::protocol::execute;
use crate::tests::support::*;

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
