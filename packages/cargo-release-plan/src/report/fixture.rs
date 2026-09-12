// Full producer-shaped reports keep artifact consumer tests tied to the wire contract.

use serde_json::{Value, json};

use crate::plan::SCHEMA_VERSION;
use crate::report::ReportFile;

pub(crate) fn package(name: &str, status: &str, consumer_contract: bool) -> Value {
    let version = if status == "pending-release" {
        "1.0.1"
    } else {
        "1.0.0"
    };
    let changed = if status == "unchanged" {
        json!([])
    } else {
        json!([{"source": "package", "path": "src/lib.rs", "change": "modified"}])
    };
    let count = usize::from(status != "unchanged");
    json!({
        "name": name,
        "declared_version": version,
        "status": status,
        "anchor": {"commit": "anchor", "version": "1.0.0"},
        "changed": changed,
        "stat": {"files": count, "insertions": count, "deletions": count},
        "dependencies": [],
        "dependents": [],
        "consumer_contract": consumer_contract
    })
}

pub(crate) fn report(packages: Vec<Value>) -> ReportFile {
    let packages = Value::Array(packages);
    let report: ReportFile = serde_json::from_value(json!({
        "schema_version": SCHEMA_VERSION,
        "head": "head",
        "packages": packages,
        "non_publishable_packages": [],
        "groups": {}
    }))
    .unwrap();
    report.validate().unwrap();
    report
}
