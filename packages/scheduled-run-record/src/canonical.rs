use std::collections::BTreeMap;
use std::fmt::Write;

use ohno::AppError;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Orders object properties without changing the meaning of preserved JSON arrays.
///
/// Argument lists and extension arrays carry order. Only the evidence schema may identify
/// particular collections as unordered; generic JSON normalization never makes that decision.
pub(crate) fn canonicalize(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| (key, canonicalize(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(array) => Value::Array(array.into_iter().map(canonicalize).collect()),
        scalar => scalar,
    }
}

/// Stably orders a schema-declared inventory while retaining all duplicate observations.
pub(crate) fn sort_observations<T: Serialize, K: Ord>(
    observations: &mut Vec<T>,
    identity: impl Fn(&T) -> K,
) -> Result<(), AppError> {
    if observations.len() < 2 {
        return Ok(());
    }
    let mut keyed = observations
        .drain(..)
        .map(|observation| {
            // Canonical bytes break ties between conflicting observations of the same ID,
            // without reordering arrays inside either observation.
            let key = (identity(&observation), json(&normalized(&observation)?)?);
            Ok((key, observation))
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    keyed.sort_by(|(left, _), (right, _)| left.cmp(right));
    observations.extend(keyed.into_iter().map(|(_, observation)| observation));
    Ok(())
}

pub(crate) fn json(value: &impl Serialize) -> Result<String, AppError> {
    serde_json::to_string(value).map_err(|error| EncodeError::caused_by(error).into())
}

pub(crate) fn normalized(value: &impl Serialize) -> Result<Value, AppError> {
    serde_json::to_value(value)
        .map(canonicalize)
        .map_err(|error| EncodeError::caused_by(error).into())
}

pub(crate) fn digest(text: &str) -> String {
    let mut hex = String::new();
    for byte in Sha256::digest(text.as_bytes()) {
        write!(hex, "{byte:02x}").expect("formatting into a String is infallible");
    }
    hex
}

/// Identifies failure to encode controller-owned protocol values.
#[ohno::error]
#[display("cannot encode run-record JSON")]
struct EncodeError;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn nested_arrays_keep_order_and_change_the_digest() {
        let first = canonicalize(json!({"replay": {"argv": ["cargo", "test", "-p", "example"]}}));
        let second = canonicalize(json!({"replay": {"argv": ["test", "cargo", "-p", "example"]}}));
        assert_ne!(digest(&first.to_string()), digest(&second.to_string()));
        assert_eq!(
            first,
            json!({"replay": {"argv": ["cargo", "test", "-p", "example"]}})
        );
    }

    #[test]
    fn unordered_inventory_sort_preserves_duplicates_and_extension_array_order() {
        let mut observations = vec![
            json!({"id": 10, "extension": ["last", "first"]}),
            json!({"id": 2, "extension": ["last", "first"]}),
            json!({"id": 2, "extension": ["last", "first"]}),
        ];
        sort_observations(&mut observations, |observation| {
            observation.get("id").unwrap().as_u64().unwrap()
        })
        .unwrap();
        assert_eq!(
            observations,
            [
                json!({"id": 2, "extension": ["last", "first"]}),
                json!({"id": 2, "extension": ["last", "first"]}),
                json!({"id": 10, "extension": ["last", "first"]}),
            ]
        );
    }
}
