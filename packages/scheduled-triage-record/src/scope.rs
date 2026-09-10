use ohno::AppError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::analysis::validate_citations;
use crate::protocol::require;

/// One supported operation/scope, retaining absent attribution for prerequisite-only failures.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Scope {
    pub(crate) operation: String,
    /// Package attribution can be absent before package discovery or execution.
    pub(crate) package: Option<String>,
    /// Planning/setup failures may precede selection of a catalog check.
    pub(crate) check_id: Option<String>,
    /// Repository-level planning failures need not execute on a target platform.
    pub(crate) platform: Option<String>,
    /// A checker replay is absent for infrastructure/operator operations.
    pub(crate) replay: Option<Value>,
    pub(crate) citations: Vec<String>,
}

impl Scope {
    pub(crate) fn same_verification_scope(&self, other: &Self) -> bool {
        self.package == other.package
            && self.check_id == other.check_id
            && self.platform == other.platform
            && self.replay == other.replay
    }

    pub(crate) fn validate(&self, evidence: &Value) -> Result<(), AppError> {
        require(!self.operation.trim().is_empty(), "scope operation")?;
        for text in [&self.package, &self.check_id, &self.platform]
            .into_iter()
            .flatten()
        {
            require(!text.trim().is_empty(), "empty scope attribution")?;
        }
        require(
            self.replay.as_ref().is_none_or(Value::is_object),
            "replay must remain typed data",
        )?;
        validate_citations(&self.citations, evidence)
    }
}
