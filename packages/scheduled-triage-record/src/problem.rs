use ohno::AppError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::analysis::validate_citations;
use crate::protocol::require;
use crate::scope::Scope;

/// Evidence-backed diagnosis supplied by AI, independently of canonical issue identity.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Diagnosis {
    pub(crate) title: String,
    pub(crate) summary: String,
    pub(crate) cause: String,
    pub(crate) category: Category,
    pub(crate) repair_disposition: RepairDisposition,
    pub(crate) repair_reason: String,
    pub(crate) citations: Vec<String>,
    pub(crate) scope: Vec<Scope>,
}

impl Diagnosis {
    pub(crate) fn validate(&self, evidence: &Value) -> Result<(), AppError> {
        require(
            [&self.title, &self.summary, &self.cause, &self.repair_reason]
                .iter()
                .all(|text| !text.trim().is_empty()),
            "diagnosis and repair disposition need reasoning",
        )?;
        validate_citations(&self.citations, evidence)?;
        require(
            !self.scope.is_empty(),
            "preserve the complete affected scope",
        )?;
        for scope in &self.scope {
            scope.validate(evidence)?;
        }
        require(
            self.repair_disposition != RepairDisposition::Actionable
                || matches!(self.category, Category::Code | Category::Nondeterminism),
            "operator recovery is not a source repair",
        )?;
        Ok(())
    }
}

/// Categories describe the diagnosed operation, not the job or checker that happened to fail.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Category {
    Code,
    Infrastructure,
    Operator,
    Nondeterminism,
    Unknown,
}

/// Future repair handoff data; even actionable records cannot bypass the admission capability gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum RepairDisposition {
    Actionable,
    NeedsHuman,
    OperatorRecovery,
    Unresolved,
}
