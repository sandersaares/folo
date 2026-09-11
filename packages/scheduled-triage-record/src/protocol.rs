use ohno::AppError;
use serde::Deserialize;
use serde_json::Value;

use crate::analysis::AnalysisRecord;
use crate::basis::EvidenceBasis;
use crate::comparison::ComparisonIndex;
use crate::lifecycle::{ProblemRecord, ProblemUpdate, update_problem};

/// Requests are data operations only; no process, file, GitHub or repair action is accepted.
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Inspect {
        evidence: Value,
    },
    ValidateAnalysis {
        evidence: Value,
        analysis: Box<AnalysisRecord>,
        index: ComparisonIndex,
        basis: Box<EvidenceBasis>,
    },
    UpdateProblem {
        existing: Option<Box<ProblemRecord>>,
        incoming: Box<ProblemUpdate>,
    },
}

pub(crate) fn execute(text: &str) -> Result<String, AppError> {
    let request: Request = serde_json::from_str(text).map_err(ParseRequestError::caused_by)?;
    match request {
        Request::Inspect { evidence } => scheduled_run_record::execute(
            &serde_json::json!({"op": "prepare", "evidence": evidence}).to_string(),
        ),
        Request::ValidateAnalysis {
            evidence,
            analysis,
            index,
            basis,
        } => {
            analysis.validate(&evidence, &index, &basis)?;
            serde_json::to_string(&analysis)
                .map_err(|error| EncodeResponseError::caused_by(error).into())
        }
        Request::UpdateProblem { existing, incoming } => {
            serde_json::to_string(&update_problem(existing.map(|record| *record), *incoming)?)
                .map_err(|error| EncodeResponseError::caused_by(error).into())
        }
    }
}

pub(crate) fn shared(request: &Value) -> Result<Value, AppError> {
    let output = scheduled_run_record::execute(&request.to_string())?;
    serde_json::from_str(&output).map_err(|error| ParseRequestError::caused_by(error).into())
}

pub(crate) fn require(condition: bool, requirement: &'static str) -> Result<(), AppError> {
    if condition {
        Ok(())
    } else {
        Err(InvalidRecordError::new(requirement).into())
    }
}

/// Identifies malformed or unsupported triage requests.
#[ohno::error]
#[display("cannot parse triage request")]
struct ParseRequestError;

/// Identifies a structural contract violation without replacing AI diagnosis.
#[ohno::error]
#[display("triage record requires {requirement}")]
struct InvalidRecordError {
    requirement: &'static str,
}

/// Identifies failure to serialize validated output.
#[ohno::error]
#[display("cannot encode triage response")]
struct EncodeResponseError;
