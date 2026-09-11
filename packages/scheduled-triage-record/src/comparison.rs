use std::collections::BTreeSet;
use std::num::NonZero;

use ohno::AppError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::problem::{Diagnosis, RepairDisposition};
use crate::protocol::require;

/// A complete API-derived index and full-read receipts supplied by the read-only helper.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComparisonIndex {
    pub(crate) digest: String,
    pub(crate) complete: bool,
    pub(crate) entries: Vec<IndexEntry>,
}

impl ComparisonIndex {
    pub(crate) fn validate(
        &self,
        digest: &str,
        considered: &[NonZero<u64>],
    ) -> Result<(), AppError> {
        require(
            self.complete && self.digest == digest && !digest.is_empty(),
            "comparison index is incomplete or changed",
        )?;
        let issues: BTreeSet<_> = self
            .entries
            .iter()
            .map(|entry| entry.issue_number)
            .collect();
        let considered_set: BTreeSet<_> = considered.iter().copied().collect();
        require(
            issues.len() == self.entries.len()
                && considered_set.len() == considered.len()
                && issues == considered_set,
            "analysis must consider the complete problem index",
        )
    }

    fn read(&self, number: NonZero<u64>, digest: &str) -> Result<&IndexEntry, AppError> {
        let entry = self
            .entries
            .iter()
            .find(|entry| entry.issue_number == number)
            .ok_or_else(|| ComparisonError::new("candidate is absent from the complete index"))?;
        require(
            entry.full_read_digest.as_deref() == Some(digest) && !digest.is_empty(),
            "plausible candidate must have a current complete read",
        )?;
        Ok(entry)
    }
}

/// Compact candidate data plus an optional receipt obtained only after fetching the full record.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexEntry {
    pub(crate) issue_number: NonZero<u64>,
    pub(crate) generation: NonZero<u64>,
    pub(crate) scope_revision: NonZero<u64>,
    pub(crate) record_digest: String,
    pub(crate) summary: Value,
    pub(crate) full_read_digest: Option<String>,
    /// The helper projects this from the verified full problem, not the discovery summary.
    pub(crate) prior_support: Option<PriorSupport>,
}

/// Factual occurrence diagnoses bound to the full record the model finished reading.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PriorSupport {
    full_read_digest: String,
    current_diagnosis: Diagnosis,
    occurrences: Vec<OccurrenceSupport>,
}

/// Retains the occurrence of a prior diagnosis so historical scope cannot authorize newer work.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OccurrenceSupport {
    generation: NonZero<u64>,
    diagnosis: Diagnosis,
}

/// The AI must state why candidates match or differ; the helper never chooses a cause.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum MatchDecision {
    New {
        reason: String,
        closest_candidates: Vec<CandidateComparison>,
    },
    Existing {
        issue_number: NonZero<u64>,
        expected_generation: NonZero<u64>,
        expected_scope_revision: NonZero<u64>,
        target_generation: NonZero<u64>,
        record_digest: String,
        full_read_digest: String,
        relation: Relation,
        reason: String,
    },
    Ambiguous {
        reason: String,
        candidates: Vec<CandidateComparison>,
    },
}

impl MatchDecision {
    pub(crate) fn supports_actionable(
        &self,
        diagnosis: &Diagnosis,
        index: &ComparisonIndex,
    ) -> Result<bool, AppError> {
        let Self::Existing {
            issue_number,
            target_generation,
            full_read_digest,
            ..
        } = self
        else {
            return Ok(false);
        };
        let entry = index.read(*issue_number, full_read_digest)?;
        let Some(support) = &entry.prior_support else {
            return Ok(false);
        };
        let same_cause = |prior: &Diagnosis| {
            prior.repair_disposition == RepairDisposition::Actionable
                && prior.category == diagnosis.category
                && prior.cause == diagnosis.cause
        };
        // Equality preserves an already stated claim; different causal wording remains an AI
        // assertion requiring current actionable support, not deterministic semantic matching.
        if support.full_read_digest != *full_read_digest
            || entry.record_digest != *full_read_digest
            || (*target_generation == entry.generation && !same_cause(&support.current_diagnosis))
        {
            return Ok(false);
        }
        Ok(diagnosis.scope.iter().all(|scope| {
            support
                .occurrences
                .iter()
                .filter(|prior| {
                    prior.generation == *target_generation && same_cause(&prior.diagnosis)
                })
                .flat_map(|prior| &prior.diagnosis.scope)
                .any(|prior| scope.same_verification_scope(prior))
        }))
    }

    pub(crate) fn validate(&self, index: &ComparisonIndex) -> Result<(), AppError> {
        match self {
            Self::New {
                reason,
                closest_candidates,
            } => {
                require(
                    !reason.trim().is_empty(),
                    "unmatched problem needs separation reasoning",
                )?;
                require(
                    index.entries.is_empty() || !closest_candidates.is_empty(),
                    "new problem must compare its closest existing candidates",
                )?;
                for candidate in closest_candidates {
                    candidate.validate(index)?;
                }
            }
            Self::Existing {
                issue_number,
                expected_generation,
                expected_scope_revision,
                target_generation,
                relation,
                record_digest,
                full_read_digest,
                reason,
                ..
            } => {
                let entry = index.read(*issue_number, full_read_digest)?;
                require(
                    !reason.trim().is_empty()
                        && entry.record_digest == *record_digest
                        && entry.generation == *expected_generation
                        && entry.scope_revision == *expected_scope_revision,
                    "matched problem changed or lacks causal reasoning",
                )?;
                require(
                    target_generation <= expected_generation
                        && (*relation == Relation::Historical
                            || target_generation == expected_generation),
                    "match must identify an applicable occurrence",
                )?;
            }
            Self::Ambiguous { reason, candidates } => {
                require(
                    !reason.trim().is_empty() && !candidates.is_empty(),
                    "ambiguous match needs candidate evidence",
                )?;
                for candidate in candidates {
                    candidate.validate(index)?;
                }
            }
        }
        Ok(())
    }
}

/// Semantic relation still requires independent occurrence/source applicability before publication.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Relation {
    Repeat,
    Recurrence,
    Historical,
}

/// A full-read candidate and the AI's explanation of its relation to the observed failure.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidateComparison {
    issue_number: NonZero<u64>,
    full_read_digest: String,
    reason: String,
}

impl CandidateComparison {
    fn validate(&self, index: &ComparisonIndex) -> Result<(), AppError> {
        require(
            !self.reason.trim().is_empty(),
            "candidate comparison reasoning",
        )?;
        _ = index.read(self.issue_number, &self.full_read_digest)?;
        Ok(())
    }
}

/// Identifies a candidate reference that cannot be established from the complete index.
#[ohno::error]
#[display("cannot compare problem: {reason}")]
struct ComparisonError {
    reason: &'static str,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn typed_index_requires_complete_read_and_current_occurrence() {
        let issue = NonZero::new(7).unwrap();
        let index = ComparisonIndex {
            digest: "snapshot".to_owned(),
            complete: true,
            entries: vec![IndexEntry {
                issue_number: issue,
                generation: NonZero::new(2).unwrap(),
                scope_revision: NonZero::new(1).unwrap(),
                record_digest: "record".to_owned(),
                summary: json!({}),
                full_read_digest: Some("read".to_owned()),
                prior_support: None,
            }],
        };
        index.validate("snapshot", &[issue]).unwrap();
        _ = index.validate("snapshot", &[]).unwrap_err();
        let decision = MatchDecision::Existing {
            issue_number: issue,
            expected_generation: NonZero::new(2).unwrap(),
            expected_scope_revision: NonZero::new(1).unwrap(),
            target_generation: NonZero::new(2).unwrap(),
            record_digest: "record".to_owned(),
            full_read_digest: "read".to_owned(),
            relation: Relation::Repeat,
            reason: "common cause".to_owned(),
        };
        decision.validate(&index).unwrap();
        _ = index.read(issue, "stale").unwrap_err();
    }

    #[test]
    fn prior_actionability_is_occurrence_scoped_and_ignores_only_citation_changes() {
        let diagnosis: Diagnosis = serde_json::from_value(json!({
            "title":"Checker defect","summary":"An independently diagnosed defect","cause":"Invalid access",
            "category":"code","repair_disposition":"actionable","repair_reason":"Correct the access",
            "citations":["/diagnostic"],"scope":[{
                "operation":"checker","package":null,"check_id":null,"platform":null,
                "replay":null,"citations":["/diagnostic"]
            }]
        }))
        .unwrap();
        let mut index: ComparisonIndex = serde_json::from_value(json!({
            "digest":"index","complete":true,"entries":[{
                "issue_number":7,"generation":2,"scope_revision":1,"record_digest":"full",
                "summary":{},"full_read_digest":"full","prior_support":{
                    "full_read_digest":"full","current_diagnosis":diagnosis,
                    "occurrences":[{"generation":1,"diagnosis":diagnosis}]
                }
            }]
        }))
        .unwrap();
        let matching: MatchDecision = serde_json::from_value(json!({
            "kind":"existing","issue_number":7,"expected_generation":2,"expected_scope_revision":1,
            "target_generation":1,"record_digest":"full","full_read_digest":"full",
            "relation":"historical","reason":"Late evidence for the established earlier occurrence"
        }))
        .unwrap();
        matching.validate(&index).unwrap();
        let mut cited = diagnosis;
        cited.scope.first_mut().unwrap().citations = vec!["/another-diagnostic".to_owned()];
        assert!(matching.supports_actionable(&cited, &index).unwrap());
        let entry = index.entries.first_mut().unwrap();
        let support = entry.prior_support.as_mut().unwrap();
        support.current_diagnosis.repair_disposition = RepairDisposition::NeedsHuman;
        assert!(matching.supports_actionable(&cited, &index).unwrap());
        let occurrence = index
            .entries
            .first_mut()
            .unwrap()
            .prior_support
            .as_mut()
            .unwrap()
            .occurrences
            .first_mut()
            .unwrap();
        occurrence.generation = NonZero::new(2).unwrap();
        assert!(!matching.supports_actionable(&cited, &index).unwrap());
    }
}
