use std::cmp::Ordering;
use std::num::NonZero;

use jiff::Timestamp;
use ohno::AppError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::analysis::Revision;
use crate::basis::EvidenceBasis;
use crate::comparison::Relation;
use crate::problem::Diagnosis;
use crate::protocol::require;
use crate::scope::Scope;

/// Occurrences are positive identities; the initial failure establishes the first one.
const INITIAL_OCCURRENCE: NonZero<u64> = NonZero::<u64>::MIN;
/// A new occurrence begins with its initial required-scope revision.
const INITIAL_SCOPE_REVISION: NonZero<u64> = NonZero::<u64>::MIN;

/// Canonical problem snapshot stored in paginated detail, independently of its compact issue root.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProblemRecord {
    schema_version: u8,
    repository_id: NonZero<u64>,
    issue_number: NonZero<u64>,
    generation: NonZero<u64>,
    scope_revision: NonZero<u64>,
    status: ProblemStatus,
    diagnosis: Diagnosis,
    observation: Observation,
    evidence: Vec<OccurrenceEvidence>,
    /// Only an independently established resolution permits recurrence.
    resolution: Option<Resolution>,
    resolved_occurrences: Vec<Resolution>,
    /// Reporter-owned findings retain their original registered scope without becoming AI receipts.
    legacy: Option<LegacyProblem>,
}

/// Incoming AI diagnosis and helper-observed applicability, not a command to resolve a problem.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProblemUpdate {
    pub(crate) issue_number: NonZero<u64>,
    pub(crate) target_generation: NonZero<u64>,
    pub(crate) revision: Revision,
    pub(crate) diagnosis: Diagnosis,
    pub(crate) observation: Observation,
    pub(crate) relation: Relation,
    pub(crate) source_relation: SourceRelation,
    pub(crate) operation_id: String,
    pub(crate) primary_evidence: Value,
    pub(crate) basis: EvidenceBasis,
}

/// Failure ordering is based on the originating execution rather than publication time.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Observation {
    pub(crate) source_sha: String,
    pub(crate) run_id: NonZero<u64>,
    pub(crate) run_attempt: NonZero<u64>,
    pub(crate) started_at: String,
    pub(crate) created_at: String,
}

impl Observation {
    fn validate(&self) -> Result<(), AppError> {
        require(
            self.source_sha.len() == 40
                && self.source_sha.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "immutable problem source",
        )?;
        _ = self
            .started_at
            .parse::<Timestamp>()
            .map_err(ParseTimeError::caused_by)?;
        _ = self
            .created_at
            .parse::<Timestamp>()
            .map_err(ParseTimeError::caused_by)?;
        Ok(())
    }

    fn compare(&self, other: &Self) -> Result<Ordering, AppError> {
        self.validate()?;
        other.validate()?;
        if self.run_id == other.run_id {
            return Ok(self.run_attempt.cmp(&other.run_attempt));
        }
        Ok((
            self.started_at
                .parse::<Timestamp>()
                .map_err(ParseTimeError::caused_by)?,
            self.created_at
                .parse::<Timestamp>()
                .map_err(ParseTimeError::caused_by)?,
            self.run_id,
        )
            .cmp(&(
                other
                    .started_at
                    .parse::<Timestamp>()
                    .map_err(ParseTimeError::caused_by)?,
                other
                    .created_at
                    .parse::<Timestamp>()
                    .map_err(ParseTimeError::caused_by)?,
                other.run_id,
            )))
    }
}

/// GitHub source comparison is supplied by the verified read adapter, never inferred from time.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum SourceRelation {
    Identical,
    Descendant,
    Unrelated,
    Unknown,
}

/// Analysis cannot produce resolution; it only observes prior applicable resolution evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ProblemStatus {
    Open,
    Resolved,
    NeedsHuman,
}

/// Every contributing revision retains its own scope and diagnosis in its applicable occurrence.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OccurrenceEvidence {
    generation: NonZero<u64>,
    revision: Revision,
    diagnosis: Diagnosis,
    observation: Observation,
    operation_id: String,
    basis: EvidenceBasis,
}

/// Applicable prior resolution, with provenance retained independently of a closed issue label.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Resolution {
    generation: NonZero<u64>,
    observation: Observation,
    evidence: Vec<ResolutionEvidence>,
    explanation: String,
}

/// Resolution provenance is either exact run evidence or an already registered hosted repair.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum ResolutionEvidence {
    Run {
        revision: Revision,
    },
    RegisteredRepair {
        issue_number: NonZero<u64>,
        generation: NonZero<u64>,
        finding_id: String,
        pr_number: NonZero<u64>,
        merge_commit_sha: String,
        reporter_record_digest: String,
    },
}

/// An AI-matched reporter issue keeps its original evidence and complete repair scope intact.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LegacyProblem {
    finding_id: String,
    generation: NonZero<u64>,
    scope: Vec<Scope>,
    record: Value,
}

pub(crate) fn update_problem(
    existing: Option<ProblemRecord>,
    incoming: ProblemUpdate,
) -> Result<ProblemRecord, AppError> {
    incoming.observation.validate()?;
    let view = incoming
        .basis
        .view(&incoming.primary_evidence, &incoming.revision)?;
    require(
        incoming.observation == incoming.basis.observation(),
        "problem observation must use the verified completion basis",
    )?;
    incoming.diagnosis.validate(&view)?;
    require(
        !incoming.operation_id.trim().is_empty()
            && incoming.observation.run_id == incoming.revision.run_id
            && incoming.observation.run_attempt == incoming.revision.run_attempt,
        "problem update must bind its publication and execution",
    )?;
    let duplicate_diagnosis = incoming.diagnosis.clone();
    let mut problem = match existing {
        None => {
            require(
                incoming.target_generation == INITIAL_OCCURRENCE
                    && incoming.relation == Relation::Repeat,
                "a new problem starts its initial occurrence",
            )?;
            ProblemRecord {
                schema_version: 1,
                repository_id: incoming.revision.repository_id,
                issue_number: incoming.issue_number,
                generation: INITIAL_OCCURRENCE,
                scope_revision: INITIAL_SCOPE_REVISION,
                status: ProblemStatus::Open,
                diagnosis: duplicate_diagnosis,
                observation: incoming.observation.clone(),
                evidence: Vec::new(),
                resolution: None,
                resolved_occurrences: Vec::new(),
                legacy: None,
            }
        }
        Some(mut problem) => {
            require(
                problem.schema_version == 1
                    && problem.repository_id == incoming.revision.repository_id
                    && problem.issue_number == incoming.issue_number,
                "canonical problem identity",
            )?;
            if let Some(item) = problem
                .evidence
                .iter()
                .find(|item| item.operation_id == incoming.operation_id)
            {
                require(
                    item.revision == incoming.revision
                        && item.diagnosis == incoming.diagnosis
                        && item.observation == incoming.observation,
                    "conflicting problem publication operation",
                )?;
                return Ok(problem);
            }
            if incoming.relation != Relation::Recurrence
                && problem.evidence.iter().any(|item| {
                    (
                        item.generation,
                        &item.revision,
                        &item.diagnosis,
                        &item.observation,
                    ) == (
                        incoming.target_generation,
                        &incoming.revision,
                        &incoming.diagnosis,
                        &incoming.observation,
                    )
                })
            {
                // Reconsidering a refreshed index must not republish unchanged evidence and
                // change that index again before the next independently diagnosed problem.
                // A new recurrence still needs resolution and ordering proof; only a known
                // publication operation above can replay an already completed transition.
                return Ok(problem);
            }
            require(
                incoming.target_generation <= problem.generation,
                "unknown future occurrence",
            )?;
            if incoming.relation != Relation::Historical {
                require(
                    incoming.target_generation == problem.generation,
                    "stale problem generation",
                )?;
            }
            match incoming.relation {
                Relation::Recurrence => {
                    let resolution = problem
                        .resolution
                        .as_ref()
                        .ok_or_else(InvalidResolution::new)?;
                    require(
                        problem.status == ProblemStatus::Resolved
                            && resolution.generation == problem.generation
                            && !resolution.evidence.is_empty()
                            && !resolution.explanation.trim().is_empty()
                            && incoming.observation.compare(&resolution.observation)?
                                == Ordering::Greater
                            && matches!(
                                incoming.source_relation,
                                SourceRelation::Identical | SourceRelation::Descendant
                            ),
                        "recurrence needs applicable resolution, source ancestry and newer execution",
                    )?;
                    problem.generation = problem
                        .generation
                        .checked_add(1)
                        .ok_or_else(GenerationOverflow::new)?;
                    problem.scope_revision = INITIAL_SCOPE_REVISION;
                    problem.status = ProblemStatus::Open;
                    problem.resolved_occurrences.push(
                        problem
                            .resolution
                            .take()
                            .expect("the resolution was validated before advancing the occurrence"),
                    );
                    problem.diagnosis = duplicate_diagnosis;
                    problem.observation = incoming.observation.clone();
                }
                Relation::Repeat => {
                    require(
                        problem.status != ProblemStatus::Resolved,
                        "resolved problems require recurrence or historical disposition",
                    )?;
                    if incoming.observation.compare(&problem.observation)? != Ordering::Less {
                        problem.diagnosis = duplicate_diagnosis;
                        if matches!(
                            incoming.source_relation,
                            SourceRelation::Identical | SourceRelation::Descendant
                        ) {
                            problem.observation = incoming.observation.clone();
                        }
                    }
                }
                Relation::Historical => {
                    require(
                        incoming.observation.compare(&problem.observation)? != Ordering::Greater
                            || incoming.source_relation == SourceRelation::Unrelated,
                        "new applicable evidence is not a historical observation",
                    )?;
                    require(
                        incoming.target_generation == problem.generation
                            || problem.resolved_occurrences.iter().any(|resolution| {
                                resolution.generation == incoming.target_generation
                            }),
                        "historical occurrence has no established identity",
                    )?;
                }
            }
            // Required scope follows occurrence membership, not delivery order. Historical
            // evidence can expand this occurrence, but cannot affect a newer one's repair fence.
            if incoming.target_generation == problem.generation {
                let grows_scope = incoming.diagnosis.scope.iter().any(|scope| {
                    !problem
                        .evidence
                        .iter()
                        .filter(|item| item.generation == problem.generation)
                        .flat_map(|item| &item.diagnosis.scope)
                        .chain(
                            problem
                                .legacy
                                .iter()
                                .filter(|legacy| legacy.generation == problem.generation)
                                .flat_map(|legacy| &legacy.scope),
                        )
                        .any(|existing| scope.same_verification_scope(existing))
                });
                if grows_scope {
                    problem.scope_revision = problem
                        .scope_revision
                        .checked_add(1)
                        .ok_or_else(GenerationOverflow::new)?;
                }
            }
            problem
        }
    };
    problem.evidence.push(OccurrenceEvidence {
        generation: if incoming.relation == Relation::Historical {
            incoming.target_generation
        } else {
            problem.generation
        },
        revision: incoming.revision,
        diagnosis: incoming.diagnosis,
        observation: incoming.observation,
        operation_id: incoming.operation_id,
        basis: incoming.basis,
    });
    Ok(problem)
}

/// Identifies invalid API-derived execution time rather than inventing chronology.
#[ohno::error]
#[display("cannot parse problem observation time")]
struct ParseTimeError;

/// Identifies missing prior resolution required by recurrence.
#[ohno::error]
#[display("problem has no established resolution")]
struct InvalidResolution;

/// Retains occurrence identity instead of wrapping counters.
#[ohno::error]
#[display("problem occurrence counter is exhausted")]
struct GenerationOverflow;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn typed_observation_order_uses_attempts_and_actual_start_time() {
        let first = Observation {
            source_sha: "a".repeat(40),
            run_id: NonZero::new(2).unwrap(),
            run_attempt: NonZero::new(1).unwrap(),
            started_at: "2026-09-09T01:00:00Z".to_owned(),
            created_at: "2026-09-09T01:00:00Z".to_owned(),
        };
        let mut next = first.clone();
        next.run_attempt = NonZero::new(2).unwrap();
        assert_eq!(next.compare(&first).unwrap(), Ordering::Greater);
        next.run_id = NonZero::new(1).unwrap();
        next.started_at = "2026-09-09T02:00:00Z".to_owned();
        assert_eq!(next.compare(&first).unwrap(), Ordering::Greater);
        next.source_sha = "not-a-sha".to_owned();
        _ = next.validate().unwrap_err();
    }
}
