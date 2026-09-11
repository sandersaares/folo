use std::collections::{BTreeMap, BTreeSet};
use std::iter;
use std::num::NonZero;

use ohno::AppError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::basis::EvidenceBasis;
use crate::comparison::{ComparisonIndex, MatchDecision};
use crate::problem::{Diagnosis, RepairDisposition};
use crate::protocol::{require, shared};

/// AI-authored decisions for one immutable revision, validated before publication is prepared.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AnalysisRecord {
    pub(crate) schema_version: u8,
    pub(crate) analysis_id: String,
    pub(crate) checkpoint: NonZero<u64>,
    pub(crate) revision: Revision,
    pub(crate) status: AnalysisStatus,
    pub(crate) considered_issues: Vec<NonZero<u64>>,
    pub(crate) index_digest: String,
    pub(crate) jobs: Vec<JobDisposition>,
    pub(crate) results: Vec<ResultDisposition>,
    pub(crate) gaps: Vec<GapDisposition>,
    pub(crate) problems: Vec<ProblemDecision>,
    pub(crate) reason: String,
    #[serde(default)]
    support_dispositions: BTreeMap<String, Disposition>,
    /// Required when the complete attempt has no jobs; optional for explained job-level failures.
    workflow: Option<Disposition>,
}

impl AnalysisRecord {
    pub(crate) fn validate(
        &self,
        evidence: &Value,
        index: &ComparisonIndex,
        basis: &EvidenceBasis,
    ) -> Result<(), AppError> {
        require(
            self.schema_version == 1 && !self.analysis_id.trim().is_empty(),
            "analysis identity",
        )?;
        let prepared = shared(&serde_json::json!({"op":"prepare", "evidence":evidence}))?;
        let identity = prepared
            .get("identity")
            .ok_or_else(|| InvalidAnalysis::new("source identity"))?;
        require(
            identity.get("repository_id").and_then(Value::as_u64)
                == Some(self.revision.repository_id.get())
                && identity.get("workflow_id").and_then(Value::as_u64)
                    == Some(self.revision.workflow_id.get())
                && identity.get("run_id").and_then(Value::as_u64)
                    == Some(self.revision.run_id.get())
                && evidence
                    .pointer("/attempt/run_attempt")
                    .and_then(Value::as_u64)
                    == Some(self.revision.run_attempt.get())
                && prepared.get("digest").and_then(Value::as_str)
                    == Some(self.revision.digest.as_str()),
            "analysis must name the exact restored evidence revision",
        )?;
        index.validate(&self.index_digest, &self.considered_issues)?;
        let view = basis.view(evidence, &self.revision)?;
        let complete = self.status == AnalysisStatus::Complete;
        if !complete {
            require(
                !self.reason.trim().is_empty(),
                "unfinished analysis needs a reason",
            )?;
        }
        let mut problems = BTreeMap::new();
        let mut canonical_issues = BTreeSet::new();
        for problem in &self.problems {
            require(!problem.key.trim().is_empty(), "problem proposal key")?;
            require(
                problems.insert(&problem.key, problem).is_none(),
                "duplicate problem proposal",
            )?;
            problem.diagnosis.validate(&view)?;
            problem.matching.validate(index)?;
            if let MatchDecision::Existing { issue_number, .. } = &problem.matching {
                require(
                    canonical_issues.insert(issue_number),
                    "combine contributions to the same canonical problem",
                )?;
            }
            require(
                !complete
                    || (!matches!(problem.matching, MatchDecision::Ambiguous { .. })
                        && problem.diagnosis.repair_disposition != RepairDisposition::Unresolved),
                "unresolved problems cannot complete analysis",
            )?;
        }
        let jobs = view
            .pointer("/api_evidence/jobs")
            .and_then(Value::as_array)
            .ok_or_else(|| InvalidAnalysis::new("job inventory"))?;
        let mut accounted = BTreeSet::new();
        for disposition in &self.jobs {
            require(
                accounted.insert(disposition.job_id),
                "duplicate job disposition",
            )?;
            let job = jobs
                .iter()
                .find(|job| job.get("id").and_then(Value::as_u64) == Some(disposition.job_id.get()))
                .ok_or_else(|| InvalidAnalysis::new("unknown job"))?;
            disposition
                .disposition
                .validate(&view, &problems, complete)?;
            let steps = job.get("steps").and_then(Value::as_array);
            let mut accounted_steps = BTreeSet::new();
            for step in &disposition.steps {
                require(
                    accounted_steps.insert(step.number),
                    "duplicate step disposition",
                )?;
                require(
                    steps.is_some_and(|steps| {
                        steps.iter().any(|candidate| {
                            candidate.get("number").and_then(Value::as_u64)
                                == Some(step.number.get())
                        })
                    }),
                    "unknown step",
                )?;
                step.disposition.validate(&view, &problems, complete)?;
            }
            if complete {
                require(steps.is_some(), "missing steps remain unresolved")?;
                for step in steps
                    .into_iter()
                    .flatten()
                    .filter(|step| unsuccessful(step))
                {
                    let number = step
                        .get("number")
                        .and_then(Value::as_u64)
                        .and_then(NonZero::new);
                    require(
                        number.is_some_and(|number| accounted_steps.contains(&number)),
                        "failed step has no disposition",
                    )?;
                }
            }
        }
        if complete {
            if jobs.is_empty() {
                let workflow = self
                    .workflow
                    .as_ref()
                    .ok_or_else(|| InvalidAnalysis::new("workflow disposition"))?;
                require(
                    matches!(
                        workflow.kind,
                        DispositionKind::Cancelled
                            | DispositionKind::Infrastructure
                            | DispositionKind::Blocked
                    ) && matches!(
                        view.pointer("/api_evidence/workflow_conclusion")
                            .and_then(Value::as_str),
                        Some(
                            "cancelled"
                                | "failure"
                                | "timed_out"
                                | "startup_failure"
                                | "action_required"
                        )
                    ) && workflow
                        .citations
                        .iter()
                        .any(|citation| citation == "/api_evidence/workflow_conclusion"),
                    "empty execution needs explicit workflow-level failure or cancellation evidence",
                )?;
                require(
                    self.problems.iter().all(|problem| {
                        problem.diagnosis.repair_disposition != RepairDisposition::Actionable
                    }),
                    "unexecuted jobs cannot establish a source repair",
                )?;
            }
            for job in jobs.iter().filter(|job| {
                unsuccessful(job)
                    || job.get("conclusion").and_then(Value::as_str) == Some("skipped")
                    || job
                        .get("steps")
                        .and_then(Value::as_array)
                        .is_none_or(|steps| steps.iter().any(unsuccessful))
            }) {
                let id = job.get("id").and_then(Value::as_u64).and_then(NonZero::new);
                require(
                    id.is_some_and(|id| accounted.contains(&id)),
                    "unsuccessful job has no disposition",
                )?;
            }
        }
        let sources: BTreeMap<_, _> = iter::once((self.revision.digest.as_str(), evidence))
            .chain(
                basis
                    .supporting_revisions
                    .iter()
                    .map(|support| (support.digest.as_str(), &support.evidence)),
            )
            .collect();
        let mut accounted_results = BTreeSet::new();
        for result in &self.results {
            let digest = result
                .source_digest
                .as_deref()
                .unwrap_or(&self.revision.digest);
            let results = sources
                .get(digest)
                .and_then(|source| source.pointer("/attempt/results"))
                .and_then(Value::as_array)
                .ok_or_else(|| InvalidAnalysis::new("checker result source"))?;
            require(
                result.index < results.len() && accounted_results.insert((digest, result.index)),
                "unknown or duplicate result disposition",
            )?;
            result.disposition.validate(&view, &problems, complete)?;
        }
        if complete {
            for (digest, source) in &sources {
                let results = source
                    .pointer("/attempt/results")
                    .and_then(Value::as_array)
                    .ok_or_else(|| InvalidAnalysis::new("checker result inventory"))?;
                for (position, result) in results.iter().enumerate() {
                    require(
                        result.get("outcome").and_then(Value::as_str) == Some("passed")
                            || accounted_results.contains(&(*digest, position)),
                        "unsuccessful checker result has no disposition",
                    )?;
                }
            }
        }
        let mut accounted_gaps = BTreeSet::new();
        for gap in &self.gaps {
            let digest = gap
                .source_digest
                .as_deref()
                .unwrap_or(&self.revision.digest);
            let source_gaps = sources
                .get(digest)
                .and_then(|source| source.pointer("/attempt/evidence_gaps"))
                .and_then(Value::as_array)
                .ok_or_else(|| InvalidAnalysis::new("gap source"))?;
            require(
                source_gaps.contains(&Value::String(gap.gap.clone()))
                    && accounted_gaps.insert((digest, gap.gap.as_str())),
                "unknown or duplicate gap disposition",
            )?;
            gap.disposition.validate(&view, &problems, complete)?;
        }
        if complete {
            for (digest, source) in &sources {
                let gaps = source
                    .pointer("/attempt/evidence_gaps")
                    .and_then(Value::as_array)
                    .ok_or_else(|| InvalidAnalysis::new("evidence gaps"))?;
                require(
                    gaps.iter()
                        .filter_map(Value::as_str)
                        .all(|gap| accounted_gaps.contains(&(*digest, gap))),
                    "original or supporting evidence gap has no disposition",
                )?;
            }
        }
        for (digest, disposition) in &self.support_dispositions {
            require(
                basis
                    .supporting_revisions
                    .iter()
                    .any(|support| &support.digest == digest),
                "unknown supporting revision disposition",
            )?;
            disposition.validate(&view, &problems, complete)?;
        }
        if let Some(workflow) = &self.workflow {
            workflow.validate(&view, &problems, complete)?;
        }
        if complete {
            require(
                basis
                    .supporting_revisions
                    .iter()
                    .all(|support| self.support_dispositions.contains_key(&support.digest)),
                "supporting diagnostics and collection differences need explicit reasoning",
            )?;
        }
        self.validate_problem_links(index)
    }

    fn validate_problem_links(&self, index: &ComparisonIndex) -> Result<(), AppError> {
        for problem in &self.problems {
            let key = &problem.key;
            require(
                self.dispositions()
                    .any(|disposition| disposition.problem_keys.contains(key)),
                "problem is not linked to an analyzed failure",
            )?;
            if problem.diagnosis.repair_disposition == RepairDisposition::Actionable {
                let linked = || {
                    self.dispositions()
                        .filter(|disposition| disposition.problem_keys.contains(key))
                };
                // Consequences cannot establish repair eligibility. A duplicate only references
                // prior cause/scope proof, never promotes or extends it without actionable input.
                require(
                    linked().any(|disposition| disposition.kind == DispositionKind::Actionable)
                        || (linked()
                            .any(|disposition| disposition.kind == DispositionKind::Duplicate)
                            && problem
                                .matching
                                .supports_actionable(&problem.diagnosis, index)?),
                    "actionable diagnosis needs actionable evidence or verified prior cause/scope support",
                )?;
            }
        }
        Ok(())
    }

    fn dispositions(&self) -> impl Iterator<Item = &Disposition> {
        self.jobs
            .iter()
            .flat_map(|job| {
                iter::once(&job.disposition).chain(job.steps.iter().map(|step| &step.disposition))
            })
            .chain(self.results.iter().map(|result| &result.disposition))
            .chain(self.gaps.iter().map(|gap| &gap.disposition))
            .chain(self.support_dispositions.values())
            .chain(self.workflow.iter())
    }
}

/// Exact hosted revision; its digest is evidence identity, never problem identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Revision {
    pub(crate) repository_id: NonZero<u64>,
    pub(crate) workflow_id: NonZero<u64>,
    pub(crate) run_id: NonZero<u64>,
    pub(crate) run_attempt: NonZero<u64>,
    pub(crate) digest: String,
    pub(crate) issue_number: NonZero<u64>,
}

/// Analysis completion is independent of issue publication and problem resolution.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum AnalysisStatus {
    InProgress,
    Blocked,
    Complete,
}

/// A job's conclusion does not hide failed steps or several independent causes.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JobDisposition {
    job_id: NonZero<u64>,
    disposition: Disposition,
    steps: Vec<StepDisposition>,
}

/// Failed prerequisites and artifact steps require the same reasoning as checker execution.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StepDisposition {
    number: NonZero<u64>,
    disposition: Disposition,
}

/// Checker results can report failures even when the enclosing job appears successful.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResultDisposition {
    /// Absent for the primary revision; otherwise identifies a supporting reporter revision.
    source_digest: Option<String>,
    index: usize,
    disposition: Disposition,
}

/// A gap may be explained by an evidenced prerequisite failure, but cannot be silently dropped.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GapDisposition {
    /// Original gaps are never replaced by a supporting revision's cleaner collection.
    source_digest: Option<String>,
    gap: String,
    disposition: Disposition,
}

/// Supported interpretation supplied by AI, not inferred from diagnostic keywords.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Disposition {
    kind: DispositionKind,
    explanation: String,
    citations: Vec<String>,
    problem_keys: Vec<String>,
}

impl Disposition {
    fn validate(
        &self,
        evidence: &Value,
        problems: &BTreeMap<&String, &ProblemDecision>,
        complete: bool,
    ) -> Result<(), AppError> {
        require(!self.explanation.trim().is_empty(), "disposition reasoning")?;
        validate_citations(&self.citations, evidence)?;
        require(
            self.problem_keys
                .iter()
                .all(|key| problems.contains_key(key)),
            "disposition links an unknown problem",
        )?;
        require(
            !complete || self.kind != DispositionKind::Unresolved,
            "unresolved disposition",
        )?;
        if matches!(
            self.kind,
            DispositionKind::Actionable
                | DispositionKind::Duplicate
                | DispositionKind::Infrastructure
        ) {
            require(
                !self.problem_keys.is_empty(),
                "problem disposition needs a canonical link",
            )?;
        }
        Ok(())
    }
}

/// Disposition categories distinguish unexecuted consequences from independently repairable defects.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum DispositionKind {
    Actionable,
    Duplicate,
    Infrastructure,
    Blocked,
    Cancelled,
    Unresolved,
}

/// Local proposal keys connect a revision's jobs before GitHub assigns new canonical issue numbers.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProblemDecision {
    pub(crate) key: String,
    pub(crate) diagnosis: Diagnosis,
    pub(crate) matching: MatchDecision,
}

pub(crate) fn validate_citations(citations: &[String], evidence: &Value) -> Result<(), AppError> {
    require(
        !citations.is_empty()
            && citations.iter().all(|pointer| {
                pointer.starts_with('/')
                    && evidence
                        .pointer(pointer)
                        .is_some_and(|value| !value.is_null())
            }),
        "citations must identify retained evidence",
    )?;
    Ok(())
}

fn unsuccessful(value: &Value) -> bool {
    value.get("status").and_then(Value::as_str) != Some("completed")
        || !matches!(
            value.get("conclusion").and_then(Value::as_str),
            Some("success" | "skipped")
        )
}

/// Identifies missing structural evidence needed to account for a revision.
#[ohno::error]
#[display("analysis lacks {requirement}")]
struct InvalidAnalysis {
    requirement: &'static str,
}
