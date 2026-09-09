use std::collections::BTreeMap;
use std::num::NonZero;

use jiff::Timestamp;
use ohno::AppError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::canonical::{digest, json, normalized, sort_observations};

/// Actions identifies Git commits with full SHA-1 hexadecimal object names.
const COMMIT_HEX_LENGTH: usize = 40;

/// Observations supplied by the reviewed reporter, never semantic finding identities.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Evidence {
    pub(crate) repository: Repository,
    pub(crate) workflow: Workflow,
    pub(crate) run_id: NonZero<u64>,
    pub(crate) attempt: Attempt,
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, Value>,
}

impl Evidence {
    pub(crate) fn identity(&self) -> Identity {
        Identity {
            repository_id: self.repository.id,
            workflow_id: self.workflow.id,
            run_id: self.run_id,
        }
    }

    pub(crate) fn validate(mut self) -> Result<ValidatedEvidence, AppError> {
        let name_parts: Vec<_> = self.repository.name.split('/').collect();
        require(
            name_parts.len() == 2
                && name_parts.iter().all(|part| {
                    !part.is_empty()
                        && part
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                }),
            "repository name must identify owner/repository",
        )?;
        require(
            !self.workflow.name.trim().is_empty()
                && self.workflow.path.starts_with(".github/workflows/")
                && !self.workflow.path.contains("..")
                && !self.workflow.path.contains('\\')
                && !self.workflow.path.contains('\n'),
            "workflow name or path is malformed",
        )?;
        for sha in [&self.attempt.run_sha, &self.attempt.controller_sha] {
            // Actions supplies full Git object names, not abbreviations or mutable refs.
            require(
                sha.len() == COMMIT_HEX_LENGTH && sha.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "run and controller SHA must be full hexadecimal object names",
            )?;
        }
        for (name, value) in [
            ("created_at", &self.attempt.created_at),
            ("started_at", &self.attempt.started_at),
            ("completed_at", &self.attempt.completed_at),
        ] {
            if let Some(value) = value {
                require(
                    value.parse::<Timestamp>().is_ok(),
                    format!("{name} is not an API timestamp"),
                )?;
            } else {
                gap(&mut self.attempt.evidence_gaps, format!("missing {name}"));
            }
        }
        let attempt = &mut self.attempt;
        require(
            attempt.manifest.as_ref().is_none_or(Value::is_object)
                && attempt.plan.as_ref().is_none_or(Value::is_object),
            "validated manifest and plan must be JSON objects",
        )?;
        if attempt.manifest.is_none() {
            gap(&mut attempt.evidence_gaps, "validated manifest unavailable");
        }
        if attempt.plan.is_none() {
            gap(&mut attempt.evidence_gaps, "validated plan unavailable");
        }
        require(
            !attempt.validated_no_work || (attempt.manifest.is_some() && attempt.plan.is_some()),
            "validated no-work requires a validated manifest and plan",
        )?;
        if attempt.jobs.is_empty() {
            gap(&mut attempt.evidence_gaps, "job inventory is empty");
        }
        if attempt.results.is_empty() && !attempt.validated_no_work {
            gap(
                &mut attempt.evidence_gaps,
                "checker result inventory is empty",
            );
        }
        for job in &attempt.jobs {
            let label = job
                .id
                .map_or_else(|| "unknown".to_owned(), |id| id.to_string());
            if job.id.is_none() || job.name.as_ref().is_none_or(|name| name.trim().is_empty()) {
                gap(
                    &mut attempt.evidence_gaps,
                    format!("job {label}: incomplete identity"),
                );
            }
            if job.status.is_none() || job.conclusion.is_none() || job.steps.is_none() {
                gap(
                    &mut attempt.evidence_gaps,
                    format!("job {label}: incomplete status or step inventory"),
                );
            }
            if job.steps.as_ref().is_some_and(Vec::is_empty)
                && job.conclusion.as_deref() != Some("skipped")
            {
                gap(
                    &mut attempt.evidence_gaps,
                    format!("job {label}: step inventory is empty"),
                );
            }
            if job.unsuccessful() && job.log.is_none() {
                gap(
                    &mut attempt.evidence_gaps,
                    format!("job {label}: failure log unavailable"),
                );
            }
            if let Some(log) = &job.log {
                require(
                    log.url.starts_with("https://"),
                    "job log must retain its original HTTPS reference",
                )?;
                if log.unavailable {
                    gap(
                        &mut attempt.evidence_gaps,
                        format!("job {label}: failure log unavailable"),
                    );
                } else if log.excerpt.is_none() || log.bytes.is_none() {
                    gap(
                        &mut attempt.evidence_gaps,
                        format!("job {label}: incomplete log capture metadata"),
                    );
                }
            }
        }
        let should_report = !attempt.evidence_gaps.is_empty()
            || !matches!(attempt.workflow_conclusion.as_deref(), Some("success"))
                && !(attempt.validated_no_work
                    && matches!(
                        attempt.workflow_conclusion.as_deref(),
                        Some("skipped" | "neutral")
                    ))
            || attempt.jobs.iter().any(Job::unsuccessful)
            || attempt
                .results
                .iter()
                .any(|result| result.outcome.as_deref() != Some("passed"));
        for job in &mut attempt.jobs {
            if let Some(steps) = &mut job.steps {
                sort_observations(steps, |step| step.number)?;
            }
        }
        sort_observations(&mut attempt.jobs, |job| job.id)?;
        sort_observations(&mut attempt.results, |result| {
            result
                .extra
                .get("check_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })?;
        attempt.evidence_gaps.sort();
        let canonical = json(&normalized(&self)?)?;
        Ok(ValidatedEvidence {
            identity: self.identity(),
            run_attempt: self.attempt.run_attempt,
            digest: digest(&canonical),
            should_report,
            evidence: self,
            canonical,
        })
    }
}

/// Validated evidence with its immutable publication identity and canonical bytes.
#[derive(Debug)]
pub(crate) struct ValidatedEvidence {
    pub(crate) identity: Identity,
    pub(crate) run_attempt: NonZero<u64>,
    pub(crate) digest: String,
    pub(crate) should_report: bool,
    pub(crate) evidence: Evidence,
    pub(crate) canonical: String,
}

/// Numeric API identity of a run, independent of names, symptoms and reruns.
#[expect(
    clippy::struct_field_names,
    reason = "protocol field names distinguish numeric API IDs from names and attempt numbers"
)]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Identity {
    pub(crate) repository_id: NonZero<u64>,
    pub(crate) workflow_id: NonZero<u64>,
    pub(crate) run_id: NonZero<u64>,
}

impl Identity {
    pub(crate) fn key(&self) -> String {
        format!(
            "{}/{}/{}",
            self.repository_id, self.workflow_id, self.run_id
        )
    }
}

/// Repository API metadata retained alongside the numeric identity.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Repository {
    pub(crate) id: NonZero<u64>,
    pub(crate) name: String,
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, Value>,
}

/// Workflow API metadata retained independently of candidate execution.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Workflow {
    pub(crate) id: NonZero<u64>,
    pub(crate) name: String,
    pub(crate) path: String,
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, Value>,
}

/// A complete observation of one attempt, including missing-evidence diagnostics.
///
/// Timestamps and artifacts may be absent when setup or collection fails; their absence
/// remains reportable evidence instead of preventing the attempt from being recorded.
#[expect(
    clippy::struct_field_names,
    reason = "run_attempt is the GitHub API field name and differs from the run number"
)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Attempt {
    pub(crate) run_attempt: NonZero<u64>,
    pub(crate) run_number: NonZero<u64>,
    pub(crate) created_at: Option<String>,
    pub(crate) started_at: Option<String>,
    pub(crate) completed_at: Option<String>,
    pub(crate) workflow_conclusion: Option<String>,
    pub(crate) run_sha: String,
    pub(crate) controller_sha: String,
    pub(crate) manifest: Option<Value>,
    pub(crate) plan: Option<Value>,
    #[serde(default)]
    pub(crate) validated_no_work: bool,
    pub(crate) results: Vec<CheckResult>,
    pub(crate) jobs: Vec<Job>,
    pub(crate) evidence_gaps: Vec<String>,
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, Value>,
}

/// A parsed checker observation; all checker-specific fields remain evidence.
///
/// A missing outcome records incomplete checker evidence, never an implicit pass.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct CheckResult {
    pub(crate) outcome: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, Value>,
}

/// A job inventory entry, retaining API fields beyond the minimum status contract.
///
/// Missing API metadata is retained to report partial inventory responses. Logs are collected
/// only for unsuccessful jobs, while skipped jobs can legitimately have no steps.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Job {
    pub(crate) id: Option<NonZero<u64>>,
    pub(crate) name: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) conclusion: Option<String>,
    pub(crate) steps: Option<Vec<Step>>,
    pub(crate) log: Option<JobLog>,
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, Value>,
}

impl Job {
    fn unsuccessful(&self) -> bool {
        !successful(self.status.as_deref(), self.conclusion.as_deref())
            || self.steps.as_ref().is_none_or(|steps| {
                steps.iter().any(|step| {
                    step.name.as_ref().is_none_or(|name| name.trim().is_empty())
                        || step.number.is_none()
                        || !successful(step.status.as_deref(), step.conclusion.as_deref())
                })
            })
    }
}

/// A step status that cannot be hidden by a successful job or workflow conclusion.
///
/// Absent API properties are reportable incompleteness, not omitted steps.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Step {
    pub(crate) number: Option<NonZero<u64>>,
    pub(crate) name: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) conclusion: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, Value>,
}

/// Collector-bounded log evidence with explicit truncation and original reference.
///
/// Unavailable logs retain their reference even when no excerpt or byte count was collected.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct JobLog {
    pub(crate) url: String,
    pub(crate) excerpt: Option<String>,
    pub(crate) bytes: Option<u64>,
    #[serde(default)]
    pub(crate) truncated: bool,
    #[serde(default)]
    pub(crate) excerpt_truncated: bool,
    #[serde(default)]
    pub(crate) unavailable: bool,
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, Value>,
}

fn successful(status: Option<&str>, conclusion: Option<&str>) -> bool {
    status == Some("completed") && matches!(conclusion, Some("success" | "skipped"))
}

fn gap(gaps: &mut Vec<String>, value: impl Into<String>) {
    let value = value.into();
    // Normalization is idempotent without deduplicating the observations the caller supplied.
    if !gaps.contains(&value) {
        gaps.push(value);
    }
}

pub(crate) fn require(condition: bool, detail: impl Into<String>) -> Result<(), AppError> {
    if condition {
        Ok(())
    } else {
        Err(InvalidRecordError::new(detail.into()).into())
    }
}

/// Identifies evidence or persisted state that cannot satisfy the intake protocol.
#[ohno::error]
#[display("invalid run record: {detail}")]
pub(crate) struct InvalidRecordError {
    detail: String,
}
