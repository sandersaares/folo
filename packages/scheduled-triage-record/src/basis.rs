use std::collections::BTreeSet;
use std::num::NonZero;

use jiff::Timestamp;
use ohno::AppError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::analysis::Revision;
use crate::lifecycle::Observation;
use crate::protocol::{require, shared};

/// Durable completion basis for an immutable primary revision with incomplete collection.
///
/// Supporting reporter revisions and the complete exact-attempt API inventory remain separate
/// observations. They neither replace the primary digest nor acknowledge another revision.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvidenceBasis {
    schema_version: u8,
    api_digest: String,
    api_evidence: ApiEvidence,
    pub(crate) supporting_revisions: Vec<SupportingRevision>,
}

impl EvidenceBasis {
    pub(crate) fn observation(&self) -> Observation {
        let api = &self.api_evidence;
        Observation {
            source_sha: api
                .source_sha
                .as_ref()
                .unwrap_or(&api.controller_sha)
                .clone(),
            run_id: api.run_id,
            run_attempt: api.run_attempt,
            started_at: api.started_at.clone(),
            created_at: api.created_at.clone(),
        }
    }

    pub(crate) fn view(&self, primary: &Value, revision: &Revision) -> Result<Value, AppError> {
        let api = &self.api_evidence;
        _ = api
            .started_at
            .parse::<Timestamp>()
            .map_err(ParseApiTime::caused_by)?;
        _ = api
            .created_at
            .parse::<Timestamp>()
            .map_err(ParseApiTime::caused_by)?;
        let prepared_primary = shared(&json!({"op":"prepare", "evidence":primary}))?;
        require(
            prepared_primary.get("digest").and_then(Value::as_str)
                == Some(revision.digest.as_str())
                && prepared_primary.get("evidence") == Some(primary),
            "immutable canonical primary revision",
        )?;
        require(
            self.schema_version == 1
                && api.repository_id == revision.repository_id
                && api.workflow_id == revision.workflow_id
                && api.run_id == revision.run_id
                && api.run_attempt == revision.run_attempt
                && primary
                    .pointer("/attempt/controller_sha")
                    .and_then(Value::as_str)
                    == Some(api.controller_sha.as_str()),
            "exact-attempt API provenance",
        )?;
        let fingerprint = shared(&json!({"op":"fingerprint", "value":api}))?;
        require(
            fingerprint.get("digest").and_then(Value::as_str) == Some(self.api_digest.as_str()),
            "API completion evidence digest",
        )?;
        require(
            !api.jobs.is_empty() && api.total_count == api.jobs.len(),
            "complete API job pagination",
        )?;
        let mut ids = BTreeSet::new();
        for job in &api.jobs {
            let id = job.get("id").and_then(Value::as_u64).filter(|id| *id > 0);
            require(
                id.is_some_and(|id| ids.insert(id))
                    && job.get("run_id").and_then(Value::as_u64) == Some(api.run_id.get())
                    && job
                        .get("run_attempt")
                        .is_none_or(|attempt| attempt.as_u64() == Some(api.run_attempt.get()))
                    && job
                        .get("head_sha")
                        .is_none_or(|sha| sha.as_str() == Some(api.controller_sha.as_str())),
                "unique exact-attempt API jobs",
            )?;
            let steps = job
                .get("steps")
                .and_then(Value::as_array)
                .ok_or_else(MissingInventory::new)?;
            let mut numbers = BTreeSet::new();
            for step in steps {
                require(
                    step.get("number")
                        .and_then(Value::as_u64)
                        .is_some_and(|number| number > 0 && numbers.insert(number)),
                    "unique API step numbers",
                )?;
            }
        }
        let mut digests = BTreeSet::from([revision.digest.as_str()]);
        let mut source = primary
            .pointer("/attempt/manifest/source_sha")
            .and_then(Value::as_str);
        self.compare_jobs(primary)?;
        for support in &self.supporting_revisions {
            let prepared = shared(&json!({"op":"prepare", "evidence":support.evidence}))?;
            require(
                digests.insert(&support.digest)
                    && prepared.get("digest").and_then(Value::as_str)
                        == Some(support.digest.as_str())
                    && prepared.get("evidence") == Some(&support.evidence)
                    && prepared
                        .pointer("/identity/repository_id")
                        .and_then(Value::as_u64)
                        == Some(revision.repository_id.get())
                    && prepared
                        .pointer("/identity/workflow_id")
                        .and_then(Value::as_u64)
                        == Some(revision.workflow_id.get())
                    && prepared.pointer("/identity/run_id").and_then(Value::as_u64)
                        == Some(revision.run_id.get())
                    && support
                        .evidence
                        .pointer("/attempt/run_attempt")
                        .and_then(Value::as_u64)
                        == Some(revision.run_attempt.get())
                    && support
                        .evidence
                        .pointer("/attempt/controller_sha")
                        .and_then(Value::as_str)
                        == Some(api.controller_sha.as_str()),
                "support must be a distinct committed revision of the same run attempt",
            )?;
            if let Some(known) = support
                .evidence
                .pointer("/attempt/manifest/source_sha")
                .and_then(Value::as_str)
            {
                require(
                    source.is_none_or(|source| source == known),
                    "supporting source identity conflicts",
                )?;
                source = Some(known);
            }
            self.compare_jobs(&support.evidence)?;
        }
        require(
            source == api.source_sha.as_deref(),
            "applicable source identity",
        )?;
        let mut view = primary.clone();
        let object = view.as_object_mut().ok_or_else(MissingInventory::new)?;
        _ = object.insert("api_evidence".to_owned(), json!(api));
        _ = object.insert(
            "supporting_revisions".to_owned(),
            json!(self.supporting_revisions),
        );
        Ok(view)
    }

    fn compare_jobs(&self, evidence: &Value) -> Result<(), AppError> {
        let jobs = evidence
            .pointer("/attempt/jobs")
            .and_then(Value::as_array)
            .ok_or_else(MissingInventory::new)?;
        for job in jobs {
            let id = job
                .get("id")
                .and_then(Value::as_u64)
                .ok_or_else(MissingInventory::new)?;
            let actual = self
                .api_evidence
                .jobs
                .iter()
                .find(|actual| actual.get("id").and_then(Value::as_u64) == Some(id))
                .ok_or_else(MissingInventory::new)?;
            compare_status(job, actual)?;
            if let Some(steps) = job.get("steps").and_then(Value::as_array) {
                let actual_steps = actual
                    .get("steps")
                    .and_then(Value::as_array)
                    .ok_or_else(MissingInventory::new)?;
                for step in steps {
                    let number = step
                        .get("number")
                        .and_then(Value::as_u64)
                        .ok_or_else(MissingInventory::new)?;
                    let actual = actual_steps
                        .iter()
                        .find(|actual| actual.get("number").and_then(Value::as_u64) == Some(number))
                        .ok_or_else(MissingInventory::new)?;
                    compare_status(step, actual)?;
                }
            }
        }
        Ok(())
    }
}

/// Exact API inventory supplied by the provenance-checking read adapter.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApiEvidence {
    repository_id: NonZero<u64>,
    workflow_id: NonZero<u64>,
    run_id: NonZero<u64>,
    run_attempt: NonZero<u64>,
    controller_sha: String,
    started_at: String,
    created_at: String,
    /// Planning can fail before any candidate source is selected.
    source_sha: Option<String>,
    total_count: usize,
    jobs: Vec<Value>,
}

/// A reporter revision restored from the same issue's committed page index.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SupportingRevision {
    pub(crate) digest: String,
    pub(crate) evidence: Value,
}

fn compare_status(observed: &Value, actual: &Value) -> Result<(), AppError> {
    for field in ["status", "conclusion"] {
        if let Some(value) = observed.get(field).filter(|value| !value.is_null()) {
            require(
                actual.get(field) == Some(value),
                "conflicting execution status requires reconciliation",
            )?;
        }
    }
    Ok(())
}

/// Identifies missing job/step identity or inventory rather than accepting partial execution proof.
#[ohno::error]
#[display("exact-attempt execution inventory is incomplete")]
struct MissingInventory;

/// Invalid API timestamps cannot supply the ordering that incomplete collection omitted.
#[ohno::error]
#[display("cannot parse exact-attempt API time")]
struct ParseApiTime;
