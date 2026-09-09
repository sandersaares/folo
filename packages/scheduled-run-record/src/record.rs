use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZero;

use ohno::AppError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::canonical::{digest, json};
use crate::evidence::{Evidence, Identity, require};
use crate::pages::{
    Comment, DecodedPage, PAGE_BYTES, PageHeader, SCHEMA_VERSION, decode, validate_body_size,
};

/// Validated append-only revision catalogue reconstructed from complete evidence page sets.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    pub(crate) identity: Identity,
    pub(crate) revisions: Vec<Revision>,
}

impl Record {
    pub(crate) fn validate(mut self) -> Result<Self, AppError> {
        let mut keys = BTreeSet::new();
        let mut comment_ids = BTreeSet::new();
        for revision in &self.revisions {
            let evidence = revision.evidence.clone().validate()?;
            require(
                evidence.identity == self.identity,
                "revision belongs to a different run",
            )?;
            require(
                evidence.digest == revision.digest,
                "revision digest mismatch",
            )?;
            require(
                evidence.should_report == revision.should_report,
                "revision reporting verdict mismatch",
            )?;
            require(
                keys.insert((evidence.run_attempt, revision.digest.clone())),
                "record contains a duplicate revision",
            )?;
            require(
                evidence.canonical.len().div_ceil(PAGE_BYTES) == revision.pages.len(),
                "revision has an incomplete page reference list",
            )?;
            for (index, reference) in revision.pages.iter().enumerate() {
                let page = PageHeader {
                    schema_version: SCHEMA_VERSION,
                    identity: evidence.identity.clone(),
                    run_attempt: evidence.run_attempt,
                    digest: evidence.digest.clone(),
                    page: NonZero::new(
                        index
                            .checked_add(1)
                            .expect("a page reference occupies memory"),
                    )
                    .expect("a zero-based page index plus one is nonzero"),
                    page_count: NonZero::new(revision.pages.len())
                        .expect("canonical evidence has at least one page"),
                };
                require(
                    page.operation_id() == reference.operation_id,
                    "page reference has the wrong operation identity or order",
                )?;
                require(
                    comment_ids.insert(reference.id),
                    "comment reference is reused by different pages",
                )?;
            }
        }
        self.revisions.sort_by(|left, right| {
            (&left.evidence.attempt.run_attempt, &left.digest)
                .cmp(&(&right.evidence.attempt.run_attempt, &right.digest))
        });
        Ok(self)
    }
}

/// Immutable evidence revision plus verified GitHub references for every page.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Revision {
    pub(crate) digest: String,
    pub(crate) should_report: bool,
    pub(crate) evidence: Evidence,
    pub(crate) pages: Vec<PageReference>,
}

/// Caller-owned GitHub ID bound to a deterministic evidence-page operation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PageReference {
    pub(crate) operation_id: String,
    pub(crate) id: NonZero<u64>,
}

/// Restored committed revisions and explicitly unpublished partial deliveries.
#[derive(Debug, Serialize)]
pub(crate) struct Restored {
    pub(crate) record: Record,
    pub(crate) incomplete_revisions: Vec<IncompleteRevision>,
}

/// Diagnostic for an interrupted page sequence that is not a published revision.
#[derive(Debug, Serialize)]
pub(crate) struct IncompleteRevision {
    run_attempt: NonZero<u64>,
    digest: String,
    expected_pages: NonZero<usize>,
    present_pages: Vec<NonZero<usize>>,
}

/// Bounded GitHub issue update; durable full history remains in evidence comments.
#[derive(Debug, Serialize)]
pub(crate) struct Rendered {
    title: &'static str,
    body: String,
    issue_marker: String,
    index_digest: String,
}

/// Binds the committed issue summary to the exact complete revisions and GitHub page references.
///
/// The reporter compares this checkpoint with restored comments before appending evidence, so
/// lost/deleted committed pages cannot silently shrink the indexed history.
#[derive(Serialize)]
struct PublicationCheckpoint<'a> {
    schema_version: u8,
    index_digest: &'a str,
}

/// Compact run root interoperable with the shared scheduled-record reader.
///
/// Fields follow the shared writer's alphabetical order so an equivalent root has one marker
/// spelling, including while publication is pending. No revision is implied by this identity.
#[derive(Debug, Serialize)]
struct RunRoot {
    repository_id: NonZero<u64>,
    run_id: NonZero<u64>,
    schema_version: u8,
    workflow_id: NonZero<u64>,
}

pub(crate) fn issue_marker(identity: &Identity) -> Result<String, AppError> {
    let root = RunRoot {
        repository_id: identity.repository_id,
        run_id: identity.run_id,
        schema_version: SCHEMA_VERSION,
        workflow_id: identity.workflow_id,
    };
    Ok(format!("<!-- scheduled-run:v1 {} -->", json(&root)?))
}

pub(crate) fn restore(identity: Identity, comments: Vec<Comment>) -> Result<Restored, AppError> {
    let mut groups: BTreeMap<_, BTreeMap<_, (NonZero<u64>, DecodedPage)>> = BTreeMap::new();
    let mut ids = BTreeMap::new();
    for comment in comments {
        if let Some(previous) = ids.insert(comment.id, comment.body.clone()) {
            require(
                previous == comment.body,
                "same comment ID has conflicting bodies",
            )?;
        }
        let Some(page) = decode(&comment.body)? else {
            continue;
        };
        require(
            page.header.identity == identity,
            "evidence page belongs to another run",
        )?;
        let key = (page.header.run_attempt, page.header.digest.clone());
        let pages = groups.entry(key).or_default();
        if let Some((_, first)) = pages.first_key_value().map(|(_, entry)| entry) {
            require(
                first.header.page_count == page.header.page_count,
                "revision page counts disagree",
            )?;
        }
        if let Some((id, previous)) = pages.get_mut(&page.header.page) {
            require(
                previous.header == page.header && previous.bytes == page.bytes,
                "duplicate operation has conflicting evidence",
            )?;
            // A successful POST whose response was lost can leave duplicate identical comments.
            // Select the lowest numeric API ID deterministically, without deleting either copy.
            *id = (*id).min(comment.id);
        } else {
            _ = pages.insert(page.header.page, (comment.id, page));
        }
    }
    let mut revisions = Vec::new();
    let mut incomplete_revisions = Vec::new();
    for ((run_attempt, expected_digest), pages) in groups {
        let (_, first) = pages
            .first_key_value()
            .map(|(_, entry)| entry)
            .expect("groups are created only when inserting an evidence page");
        let expected_pages = first.header.page_count;
        if pages.len() != expected_pages.get() {
            incomplete_revisions.push(IncompleteRevision {
                run_attempt,
                digest: expected_digest,
                expected_pages,
                present_pages: pages.keys().copied().collect(),
            });
            continue;
        }
        let bytes: Vec<_> = pages
            .values()
            .flat_map(|(_, page)| page.bytes.iter().copied())
            .collect();
        let canonical = String::from_utf8(bytes).map_err(ReadEvidenceError::caused_by)?;
        let evidence: Evidence =
            serde_json::from_str(&canonical).map_err(ParseEvidenceError::caused_by)?;
        let evidence = evidence.validate()?;
        // Equality with the normalized bytes makes their computed digest also attest the
        // received bytes; hashing both copies would repeat identical work.
        require(
            evidence.identity == identity
                && evidence.run_attempt == run_attempt
                && evidence.digest == expected_digest
                && evidence.canonical == canonical,
            "page marker does not identify canonical evidence",
        )?;
        require(
            canonical.len().div_ceil(PAGE_BYTES) == pages.len()
                && canonical
                    .as_bytes()
                    .chunks(PAGE_BYTES)
                    .zip(pages.values())
                    .all(|(expected, (_, actual))| expected == actual.bytes),
            "evidence fragments do not match deterministic page boundaries",
        )?;
        let references = pages
            .into_values()
            .map(|(id, page)| PageReference {
                operation_id: page.header.operation_id(),
                id,
            })
            .collect();
        revisions.push(Revision {
            digest: evidence.digest,
            should_report: evidence.should_report,
            evidence: evidence.evidence,
            pages: references,
        });
    }
    Ok(Restored {
        // Every revision was validated while decoding, and the ordered maps establish unique
        // revision keys and contiguous pages. Do not hash and encode the same evidence again.
        record: Record {
            identity,
            revisions,
        },
        incomplete_revisions,
    })
}

pub(crate) fn merge(record: Record, incoming: Record) -> Result<Record, AppError> {
    let mut record = record.validate()?;
    let incoming = incoming.validate()?;
    require(
        record.identity == incoming.identity,
        "cannot merge records for different runs",
    )?;
    for revision in incoming.revisions {
        if let Some(existing) = record.revisions.iter_mut().find(|existing| {
            existing.evidence.attempt.run_attempt == revision.evidence.attempt.run_attempt
                && existing.digest == revision.digest
        }) {
            for (existing, incoming) in existing.pages.iter_mut().zip(revision.pages) {
                existing.id = existing.id.min(incoming.id);
            }
        } else {
            record.revisions.push(revision);
        }
    }
    let mut ids = BTreeSet::new();
    for reference in record.revisions.iter().flat_map(|revision| &revision.pages) {
        require(
            ids.insert(reference.id),
            "merged comment reference is reused by different pages",
        )?;
    }
    record.revisions.sort_by(|left, right| {
        (&left.evidence.attempt.run_attempt, &left.digest)
            .cmp(&(&right.evidence.attempt.run_attempt, &right.digest))
    });
    Ok(record)
}

pub(crate) fn render(record: Record) -> Result<Rendered, AppError> {
    let record = record.validate()?;
    let marker = issue_marker(&record.identity)?;
    // Evidence digests bind content; page references additionally bind the persisted GitHub
    // copies. Avoid reserializing the potentially large evidence payload merely to checkpoint it.
    let references: Vec<_> = record
        .revisions
        .iter()
        .map(|revision| {
            (
                revision.evidence.attempt.run_attempt,
                &revision.digest,
                &revision.pages,
            )
        })
        .collect();
    let index_digest = digest(&json(&(&record.identity, references))?);
    let checkpoint = json(&PublicationCheckpoint {
        schema_version: SCHEMA_VERSION,
        index_digest: &index_digest,
    })?;
    let attempts: BTreeSet<_> = record
        .revisions
        .iter()
        .map(|revision| revision.evidence.attempt.run_attempt)
        .collect();
    let failed = record
        .revisions
        .iter()
        .filter(|revision| revision.should_report)
        .count();
    let latest = if let Some(revision) = record.revisions.last() {
        let reference = revision
            .pages
            .first()
            .expect("validated revisions have at least one evidence page");
        format!(
            "Highest recorded attempt: {}. [Evidence page](#issuecomment-{}).\n\
             Revisions within an attempt are ordered by digest, not by inferred arrival time.\n",
            revision.evidence.attempt.run_attempt, reference.id,
        )
    } else {
        "Evidence publication is pending; no complete revision is indexed.\n".to_owned()
    };
    let body = format!(
        "[Copilot speaking]\n{marker}\n\
         <!-- scheduled-run-publication:v1 {checkpoint} -->\n\
         # Scheduled validation failed\n\n\
         Hosted evidence intake only. This issue is not a triaged problem, a semantic finding, \
         or repair authorization.\n\n\
         Repository ID: {}. Workflow ID: {}. Run ID: {}.\n\
         Recorded attempts: {}. Complete evidence revisions: {}. \
         Failed or incomplete evidence revisions: {}.\n\n\
         {latest}\n\
         The complete append-only history is stored in reporter-owned \
         `scheduled-run-evidence:v1` comments. Each marker identifies its attempt, digest and \
         page position. All pages must exist and validate before a revision is indexed. \
         Partial page sequences remain evidence but are not indexed as complete revisions. \
         Successful reruns do not erase earlier failure evidence.\n",
        record.identity.repository_id,
        record.identity.workflow_id,
        record.identity.run_id,
        attempts.len(),
        record.revisions.len(),
        failed,
    );
    validate_body_size(&body)?;
    Ok(Rendered {
        title: "Scheduled validation failed",
        body,
        issue_marker: marker,
        index_digest,
    })
}

pub(crate) fn validate_json(value: Value) -> Result<Record, AppError> {
    let record: Record = serde_json::from_value(value).map_err(ParseEvidenceError::caused_by)?;
    record.validate()
}

/// Identifies invalid UTF-8 in fully assembled evidence, not a permissible split code point.
#[ohno::error]
#[display("cannot read assembled evidence as UTF-8")]
struct ReadEvidenceError;

/// Identifies JSON that cannot represent the typed evidence or record contract.
#[ohno::error]
#[display("cannot parse stored run evidence")]
struct ParseEvidenceError;
