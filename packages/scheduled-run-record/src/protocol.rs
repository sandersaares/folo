use ohno::AppError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::canonical::{digest, json, normalized};
use crate::document::{DocumentKind, prepare_document, restore_documents};
use crate::evidence::{Evidence, Identity};
use crate::gap_paths::normalize_gap_paths;
use crate::pages::{Comment, Page, prepare_pages};
use crate::record::{Record, issue_marker, merge, render, restore, validate_json};

/// Controller operations; all persistence and API ownership checks remain with the caller.
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Prepare {
        evidence: Box<Evidence>,
        #[serde(default)]
        transient_path_prefixes: Vec<String>,
    },
    Restore {
        identity: Identity,
        comments: Vec<Comment>,
    },
    Merge {
        record: Record,
        incoming: Record,
    },
    Validate {
        record: Value,
    },
    Render {
        record: Record,
    },
    PrepareDocument {
        kind: DocumentKind,
        owner: String,
        document: Value,
    },
    RestoreDocuments {
        kind: DocumentKind,
        owner: String,
        comments: Vec<Comment>,
    },
    Fingerprint {
        value: Value,
    },
}

/// Prepared evidence and deterministic page writes, none yet acknowledged as published.
#[derive(Debug, Serialize)]
struct Prepared {
    identity: Identity,
    issue_marker: String,
    should_report: bool,
    digest: String,
    evidence: Value,
    pages: Vec<Page>,
}

/// Executes one complete JSON request without external side effects.
pub fn execute(text: &str) -> Result<String, AppError> {
    let request: Request = serde_json::from_str(text).map_err(ParseRequestError::caused_by)?;
    match request {
        Request::Prepare {
            mut evidence,
            transient_path_prefixes,
        } => {
            normalize_gap_paths(&mut evidence.attempt.evidence_gaps, transient_path_prefixes)?;
            let evidence = evidence.validate()?;
            let pages = prepare_pages(&evidence)?;
            json(&Prepared {
                issue_marker: issue_marker(&evidence.identity)?,
                identity: evidence.identity,
                should_report: evidence.should_report,
                digest: evidence.digest,
                evidence: normalized(&evidence.evidence)?,
                pages,
            })
        }
        Request::Restore { identity, comments } => json(&restore(identity, comments)?),
        Request::Merge { record, incoming } => json(&merge(record, incoming)?),
        Request::Validate { record } => json(&validate_json(record)?),
        Request::Render { record } => json(&render(record)?),
        Request::PrepareDocument {
            kind,
            owner,
            document,
        } => json(&prepare_document(kind, &owner, document)?),
        Request::RestoreDocuments {
            kind,
            owner,
            comments,
        } => json(&restore_documents(kind, &owner, comments)?),
        Request::Fingerprint { value } => {
            json(&serde_json::json!({"digest": digest(&json(&normalized(&value)?)?)}))
        }
    }
}

/// Identifies malformed requests before any output is emitted.
#[ohno::error]
#[display("cannot parse run-record request")]
struct ParseRequestError;
