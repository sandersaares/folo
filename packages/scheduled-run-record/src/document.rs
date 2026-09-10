use std::collections::BTreeMap;
use std::num::NonZero;

use ohno::AppError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::canonical::{canonicalize, digest, json};
use crate::evidence::require;
use crate::pages::{
    Comment, DATA_PREFIX, DATA_SUFFIX, PAGE_BYTES, Page, SCHEMA_VERSION, decode_fragment,
    encode_fragment, validate_body_size,
};

/// Role-owned detail collections use the hosted byte framing but distinct operation namespaces.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DocumentKind {
    Triage,
    Problem,
}

impl DocumentKind {
    fn marker(self) -> &'static str {
        match self {
            Self::Triage => "scheduled-triage-detail",
            Self::Problem => "scheduled-problem-detail",
        }
    }
}

/// Prepared immutable detail, not yet referenced by an externally committed root.
#[derive(Debug, Serialize)]
pub(crate) struct PreparedDocument {
    digest: String,
    pages: Vec<Page>,
}

/// Complete detail history and its page-reference checkpoint.
#[derive(Debug, Serialize)]
pub(crate) struct Documents {
    revisions: Vec<DocumentRevision>,
    incomplete_revisions: Vec<String>,
    index_digest: String,
}

/// A canonical JSON detail whose bytes and page references have been restored.
#[derive(Debug, Serialize)]
struct DocumentRevision {
    digest: String,
    document: Value,
    pages: Vec<DocumentReference>,
}

/// Remote comment identity participates in the committed index, detecting deleted/replaced pages.
#[derive(Debug, Serialize)]
struct DocumentReference {
    operation_id: String,
    id: NonZero<u64>,
}

/// Coordinates are independent of the meaning of an analysis or problem document.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Header {
    schema_version: u8,
    kind: DocumentKind,
    owner: String,
    digest: String,
    page: NonZero<usize>,
    page_count: NonZero<usize>,
}

impl Header {
    fn operation_id(&self) -> String {
        format!(
            "{}/v1/{}/{}/{}",
            self.kind.marker(),
            self.owner,
            self.digest,
            self.page
        )
    }

    fn body(&self, bytes: &[u8]) -> Result<String, AppError> {
        let body = format!(
            "[Copilot speaking]\n<!-- {}:v1 {} -->\n\
             Owned analysis detail. Decode and concatenate pages; verify the canonical JSON digest.\
             {DATA_PREFIX}{}{DATA_SUFFIX}",
            self.kind.marker(),
            json(self)?,
            encode_fragment(bytes)
        );
        validate_body_size(&body)?;
        Ok(body)
    }
}

fn validate_owner(owner: &str) -> Result<(), AppError> {
    let parts: Vec<_> = owner.split('/').collect();
    require(
        parts.len() == 2
            && parts
                .iter()
                .all(|part| part.parse::<NonZero<u64>>().is_ok()),
        "document owner must identify numeric repository and issue",
    )
}

pub(crate) fn prepare_document(
    kind: DocumentKind,
    owner: &str,
    document: Value,
) -> Result<PreparedDocument, AppError> {
    validate_owner(owner)?;
    require(document.is_object(), "detail must be a JSON object")?;
    let canonical = json(&canonicalize(document))?;
    let digest = digest(&canonical);
    let count = NonZero::new(canonical.len().div_ceil(PAGE_BYTES))
        .expect("a JSON object has nonempty encoding");
    let pages = canonical
        .as_bytes()
        .chunks(PAGE_BYTES)
        .enumerate()
        .map(|(index, bytes)| {
            let header = Header {
                schema_version: SCHEMA_VERSION,
                kind,
                owner: owner.to_owned(),
                digest: digest.clone(),
                page: NonZero::new(index.checked_add(1).expect("a page occupies memory"))
                    .expect("a zero-based page index plus one is nonzero"),
                page_count: count,
            };
            Ok(Page {
                operation_id: header.operation_id(),
                body: header.body(bytes)?,
            })
        })
        .collect::<Result<_, AppError>>()?;
    Ok(PreparedDocument { digest, pages })
}

pub(crate) fn restore_documents(
    kind: DocumentKind,
    owner: &str,
    comments: Vec<Comment>,
) -> Result<Documents, AppError> {
    validate_owner(owner)?;
    let prefix = format!("[Copilot speaking]\n<!-- {}:v1 ", kind.marker());
    let mut groups = BTreeMap::<String, BTreeMap<usize, (NonZero<u64>, Header, Vec<u8>)>>::new();
    let mut ids = BTreeMap::new();
    for comment in comments {
        if let Some(previous) = ids.insert(comment.id, comment.body.clone()) {
            require(previous == comment.body, "conflicting comment identity")?;
        }
        let Some(rest) = comment.body.strip_prefix(&prefix) else {
            continue;
        };
        validate_body_size(&comment.body)?;
        let (header, rest) = rest
            .split_once(" -->\n")
            .ok_or_else(|| DocumentError::new("missing detail header".to_owned()))?;
        let header: Header = serde_json::from_str(header).map_err(ParseDocumentError::caused_by)?;
        require(
            header.kind == kind
                && header.owner == owner
                && header.schema_version == SCHEMA_VERSION
                && header.page <= header.page_count,
            "foreign or malformed detail coordinates",
        )?;
        let (_, payload) = rest
            .split_once(DATA_PREFIX)
            .ok_or_else(|| DocumentError::new("missing detail payload".to_owned()))?;
        let payload = payload
            .strip_suffix(DATA_SUFFIX)
            .ok_or_else(|| DocumentError::new("missing detail terminator".to_owned()))?;
        let bytes = decode_fragment(payload)?;
        require(
            header.body(&bytes)? == comment.body,
            "detail rendering differs",
        )?;
        let group = groups.entry(header.digest.clone()).or_default();
        if let Some((_, first, _)) = group.values().next() {
            require(
                first.page_count == header.page_count,
                "conflicting detail page counts",
            )?;
        }
        if let Some((id, previous, previous_bytes)) = group.get_mut(&header.page.get()) {
            require(
                previous == &header && previous_bytes == &bytes,
                "conflicting detail operation",
            )?;
            *id = (*id).min(comment.id);
        } else {
            _ = group.insert(header.page.get(), (comment.id, header, bytes));
        }
    }
    let mut revisions = Vec::new();
    let mut incomplete_revisions = Vec::new();
    for (expected_digest, pages) in groups {
        let (_, first, _) = pages
            .values()
            .next()
            .expect("a group is created by inserting a page");
        if pages.len() != first.page_count.get() {
            incomplete_revisions.push(expected_digest);
            continue;
        }
        let bytes: Vec<_> = pages
            .values()
            .flat_map(|(_, _, bytes)| bytes.iter().copied())
            .collect();
        let text = String::from_utf8(bytes).map_err(ReadDocumentError::caused_by)?;
        let document: Value = serde_json::from_str(&text).map_err(ParseDocumentError::caused_by)?;
        require(
            document.is_object()
                && json(&canonicalize(document.clone()))? == text
                && digest(&text) == expected_digest,
            "detail is not the declared canonical document",
        )?;
        let prepared = prepare_document(kind, owner, document.clone())?;
        require(
            prepared.pages.len() == pages.len()
                && prepared.pages.iter().zip(pages.values()).all(
                    |(expected, (_, header, bytes))| {
                        header.body(bytes).is_ok_and(|body| body == expected.body)
                    },
                ),
            "detail pages have different boundaries",
        )?;
        revisions.push(DocumentRevision {
            digest: expected_digest,
            document,
            pages: pages
                .into_values()
                .map(|(id, header, _)| DocumentReference {
                    operation_id: header.operation_id(),
                    id,
                })
                .collect(),
        });
    }
    // Payload bytes are already bound by their digest; bind remote references without rehashing logs.
    let index: Vec<_> = revisions
        .iter()
        .map(|revision| (&revision.digest, &revision.pages))
        .collect();
    Ok(Documents {
        index_digest: digest(&json(&index)?),
        revisions,
        incomplete_revisions,
    })
}

/// Describes a structural detail-record requirement.
#[ohno::error]
#[display("invalid detail record: {detail}")]
struct DocumentError {
    detail: String,
}

/// Identifies invalid UTF-8 after concatenating decoded fragments.
#[ohno::error]
#[display("cannot read detail bytes")]
struct ReadDocumentError;

/// Identifies invalid JSON in a detail header or payload.
#[ohno::error]
#[display("cannot parse detail JSON")]
struct ParseDocumentError;
