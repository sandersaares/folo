use std::num::NonZero;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ohno::AppError;
use serde::{Deserialize, Serialize};

use crate::canonical::json;
use crate::evidence::{Identity, ValidatedEvidence, require};

/// Payload cap measured in UTF-8 bytes, stricter than GitHub's character cap.
///
/// The unused space below GitHub's 65,536-character limit accommodates API counting differences
/// and future reporter metadata without reducing the retained evidence. Ref: implementation.md.
pub(crate) const BODY_LIMIT: usize = 60_000;

/// Raw evidence bytes per page; base64 expansion leaves space for the marker and explanatory text.
pub(crate) const PAGE_BYTES: usize = 40_000;

/// Shared scheduled-record schema understood by the reporter and Local intake parsers.
pub(crate) const SCHEMA_VERSION: u8 = 1;

/// Exact reporter-owned page prefix, distinct from run roots and future triage comments.
const PAGE_PREFIX: &str = "[Copilot speaking]\n<!-- scheduled-run-evidence:v1 ";
pub(crate) const DATA_PREFIX: &str = "\n```base64\n";
pub(crate) const DATA_SUFFIX: &str = "\n```\n";

/// SHA-256 digests use two lowercase hexadecimal characters per digest byte.
const DIGEST_HEX_LENGTH: usize = 64;

/// Transport-ready comment with an operation identity independent of GitHub response IDs.
#[derive(Debug, Serialize)]
pub(crate) struct Page {
    pub(crate) operation_id: String,
    pub(crate) body: String,
}

/// Stable page coordinates used to recover partially delivered revisions without lost responses.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PageHeader {
    pub(crate) schema_version: u8,
    pub(crate) identity: Identity,
    pub(crate) run_attempt: NonZero<u64>,
    pub(crate) digest: String,
    pub(crate) page: NonZero<usize>,
    pub(crate) page_count: NonZero<usize>,
}

impl PageHeader {
    pub(crate) fn operation_id(&self) -> String {
        format!(
            "scheduled-run-evidence/v1/{}/{}/{}/{}",
            self.identity.key(),
            self.run_attempt,
            self.digest,
            self.page
        )
    }
}

/// API-returned comment content supplied by the caller after author/ownership verification.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Comment {
    pub(crate) id: NonZero<u64>,
    pub(crate) body: String,
}

/// A decoded page whose marker and payload have passed structural validation.
#[derive(Debug)]
pub(crate) struct DecodedPage {
    pub(crate) header: PageHeader,
    pub(crate) bytes: Vec<u8>,
}

pub(crate) fn prepare_pages(evidence: &ValidatedEvidence) -> Result<Vec<Page>, AppError> {
    paginate(
        &evidence.identity,
        evidence.run_attempt,
        &evidence.digest,
        &evidence.canonical,
        PAGE_BYTES,
    )
}

pub(crate) fn validate_body_size(body: &str) -> Result<(), AppError> {
    require(
        body.len() <= BODY_LIMIT,
        "body exceeds GitHub payload budget",
    )
}

pub(crate) fn paginate(
    identity: &Identity,
    run_attempt: NonZero<u64>,
    digest: &str,
    canonical: &str,
    page_bytes: usize,
) -> Result<Vec<Page>, AppError> {
    require(
        page_bytes > 0 && page_bytes <= PAGE_BYTES && !canonical.is_empty(),
        "invalid page size",
    )?;
    let count = NonZero::new(canonical.len().div_ceil(page_bytes))
        .expect("nonempty bytes and a positive page size always produce at least one page");
    canonical
        .as_bytes()
        .chunks(page_bytes)
        .enumerate()
        .map(|(index, bytes)| {
            let header = PageHeader {
                schema_version: SCHEMA_VERSION,
                identity: identity.clone(),
                run_attempt,
                digest: digest.to_owned(),
                page: NonZero::new(index.checked_add(1).expect("a page occupies memory"))
                    .expect("a zero-based page index plus one is nonzero"),
                page_count: count,
            };
            let body = page_body(&header, bytes)?;
            Ok(Page {
                operation_id: header.operation_id(),
                body,
            })
        })
        .collect()
}

fn page_body(header: &PageHeader, bytes: &[u8]) -> Result<String, AppError> {
    let body = format!(
        "{PAGE_PREFIX}{} -->\n\
         Hosted evidence only; not a triaged problem or repair authorization.\n\
         Attempt {}, evidence page {} of {}. Decode and concatenate base64 pages in page order \
         to recover the canonical UTF-8 JSON; verify its SHA-256 against the marker.\
         {DATA_PREFIX}{}{DATA_SUFFIX}",
        json(header)?,
        header.run_attempt,
        header.page,
        header.page_count,
        encode_fragment(bytes),
    );
    validate_body_size(&body)?;
    Ok(body)
}

pub(crate) fn decode(body: &str) -> Result<Option<DecodedPage>, AppError> {
    let Some(rest) = body.strip_prefix(PAGE_PREFIX) else {
        return Ok(None);
    };
    validate_body_size(body)?;
    let (header, rest) = rest
        .split_once(" -->\n")
        .ok_or_else(|| MalformedPageError::new("missing header terminator".to_owned()))?;
    let header: PageHeader = serde_json::from_str(header).map_err(ParsePageError::caused_by)?;
    require(
        header.schema_version == SCHEMA_VERSION
            && header.page <= header.page_count
            && valid_digest(&header.digest),
        "invalid page coordinates or digest",
    )?;
    let (_, payload) = rest
        .split_once(DATA_PREFIX)
        .ok_or_else(|| MalformedPageError::new("missing payload".to_owned()))?;
    let payload = payload
        .strip_suffix(DATA_SUFFIX)
        .ok_or_else(|| MalformedPageError::new("missing payload terminator".to_owned()))?;
    let bytes = decode_fragment(payload)?;
    // The exact rendering is part of page identity; a conflicting retry must not be accepted.
    require(
        page_body(&header, &bytes)? == body,
        "evidence page rendering mismatch",
    )?;
    Ok(Some(DecodedPage { header, bytes }))
}

pub(crate) fn encode_fragment(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

pub(crate) fn valid_digest(value: &str) -> bool {
    value.len() == DIGEST_HEX_LENGTH
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn decode_fragment(payload: &str) -> Result<Vec<u8>, AppError> {
    let bytes = STANDARD
        .decode(payload)
        .map_err(DecodePageError::caused_by)?;
    require(
        !bytes.is_empty() && bytes.len() <= PAGE_BYTES,
        "empty or oversized evidence fragment",
    )?;
    Ok(bytes)
}

/// Identifies malformed reporter-owned page syntax rather than an unrelated comment.
#[ohno::error]
#[display("malformed evidence page: {detail}")]
struct MalformedPageError {
    detail: String,
}

/// Identifies an invalid typed page header.
#[ohno::error]
#[display("cannot parse evidence page header")]
struct ParsePageError;

/// Identifies a page whose base64 payload cannot be decoded without data loss.
#[ohno::error]
#[display("cannot decode evidence page")]
struct DecodePageError;
