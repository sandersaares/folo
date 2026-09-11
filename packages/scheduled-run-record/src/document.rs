use std::collections::BTreeMap;
use std::num::NonZero;

use ohno::AppError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::canonical::{canonicalize, digest, json};
use crate::evidence::require;
use crate::pages::{
    Comment, DATA_PREFIX, DATA_SUFFIX, PAGE_BYTES, Page, SCHEMA_VERSION, decode_fragment,
    encode_fragment, valid_digest, validate_body_size,
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
                && header.page <= header.page_count
                && valid_digest(&header.digest),
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

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::pages::BODY_LIMIT;
    use crate::protocol::execute;

    fn comments(prepared: &PreparedDocument) -> Vec<Comment> {
        prepared
            .pages
            .iter()
            .zip(1_u64..)
            .map(|(page, id)| Comment {
                id: NonZero::new(id).unwrap(),
                body: page.body.clone(),
            })
            .collect()
    }

    fn header(bytes: &[u8]) -> Header {
        Header {
            schema_version: SCHEMA_VERSION,
            kind: DocumentKind::Triage,
            owner: "123/20".to_owned(),
            digest: digest(std::str::from_utf8(bytes).unwrap()),
            page: NonZero::<usize>::MIN,
            page_count: NonZero::<usize>::MIN,
        }
    }

    fn one(body: String) -> Vec<Comment> {
        vec![Comment {
            id: NonZero::<u64>::MIN,
            body,
        }]
    }

    #[test]
    fn both_role_kinds_roundtrip_without_losing_unicode_or_marker_like_data() {
        for kind in [DocumentKind::Triage, DocumentKind::Problem] {
            let value =
                json!({"diagnosis":"\u{1f642} <!-- scheduled-triage:v1 {} -->", "scope":["b","a"]});
            let prepared = prepare_document(kind, "123/20", value.clone()).unwrap();
            let restored = restore_documents(kind, "123/20", comments(&prepared)).unwrap();
            assert!(restored.incomplete_revisions.is_empty());
            assert_eq!(restored.revisions.first().unwrap().document, value);
            assert_eq!(restored.revisions.first().unwrap().digest, prepared.digest);
            assert!(
                prepared
                    .pages
                    .iter()
                    .all(|page| page.body.len() <= BODY_LIMIT)
            );
        }
    }

    #[test]
    fn json_protocol_prepares_restores_and_fingerprints_role_documents() {
        let prepared: Value = serde_json::from_str(&execute(&json!({
            "op":"prepare_document","kind":"triage","owner":"123/20","document":{"analysis":"complete"}
        }).to_string()).unwrap()).unwrap();
        let pages: Vec<_> = prepared
            .get("pages")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .zip(1_u64..)
            .map(|(page, id)| json!({"id":id,"body":page.get("body").unwrap()}))
            .collect();
        let restored: Value = serde_json::from_str(
            &execute(
                &json!({
                    "op":"restore_documents","kind":"triage","owner":"123/20","comments":pages
                })
                .to_string(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            restored.get("revisions").unwrap().as_array().unwrap().len(),
            1
        );
        let fingerprint: Value = serde_json::from_str(
            &execute(
                &json!({
                    "op":"fingerprint","value":{"analysis":"complete"}
                })
                .to_string(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(fingerprint.get("digest"), prepared.get("digest"));
    }

    #[test]
    fn unknown_owners_scalar_documents_and_malformed_coordinates_are_rejected() {
        for owner in ["", "123", "0/20", "owner/20", "123/20/30"] {
            _ = prepare_document(DocumentKind::Triage, owner, json!({})).unwrap_err();
            _ = restore_documents(DocumentKind::Triage, owner, Vec::new()).unwrap_err();
        }
        _ = prepare_document(DocumentKind::Triage, "123/20", json!([])).unwrap_err();
        let bytes = b"{}";
        for variant in 0..5 {
            let mut header = header(bytes);
            match variant {
                0 => header.schema_version = 2,
                1 => header.owner = "124/20".to_owned(),
                2 => header.kind = DocumentKind::Problem,
                3 => header.page = NonZero::new(2).unwrap(),
                _ => header.digest = "not-a-digest".to_owned(),
            }
            let body = header.body(bytes).unwrap();
            if header.kind == DocumentKind::Problem {
                assert!(
                    restore_documents(DocumentKind::Triage, "123/20", one(body))
                        .unwrap()
                        .revisions
                        .is_empty()
                );
            } else {
                _ = restore_documents(DocumentKind::Triage, "123/20", one(body)).unwrap_err();
            }
        }
    }

    #[test]
    fn duplicate_delivery_uses_known_lowest_id_and_rejects_conflicts() {
        let prepared = prepare_document(DocumentKind::Triage, "123/20", json!({"a":1})).unwrap();
        let mut input = comments(&prepared);
        let mut duplicate = input.first().unwrap().clone();
        duplicate.id = NonZero::new(9).unwrap();
        input.push(duplicate);
        input.push(input.first().unwrap().clone());
        let restored = restore_documents(DocumentKind::Triage, "123/20", input.clone()).unwrap();
        assert_eq!(
            restored
                .revisions
                .first()
                .unwrap()
                .pages
                .first()
                .unwrap()
                .id
                .get(),
            1
        );
        input.last_mut().unwrap().body.push(' ');
        _ = restore_documents(DocumentKind::Triage, "123/20", input).unwrap_err();

        let first = header(b"{\"a\":1}");
        let conflicting = vec![
            Comment {
                id: NonZero::new(1).unwrap(),
                body: first.body(b"{\"a\":1}").unwrap(),
            },
            Comment {
                id: NonZero::new(2).unwrap(),
                body: first.body(b"{\"a\":2}").unwrap(),
            },
        ];
        _ = restore_documents(DocumentKind::Triage, "123/20", conflicting).unwrap_err();
    }

    #[test]
    fn partial_pages_are_not_committed_and_page_count_conflicts_fail() {
        let mut first = header(b"{}");
        first.page_count = NonZero::new(2).unwrap();
        let body = first.body(b"{}").unwrap();
        let restored =
            restore_documents(DocumentKind::Triage, "123/20", one(body.clone())).unwrap();
        assert!(restored.revisions.is_empty());
        assert_eq!(restored.incomplete_revisions, vec![first.digest.clone()]);
        first.page = NonZero::new(2).unwrap();
        first.page_count = NonZero::new(3).unwrap();
        _ = restore_documents(
            DocumentKind::Triage,
            "123/20",
            vec![
                Comment {
                    id: NonZero::new(1).unwrap(),
                    body,
                },
                Comment {
                    id: NonZero::new(2).unwrap(),
                    body: first.body(b"{}").unwrap(),
                },
            ],
        )
        .unwrap_err();
    }

    #[test]
    fn complete_fragments_must_use_the_declared_canonical_page_boundaries() {
        let mut first = header(b"{}");
        first.page_count = NonZero::new(2).unwrap();
        let mut second = first.clone();
        second.page = NonZero::new(2).unwrap();
        _ = restore_documents(
            DocumentKind::Triage,
            "123/20",
            vec![
                Comment {
                    id: NonZero::new(1).unwrap(),
                    body: first.body(b"{").unwrap(),
                },
                Comment {
                    id: NonZero::new(2).unwrap(),
                    body: second.body(b"}").unwrap(),
                },
            ],
        )
        .unwrap_err();
    }

    #[test]
    fn malformed_framing_encoding_and_noncanonical_payloads_fail() {
        let valid = header(b"{}").body(b"{}").unwrap();
        for body in [
            valid.replace(" -->\n", " -->"),
            valid.replacen("{\"schema_version\"", "{broken", 1),
            valid.replace(DATA_PREFIX, "\n"),
            valid.trim_end().to_owned(),
            valid.replace("Owned analysis detail.", "different prose"),
            format!(
                "{}{DATA_PREFIX}%{DATA_SUFFIX}",
                valid.split_once(DATA_PREFIX).unwrap().0
            ),
        ] {
            _ = restore_documents(DocumentKind::Triage, "123/20", one(body)).unwrap_err();
        }
        for bytes in [
            b"{".as_slice(),
            b"[]".as_slice(),
            b"{ \"a\": 1 }".as_slice(),
        ] {
            _ = restore_documents(
                DocumentKind::Triage,
                "123/20",
                one(header(bytes).body(bytes).unwrap()),
            )
            .unwrap_err();
        }
        _ = restore_documents(
            DocumentKind::Triage,
            "123/20",
            one(header(b"{}").body(&[0xff]).unwrap()),
        )
        .unwrap_err();
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "uses the production payload-size boundary; small framing cases remain interpreted"
    )]
    fn production_boundaries_remain_lossless_and_size_limited() {
        let value = json!({"text":"x".repeat(PAGE_BYTES)});
        let prepared = prepare_document(DocumentKind::Triage, "123/20", value.clone()).unwrap();
        assert_eq!(prepared.pages.len(), 2);
        let restored =
            restore_documents(DocumentKind::Triage, "123/20", comments(&prepared)).unwrap();
        assert_eq!(restored.revisions.first().unwrap().document, value);
        let mut oversized = header(b"{}");
        oversized.owner = "x".repeat(BODY_LIMIT);
        _ = oversized.body(b"{}").unwrap_err();
        _ = restore_documents(
            DocumentKind::Triage,
            "123/20",
            one(format!(
                "[Copilot speaking]\n<!-- scheduled-triage-detail:v1 {}",
                "x".repeat(BODY_LIMIT)
            )),
        )
        .unwrap_err();
    }
}
