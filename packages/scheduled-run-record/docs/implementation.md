# Hosted run-evidence records

This nonpublished binary supplies pure, typed record machinery to the reviewed scheduled
reporter. Its behavioral owner is the
[scheduled workflow design](../../../.github/workflows/design.md). GitHub transport,
ownership checks, artifact parsing and controller provenance stay outside this package.
The utility does not read files, invoke processes, access the network, obtain credentials,
execute evidence, decide semantic problem identity or authorize repairs.

## Evidence and normalization

The reporter supplies repository and workflow API metadata and an observation of one run
attempt. Missing plan or checker artifacts are evidence gaps, not reasons to omit setup or
infrastructure failures. Job and step status remains authoritative even when a workflow or
checker result appears green. Validated intentional no-work is exempt only when no failure
or evidence gap accompanies it.

Object properties are recursively ordered by their names. Array order is preserved unless the
evidence schema explicitly identifies an unordered inventory: attempt jobs sort by numeric
job ID, their steps by numeric step number, attempt results by string `check_id`, and attempt
evidence gaps lexically. Missing IDs precede present IDs. Equal IDs use compact canonical JSON
as a deterministic tie-breaker, retaining every observation and duplicate.

Replay argument lists, flags, opaque manifest/plan arrays and unknown extension arrays retain
their original order. Reordering those arrays changes the digest; normalization never infers
that their order is insignificant. Unknown evidence object properties are retained. Missing
optional typed fields normalize to JSON null or their documented defaults. Generated gaps are
added once, without removing duplicate caller-supplied gaps.

`prepare` optionally accepts `transient_path_prefixes`, a list of known absolute reporter-local
directory paths. Only occurrences in `attempt.evidence_gaps` are replaced with
`<reporter-local>`, retaining surrounding scope and diagnostic text and relative suffixes.
Nested prefixes are matched longest first; prefix-list order and duplicates do not matter.
Matching is literal and case-sensitive, with path/token boundaries, and never consults the
filesystem. The collector supplies the actual path spellings emitted by its exceptions.
The prefixes are normalization configuration, not evidence, and do not enter the digest.
Raw checker diagnostics, job logs and all other observations remain unchanged. Neither an
unlisted random name nor an unknown path is guessed or normalized.

The digest is lowercase SHA-256 over the canonical compact JSON's UTF-8 bytes, with no BOM
or terminal newline. This is the serialized normalized evidence object, not a selected subset.
Changing any retained observation changes its revision digest. Identity belongs to numeric
repository ID, workflow ID and run ID; names, commit IDs and diagnostics are not issue keys.

## Publication and reconciliation

The reporter first finds or creates the issue using the exact root marker, then writes each
prepared evidence comment. It checks existing reporter-owned comments for matching operation
identities before retrying a write whose response may have been lost. It supplies the complete
comment inventory to `restore`, which verifies coordinates, byte content, canonical evidence
and digest. Only complete revisions enter the returned record. Identical duplicate deliveries
select the lowest comment ID deterministically. Conflicting duplicate content is an error.

Each complete revision is keyed by attempt number and digest. A changed observation within
the same attempt appends a revision. Out-of-order attempts sort numerically, and successful
reruns preserve prior failures. Digest order within an attempt does not imply chronology.
The `merge` operation takes the union of validated records, preserving all revisions.

The issue body is a bounded summary and root index, not a serialization of the entire history.
Complete history is discoverable in append-only reporter-owned evidence comment markers.
This permits unbounded attempt history without exhausting the issue-body limit. The reporter
updates the body only after restoring all newly written pages. Partial writes remain
discoverable but are never counted as complete revisions.

Rendering also returns `index_digest` and embeds it in a
`scheduled-run-publication:v1` checkpoint. The digest binds the ordered complete
revision identities and their GitHub comment references. Before appending, the
reporter checks restored history against that checkpoint. It can reconcile the
current prepared revision whose pages were written before an interrupted index
update; other unexplained differences block publication rather than silently
discarding deleted or missing committed history.

## Payload boundaries

Comments carry base64 fragments of the complete canonical UTF-8 JSON. Encoding prevents
embedded log text from imitating reporter markers or Markdown boundaries, and supports
splitting a UTF-8 code point across fragment boundaries. Decode fragments separately,
concatenate their bytes in numeric page order, then decode UTF-8 and validate the SHA-256.

Each fragment contains at most 40,000 bytes before base64 encoding. Every complete rendered
body is checked against a 60,000-byte ceiling, strictly below GitHub's 65,536-character limit.
The gap allows differences in character accounting and future metadata expansion. Encoded
payload pages are ASCII, making the byte bound conservative. Oversized metadata is rejected,
never silently shortened. Neither normalization nor rendering drops evidence.

Log excerpt limits belong to the collection boundary. Excerpts retain original log URLs,
byte counts and explicit capture/excerpt truncation flags. This package performs no further
truncation. All artifact references and API observations beyond the typed minimum fields
remain in the evidence digest and pages.

## JSON operation interface

Invoke `cargo run --locked --quiet --package scheduled-run-record`, sending one JSON object
on stdin. Stdout contains exactly one response JSON object. Invalid input produces no
success-shaped stdout and exits unsuccessfully with a diagnostic on stderr.

### Prepare

Input:

```json
{
  "op": "prepare",
  "evidence": {
    "repository": { "id": 123, "name": "owner/repository" },
    "workflow": {
      "id": 456,
      "name": "Full deep validation",
      "path": ".github/workflows/full-deep-validation.yml"
    },
    "run_id": 789,
    "attempt": {
      "run_attempt": 1,
      "run_number": 42,
      "created_at": "2026-09-09T01:00:00Z",
      "started_at": "2026-09-09T01:00:01Z",
      "completed_at": "2026-09-09T01:00:02Z",
      "workflow_conclusion": "failure",
      "run_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "controller_sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "manifest": null,
      "plan": null,
      "validated_no_work": false,
      "results": [],
      "jobs": [
        {
          "id": 101,
          "name": "Setup",
          "status": "completed",
          "conclusion": "failure",
          "steps": [
            {
              "number": 1,
              "name": "Install toolchain",
              "status": "completed",
              "conclusion": "failure"
            }
          ],
          "log": {
            "url": "https://api.github.com/repos/owner/repository/actions/jobs/101/logs",
            "excerpt": "Toolchain unavailable",
            "bytes": 21,
            "truncated": false,
            "excerpt_truncated": false
          }
        }
      ],
      "evidence_gaps": ["Checker artifacts unavailable"]
    }
  }
}
```

The response has `identity`, `issue_marker`, `should_report`, `digest`, normalized `evidence`
and `pages`, each with `operation_id` and `body`. `should_report: false` means do not create
a new intake issue; an existing run issue may still append the successful attempt.
Preparation never claims publication, even though all page contents are available locally.

`manifest` and `plan` are nullable observations already validated by the controller's owning
parsers. `validated_no_work` is the controller's explicit no-work attestation and requires
both observations. The utility does not reinterpret artifact schemas. Results have an
optional `outcome`; only `passed` is clean. Additional properties of evidence, repository,
workflow, attempt, job, step, result and log objects are preserved.

For collector exceptions containing scratch directories, the optional request property is:

```json
{
  "op": "prepare",
  "transient_path_prefixes": [
    "C:\\runner\\report-random",
    "C:\\runner\\report-random\\jobs-random"
  ],
  "evidence": {}
}
```

Here `evidence` is the complete object described above. Supply every known nested transient
directory whose name should not distinguish retries. Empty, relative and filesystem-root
prefixes are invalid. No paths are opened or interpreted as controller code.

### Restore, validate and merge

```json
{
  "op": "restore",
  "identity": { "repository_id": 123, "workflow_id": 456, "run_id": 789 },
  "comments": [{ "id": 111, "body": "the exact API-returned comment body" }]
}
```

The caller filters by authorized reporter ownership and supplies all pages of the API comment
inventory. Unrelated comments, including future triage comments, are ignored. Malformed
reporter page bodies are errors. The response is `{ "record": ..., "incomplete_revisions": [...] }`.
Incomplete revisions identify `run_attempt`, `digest`, `expected_pages` and `present_pages`.

The record contains `identity` and `revisions`. Each revision retains `digest`, `should_report`,
the normalized `evidence`, and ordered `pages: [{ "operation_id": "...", "id": 111 }]`.
The IDs are caller-observed GitHub IDs, not invented by this utility.

`{ "op": "validate", "record": ... }` returns the validated, deterministically ordered record.
`{ "op": "merge", "record": ..., "incoming": ... }` returns the validated union. Neither
operation verifies remote existence; `restore` is the publication boundary using the
caller's API-returned bodies.

### Render

`{ "op": "render", "record": ... }` returns `title`, `body`, `issue_marker` and `index_digest`.
An empty record is valid for preparing the initial issue before any evidence comments exist.
The title for new issues is `Deep validation failed`. The publication adapter preserves
existing issue titles, including operator edits, while refreshing the owned body.
Run issue identity uses repository, workflow and run IDs. Root marker syntax is
`<!-- scheduled-run:v1 {"repository_id":123,"run_id":789,"schema_version":1,"workflow_id":456} -->`.
The reporter labels these issues `scheduled-run-failure`; labels and GitHub writes remain
outside this utility.

Every evidence comment starts with `[Copilot speaking]` and
`<!-- scheduled-run-evidence:v1 ... -->`. Its header contains `schema_version: 1`, `identity`,
`run_attempt`, `digest`, `page` and `page_count`. Both root and page markers are compatible
with the shared scheduled-record reader. Page numbers start at one. Its operation ID is
`scheduled-run-evidence/v1/REPOSITORY_ID/WORKFLOW_ID/RUN_ID/ATTEMPT/DIGEST/PAGE`.
Root and evidence markers never identify semantic problems or repair work.

## Validation

The tests use synthetic in-memory observations and API responses. They exercise setup
failures without artifacts, mixed checker and job failures, deterministic normalization,
lost-response duplicate delivery, revision changes, out-of-order reruns, malformed identities,
missing metadata and lossless pagination. No validation test accesses GitHub or a real clock.
