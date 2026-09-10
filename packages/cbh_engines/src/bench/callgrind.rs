//! Parsing of Gungraun's Callgrind `summary.json` (schema version 6) into the
//! engine-neutral [`BenchmarkResult`] model.
//!
//! Only the fields the tool needs are modelled; serde ignores the rest. The
//! committed fixtures under `tests/fixtures/callgrind/` are real Gungraun output
//! and act as a schema-drift canary: if Gungraun changes its format, parsing the
//! fixtures fails and the mismatch is caught immediately.

use std::collections::BTreeMap;
use std::panic::{RefUnwindSafe, UnwindSafe};

use cbh_model::{BenchmarkId, BenchmarkResult, Metric, MetricKind};
use nonempty::NonEmpty;
use ohno::OhnoCore;
use serde::Deserialize;

/// The Gungraun summary schema version this parser understands.
const SUPPORTED_VERSION: &str = "6";

/// An error encountered while parsing a Callgrind `summary.json`.
///
/// The error retains the concrete parsing failure in its source chain and
/// displays that failure without adding aggregate-level wording.
#[derive(ohno::Error)]
#[no_constructors]
#[from(CallgrindJsonError, UnsupportedCallgrindVersionError)]
pub struct CallgrindParseError {
    #[error]
    core: OhnoCore,
}

/// A Callgrind `summary.json` document was malformed.
///
/// The text was not valid JSON or did not match the expected shape.
#[ohno::error]
#[display("failed to parse Callgrind summary")]
#[from(serde_json::Error)]
struct CallgrindJsonError;

/// A Callgrind summary declared a schema version the tool does not support.
#[ohno::error]
#[display(
    "unsupported Gungraun summary schema version {version:?} \
     (expected {supported_version:?})"
)]
struct UnsupportedCallgrindVersionError {
    version: String,
    /// The display template can only interpolate this type's own fields, so the
    /// version the parser understands travels with the error instead of being
    /// read from `SUPPORTED_VERSION` at format time.
    supported_version: String,
}

// The OhnoCore field contains Arc<dyn Error + Send + Sync>, which is !UnwindSafe because Arc
// requires T: RefUnwindSafe and trait objects are !RefUnwindSafe. However, ohno error types are
// immutable after construction — no &self method mutates internal state — so observing them
// through a shared reference during unwind is harmless.
impl UnwindSafe for CallgrindParseError {}
impl RefUnwindSafe for CallgrindParseError {}
impl UnwindSafe for CallgrindJsonError {}
impl RefUnwindSafe for CallgrindJsonError {}
impl UnwindSafe for UnsupportedCallgrindVersionError {}
impl RefUnwindSafe for UnsupportedCallgrindVersionError {}

/// Parses one Callgrind `summary.json` into a [`BenchmarkResult`].
///
/// # Errors
///
/// Returns [`CallgrindParseError`] if the JSON is malformed or declares an
/// unsupported schema version.
pub fn parse_callgrind_summary(json: &str) -> Result<BenchmarkResult, CallgrindParseError> {
    let summary = parse_summary(json)?;
    Ok(summary_to_record(&summary))
}

/// Deserializes and version-checks a summary without mapping it.
fn parse_summary(json: &str) -> Result<Summary, CallgrindParseError> {
    let summary: Summary = serde_json::from_str(json).map_err(CallgrindJsonError::from)?;
    if summary.version != SUPPORTED_VERSION {
        let error = UnsupportedCallgrindVersionError::new(summary.version, SUPPORTED_VERSION);
        return Err(error.into());
    }
    Ok(summary)
}

/// Maps a parsed summary to a [`BenchmarkResult`] (pure).
fn summary_to_record(summary: &Summary) -> BenchmarkResult {
    let segments: Vec<String> = [
        summary
            .package_dir
            .as_deref()
            .and_then(package_name_from_dir),
        Some(summary.module_path.clone()),
        Some(summary.function_name.clone()),
        summary.id.clone(),
    ]
    .into_iter()
    .flatten()
    .filter(|segment| !segment.is_empty())
    .collect();
    let segments = NonEmpty::from_vec(segments)
        .unwrap_or_else(|| NonEmpty::new(summary.function_name.clone()));
    let id = BenchmarkId::new(segments);

    let mut metrics = Vec::new();
    for profile in &summary.profiles {
        let Some(events) = &profile.summaries.total.summary.callgrind else {
            continue;
        };
        for (event_kind, entry) in events {
            let Some(kind) = classify(event_kind) else {
                continue;
            };
            let Some(value) = entry.metrics.new_value() else {
                continue;
            };
            metrics.push(Metric::new(kind, value.as_f64()));
        }
    }

    BenchmarkResult::new(id, metrics)
}

/// Extracts the package name from a Gungraun `package_dir` path.
///
/// The directory is an absolute path whose final component is the package
/// directory name (for example `/mnt/c/Source/folo/packages/fast_time` ->
/// `fast_time`). Only the final component is used because the full path is
/// machine-specific (it differs between, say, a WSL guest and a CI runner) and
/// would break comparability across machines. Both `/` and `\` are treated as
/// separators so Windows-style paths resolve correctly, and trailing separators
/// are ignored. Returns `None` when no non-empty component remains.
fn package_name_from_dir(package_dir: &str) -> Option<String> {
    package_dir
        .rsplit(['/', '\\'])
        .find(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
}

/// Maps a Callgrind event-kind name to the metric category the tool tracks.
///
/// Only the events that stay stable across builds are tracked: retired
/// instructions (`Ir`) and executed branch counts (`Bc`, `Bi`). Everything else
/// Gungraun emits is skipped so it is never persisted — the derived rates, raw
/// cache reads/writes, and totals, but also, deliberately, the cache-simulation and
/// branch-misprediction events (`L1hits`/`LLhits`/`RamHits`, `EstimatedCycles`,
/// `Bcm`, `Bim`). Those last are not merely unclassified: at microbenchmark
/// magnitudes their values track binary layout (embedded build paths, section
/// sizes, code/data placement) rather than the code under test, so they cannot be
/// compared build to build across a history. See the crate design notes for the
/// evidence.
fn classify(event_kind: &str) -> Option<MetricKind> {
    match event_kind {
        "Ir" => Some(MetricKind::InstructionCount),
        "Bc" => Some(MetricKind::ConditionalBranches),
        "Bi" => Some(MetricKind::IndirectBranches),
        _ => None,
    }
}

/// The subset of the Gungraun `BenchmarkSummary` the tool reads.
#[derive(Debug, Deserialize)]
struct Summary {
    version: String,
    module_path: String,
    function_name: String,
    id: Option<String>,
    #[serde(default)]
    package_dir: Option<String>,
    profiles: Vec<Profile>,
}

#[derive(Debug, Deserialize)]
struct Profile {
    summaries: ProfileData,
}

#[derive(Debug, Deserialize)]
struct ProfileData {
    total: ProfileTotal,
}

#[derive(Debug, Deserialize)]
struct ProfileTotal {
    summary: ToolSummaries,
}

/// The per-tool metric summaries Gungraun externally tags by tool name. Only the
/// Callgrind tool is read; any other tool's summary is ignored as an absent field.
#[derive(Debug, Deserialize)]
struct ToolSummaries {
    #[serde(rename = "Callgrind", default)]
    callgrind: Option<BTreeMap<String, MetricEntry>>,
}

#[derive(Debug, Deserialize)]
struct MetricEntry {
    metrics: MetricPair,
}

/// One metric as Gungraun serializes its `EitherOrBoth<MetricValue>`: the current
/// run's value under `Left`, a `[new, old]` pair under `Both`, or only a baseline
/// value under `Right` (modelled as both fields absent).
#[derive(Debug, Deserialize)]
struct MetricPair {
    #[serde(rename = "Left", default)]
    left: Option<MetricValue>,
    #[serde(rename = "Both", default)]
    both: Option<Vec<MetricValue>>,
}

impl MetricPair {
    /// The current-run value, if this metric has one (a `Right`-only metric — a
    /// value present only in a baseline — has none and is skipped).
    fn new_value(&self) -> Option<MetricValue> {
        if let Some(value) = self.left {
            return Some(value);
        }
        self.both.as_ref()?.first().copied()
    }
}

/// A single metric value, either an integer count or a floating-point rate.
#[derive(Clone, Copy, Debug, Deserialize)]
enum MetricValue {
    Int(u64),
    Float(f64),
}

impl MetricValue {
    /// The value as `f64`, the model's storage type.
    #[expect(
        clippy::cast_precision_loss,
        reason = "instruction counts well below 2^53; precision loss is irrelevant"
    )]
    fn as_f64(self) -> f64 {
        match self {
            Self::Int(value) => value as f64,
            Self::Float(value) => value,
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(
        clippy::float_cmp,
        reason = "metric values are exact integer-derived counts"
    )]

    use std::error;
    use std::fmt::Debug;
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use nonempty::nonempty;
    use ohno::ErrorExt;
    use static_assertions::assert_impl_all;

    use super::*;

    assert_impl_all!(
        CallgrindParseError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        CallgrindJsonError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );
    assert_impl_all!(
        UnsupportedCallgrindVersionError: Send,
        Sync,
        Debug,
        error::Error,
        UnwindSafe,
        RefUnwindSafe
    );

    use crate::testing::{
        CALLGRIND_PARAMETRIZED as PARAMETRIZED_FIXTURE,
        CALLGRIND_SINGLE_UNPARAMETRIZED as SINGLE_FIXTURE,
    };

    fn metric(record: &BenchmarkResult, kind: MetricKind) -> &Metric {
        record
            .metrics
            .iter()
            .find(|metric| metric.kind == kind)
            .unwrap_or_else(|| panic!("metric {kind:?} should be present"))
    }

    #[test]
    fn parses_unparametrized_identity() {
        let record = parse_callgrind_summary(SINGLE_FIXTURE).unwrap();
        assert_eq!(
            record.id,
            BenchmarkId::new(nonempty![
                "fast_time".to_owned(),
                "fast_time_timestamp_performance_cg::timestamp_capture::timestamp_capture_std_now"
                    .to_owned(),
                "timestamp_capture_std_now".to_owned(),
            ])
        );
    }

    #[test]
    fn parses_package_from_package_dir() {
        let record = parse_callgrind_summary(SINGLE_FIXTURE).unwrap();
        assert_eq!(record.id.segments.first().as_str(), "fast_time");
    }

    #[test]
    fn parses_parametrized_identity_with_value() {
        let record = parse_callgrind_summary(PARAMETRIZED_FIXTURE).unwrap();
        // The value is the final segment; the function name precedes it.
        assert_eq!(record.id.segments.last().as_str(), "two_instants");
        assert!(
            record
                .id
                .segments
                .contains(&"timestamp_capture_instant_saturating_duration_since".to_owned()),
            "{:?}",
            record.id.segments
        );
    }

    #[test]
    fn maps_the_tracked_metric_kinds() {
        let record = parse_callgrind_summary(SINGLE_FIXTURE).unwrap();

        assert_eq!(metric(&record, MetricKind::InstructionCount).value, 36.0);
        // Executed conditional and indirect branch counts each map to a distinct kind.
        assert_eq!(metric(&record, MetricKind::ConditionalBranches).value, 4.0);
        assert_eq!(metric(&record, MetricKind::IndirectBranches).value, 2.0);
    }

    #[test]
    fn skips_untracked_events() {
        let record = parse_callgrind_summary(SINGLE_FIXTURE).unwrap();
        // Only the three tracked events survive. The fixture still carries the
        // cache-simulation and branch-misprediction events (`EstimatedCycles`,
        // `L1hits`, `LLhits`, `RamHits`, `Bcm`, `Bim`), derived rates (`L1HitRate`),
        // raw cache reads (`Dr`), and totals (`TotalRW`) — all dropped.
        assert_eq!(record.metrics.len(), 3, "{:?}", record.metrics);
    }

    #[test]
    fn tracks_exactly_the_canonical_event_set() {
        let record = parse_callgrind_summary(SINGLE_FIXTURE).unwrap();
        let mut kinds: Vec<MetricKind> = record.metrics.iter().map(|m| m.kind).collect();
        kinds.sort_unstable();

        let mut expected = vec![
            MetricKind::InstructionCount,
            MetricKind::ConditionalBranches,
            MetricKind::IndirectBranches,
        ];
        expected.sort_unstable();
        assert_eq!(kinds, expected);
    }

    #[test]
    fn rejects_unsupported_version() {
        // Version validation needs a well-formed summary, not the producer's
        // unrelated profiling metadata.
        let altered = r#"{"version":"7","module_path":"m","function_name":"f","profiles":[]}"#;
        let error = parse_callgrind_summary(altered).unwrap_err();
        let unsupported = error
            .find_source::<UnsupportedCallgrindVersionError>()
            .unwrap();
        assert_eq!(unsupported.version, "7");
        assert_eq!(unsupported.supported_version, SUPPORTED_VERSION);
        assert!(error.find_source::<CallgrindJsonError>().is_none());
    }

    #[test]
    fn rejects_malformed_json() {
        let error = parse_callgrind_summary("{ not json").unwrap_err();
        assert!(error.find_source::<CallgrindJsonError>().is_some());
        assert!(
            error
                .find_source::<UnsupportedCallgrindVersionError>()
                .is_none()
        );
    }

    #[test]
    fn parse_conditions_carry_their_sources_and_fields() {
        let json_error = parse_callgrind_summary("{ not json").unwrap_err();
        assert!(json_error.find_source::<CallgrindJsonError>().is_some());
        assert!(json_error.find_source::<serde_json::Error>().is_some());

        let version_error = UnsupportedCallgrindVersionError::new("9", SUPPORTED_VERSION);
        assert_eq!(version_error.version, "9");
        assert_eq!(version_error.supported_version, SUPPORTED_VERSION);
        assert!(error::Error::source(&version_error).is_none());
    }

    #[test]
    fn callgrind_parse_error_displays_as_the_underlying_failure() {
        let version_error = UnsupportedCallgrindVersionError::new("9", SUPPORTED_VERSION);
        let expected = version_error.to_string();

        let error = CallgrindParseError::from(version_error);

        // The wrapper adds no wording of its own, so a caller that only prints the
        // error still sees the specific failure. Matching on the prefix tolerates
        // the optional trailing backtrace block.
        assert!(error.to_string().starts_with(&expected));
    }

    fn summary_json(callgrind_body: &str) -> String {
        format!(
            "{{\"version\":\"6\",\"module_path\":\"m\",\"function_name\":\"f\",\
             \"profiles\":[{{\"summaries\":{{\"total\":{{\"summary\":{callgrind_body}}}}}}}]}}"
        )
    }

    fn summary_with_package_dir(package_dir: &str) -> String {
        format!(
            "{{\"version\":\"6\",\"module_path\":\"a::bench\",\"function_name\":\"f\",\
             \"package_dir\":\"{package_dir}\",\
             \"profiles\":[{{\"summaries\":{{\"total\":{{\"summary\":{{}}}}}}}}]}}"
        )
    }

    #[test]
    fn package_name_from_dir_extracts_final_component() {
        assert_eq!(
            package_name_from_dir("/mnt/c/Source/folo/packages/fast_time"),
            Some("fast_time".to_owned())
        );
        assert_eq!(package_name_from_dir("/a/b/pkg/"), Some("pkg".to_owned()));
        assert_eq!(package_name_from_dir(r"C:\x\pkg"), Some("pkg".to_owned()));
        assert_eq!(package_name_from_dir("/a\\b\\pkg"), Some("pkg".to_owned()));
        assert_eq!(package_name_from_dir("pkg"), Some("pkg".to_owned()));
        assert_eq!(package_name_from_dir(""), None);
        assert_eq!(package_name_from_dir("/"), None);
    }

    #[test]
    fn summary_without_package_dir_has_no_package() {
        let record = parse_callgrind_summary(&summary_json("{}")).unwrap();
        assert_eq!(
            record.id,
            BenchmarkId::new(nonempty!["m".to_owned(), "f".to_owned()])
        );
    }

    #[test]
    fn same_module_path_in_different_packages_yields_distinct_ids() {
        let foo = parse_callgrind_summary(&summary_with_package_dir("/work/packages/foo")).unwrap();
        let bar = parse_callgrind_summary(&summary_with_package_dir("/work/packages/bar")).unwrap();

        // The module-path and function segments match; only the leading package
        // segment differs, so the identities stay distinct.
        assert_eq!(foo.id.segments.get(1), bar.id.segments.get(1));
        assert_eq!(foo.id.segments.get(2), bar.id.segments.get(2));
        assert_ne!(foo.id, bar.id);
        assert_eq!(foo.id.segments.first().as_str(), "foo");
        assert_eq!(bar.id.segments.first().as_str(), "bar");
    }

    #[test]
    fn skips_profile_without_callgrind_summary() {
        let record = parse_callgrind_summary(&summary_json("{}")).unwrap();
        assert!(record.metrics.is_empty());
    }

    #[test]
    fn skips_metric_present_only_in_baseline() {
        let body = "{\"Callgrind\":{\"Ir\":{\"metrics\":{}}}}";
        let record = parse_callgrind_summary(&summary_json(body)).unwrap();
        assert!(record.metrics.is_empty());
    }

    #[test]
    fn reads_new_value_from_both_pair() {
        let body = "{\"Callgrind\":{\"Ir\":{\"metrics\":{\"Both\":[{\"Int\":10},{\"Int\":9}]}}}}";
        let record = parse_callgrind_summary(&summary_json(body)).unwrap();
        assert_eq!(metric(&record, MetricKind::InstructionCount).value, 10.0);
    }

    #[test]
    fn reads_float_metric_value() {
        let body = "{\"Callgrind\":{\"Ir\":{\"metrics\":{\"Left\":{\"Float\":1.5}}}}}";
        let record = parse_callgrind_summary(&summary_json(body)).unwrap();
        assert_eq!(metric(&record, MetricKind::InstructionCount).value, 1.5);
    }
}
