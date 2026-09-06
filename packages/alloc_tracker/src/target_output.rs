//! Machine-readable JSON output of memory allocation statistics.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{Report, ReportOperation};

/// Subdirectory of the Cargo target directory that receives the JSON files.
const OUTPUT_SUBDIRECTORY: &str = "alloc_tracker";

/// Machine-readable allocation statistics for a single operation.
///
/// Carries the per-iteration slope with its confidence interval for each metric,
/// mirroring the shape `all_the_time` writes for processor time. Interval fields
/// are omitted when the interval cannot be estimated.
#[derive(Serialize)]
struct OperationOutput<'a> {
    operation: &'a str,
    total_iterations: u64,
    total_bytes_allocated: u64,
    total_allocations_count: u64,
    span_count: u64,
    slope_bytes_per_iteration: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    interval_low_bytes_per_iteration: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    interval_high_bytes_per_iteration: Option<f64>,
    slope_allocations_per_iteration: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    interval_low_allocations_per_iteration: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    interval_high_allocations_per_iteration: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slope_peak_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    interval_low_peak_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    interval_high_peak_bytes: Option<f64>,
}

impl Report {
    /// Writes machine-readable JSON statistics into the Cargo target directory.
    ///
    /// One file is written per operation, named after the operation, at
    /// `<target>/alloc_tracker/<operation>.json`. Operation names are sanitized
    /// to be filesystem-safe and existing files are overwritten.
    ///
    /// The target directory is resolved the same way as Criterion (honoring
    /// `CARGO_TARGET_DIR`), falling back to a relative `target` directory.
    ///
    /// Writes nothing if no operations were captured. This may indicate that the
    /// session was part of a "list available benchmarks" probe run instead of
    /// some real activity.
    ///
    /// # Panics
    ///
    /// Panics if the output directory cannot be created or a file cannot be
    /// written. Benchmark results are not useful without the output files they
    /// produce, so a write failure is treated as fatal rather than recoverable.
    ///
    /// Also panics if two operation names sanitize to the same file name, since
    /// writing both would silently discard one operation's results.
    pub(crate) fn write_to_target(&self) {
        let target =
            folo_utils::cargo_target_directory().unwrap_or_else(|| PathBuf::from("target"));
        self.write_to_directory(target.join(OUTPUT_SUBDIRECTORY));
    }

    /// Writes machine-readable JSON statistics into the given directory.
    ///
    /// One file is written per operation, named after the operation, as
    /// `<directory>/<operation>.json`. Operation names are sanitized to be
    /// filesystem-safe and existing files are overwritten. The directory is
    /// created if it does not exist.
    ///
    /// Each file carries the totals, the span count, and a per-iteration slope for every
    /// metric, each slope optionally accompanied by the low and high bounds of its
    /// confidence interval:
    ///
    /// | Key | Meaning |
    /// |-----|---------|
    /// | `operation` | The operation's name |
    /// | `total_iterations` | Iterations covered by every span |
    /// | `total_bytes_allocated` | Bytes allocated across every span |
    /// | `total_allocations_count` | Allocations across every span |
    /// | `span_count` | How many spans the estimates were fitted from |
    /// | `slope_bytes_per_iteration` | Bytes allocated per iteration |
    /// | `slope_allocations_per_iteration` | Allocations per iteration |
    /// | `slope_peak_bytes` | The peak outstanding bytes a single iteration holds |
    /// | `interval_low_*`, `interval_high_*` | Bounds for the matching slope |
    ///
    /// The peak keys carry no `per_iteration` suffix because the peak is a level a single
    /// iteration reaches rather than a quantity accumulated across iterations.
    ///
    /// The two omission rules are independent. An interval pair is absent whenever that
    /// metric's dispersion cannot be estimated, which a single span never supplies. All
    /// three peak keys are absent together when the operation has no peak to report at
    /// all; see [`ReportOperation::peak_outstanding_bytes`] for when that happens.
    ///
    /// Writes nothing if no operations were captured.
    ///
    /// # Panics
    ///
    /// Panics if the output directory cannot be created or a file cannot be
    /// written. Benchmark results are not useful without the output files they
    /// produce, so a write failure is treated as fatal rather than recoverable.
    ///
    /// Also panics if two operation names sanitize to the same file name, since
    /// writing both would silently discard one operation's results.
    pub fn write_to_directory(&self, directory: impl AsRef<Path>) {
        let directory = directory.as_ref();

        // Resolve every destination up front, detecting sanitized-name collisions before
        // touching the filesystem. Two operation names that sanitize to the same file name
        // would otherwise silently overwrite each other's results. Only the names are
        // needed for that, so the bodies are serialized later, one at a time.
        let mut file_names: HashMap<String, &str> = HashMap::new();
        let mut destinations: Vec<(PathBuf, &str, &ReportOperation)> = Vec::new();
        for (name, operation) in self.sorted_operations() {
            if operation.statistics().is_none() {
                // Registered but never measured operations have no spans and thus
                // no statistics, so they leave no output file behind.
                continue;
            }

            let file_name = format!("{}.json", folo_utils::sanitize_file_name(name));
            if let Some(previous) = file_names.insert(file_name.clone(), name) {
                panic!(
                    "operations {previous:?} and {name:?} both map to the output file name \
                     {file_name:?} after sanitization; rename one of them to avoid silently \
                     overwriting benchmark results"
                );
            }

            destinations.push((directory.join(file_name), name, operation));
        }

        // Without any output, no directory is created, so a probe run that captured
        // no measurable work leaves nothing behind.
        if destinations.is_empty() {
            return;
        }

        fs::create_dir_all(directory).unwrap_or_else(|error| {
            panic!(
                "failed to create benchmark output directory {}: {error}",
                directory.display()
            )
        });

        // One buffer serves every file: it reaches the size of the largest body once
        // instead of each operation leaving a separate allocation behind.
        let mut buffer: Vec<u8> = Vec::new();

        for (path, name, operation) in destinations {
            let statistics = operation
                .statistics()
                .expect("operations without statistics were filtered out above");

            let output = OperationOutput {
                operation: name,
                total_iterations: operation.total_iterations(),
                total_bytes_allocated: operation.total_bytes_allocated(),
                total_allocations_count: operation.total_allocations_count(),
                span_count: statistics.span_count,
                slope_bytes_per_iteration: statistics.bytes.slope,
                interval_low_bytes_per_iteration: statistics.bytes.interval.map(|(low, _)| low),
                interval_high_bytes_per_iteration: statistics.bytes.interval.map(|(_, high)| high),
                slope_allocations_per_iteration: statistics.allocations.slope,
                interval_low_allocations_per_iteration: statistics
                    .allocations
                    .interval
                    .map(|(low, _)| low),
                interval_high_allocations_per_iteration: statistics
                    .allocations
                    .interval
                    .map(|(_, high)| high),
                slope_peak_bytes: statistics.peak_outstanding_bytes.map(|peak| peak.slope),
                interval_low_peak_bytes: statistics
                    .peak_outstanding_bytes
                    .and_then(|peak| peak.interval)
                    .map(|(low, _)| low),
                interval_high_peak_bytes: statistics
                    .peak_outstanding_bytes
                    .and_then(|peak| peak.interval)
                    .map(|(_, high)| high),
            };

            buffer.clear();
            serde_json::to_writer_pretty(&mut buffer, &output)
                .expect("serializing fixed primitive fields to JSON cannot fail");

            fs::write(&path, &buffer).unwrap_or_else(|error| {
                panic!(
                    "failed to write benchmark output file {}: {error}",
                    path.display()
                )
            });
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::fs;
    use std::path::Path;

    use serde_json::Value;
    use tempfile::tempdir;

    use crate::Session;
    use crate::counters::register_fake_allocation;

    fn read_json(path: &Path) -> Value {
        serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
    }

    fn session_with_recorded_work(name: &str) -> Session {
        let session = Session::new().no_stdout().no_file();
        {
            let operation = session.operation(name);
            let _span = operation.measure_thread().iterations(4);
            register_fake_allocation(800, 8);
        }
        session
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Writes files, which is not supported under Miri isolation.
    fn writes_operation_statistics_as_json() {
        let session = session_with_recorded_work("allocate_vec");
        let directory = tempdir().unwrap();

        session.to_report().write_to_directory(directory.path());

        let file = directory.path().join("allocate_vec.json");
        let value = read_json(&file);

        assert_eq!(
            value.get("operation").and_then(Value::as_str),
            Some("allocate_vec")
        );
        assert_eq!(
            value.get("total_iterations").and_then(Value::as_u64),
            Some(4)
        );
        assert_eq!(
            value.get("total_bytes_allocated").and_then(Value::as_u64),
            Some(800)
        );
        assert_eq!(
            value.get("total_allocations_count").and_then(Value::as_u64),
            Some(8)
        );
        // A single recorded span yields a span count of one and per-metric slopes
        // equal to the per-iteration means, but no interval (a single span carries
        // no dispersion information), so the interval fields are omitted.
        assert_eq!(value.get("span_count").and_then(Value::as_u64), Some(1));
        assert_eq!(
            value
                .get("slope_bytes_per_iteration")
                .and_then(Value::as_f64),
            Some(200.0)
        );
        assert!(value.get("interval_low_bytes_per_iteration").is_none());
        assert!(value.get("interval_high_bytes_per_iteration").is_none());
        // The raw means, standard deviation, minimum and maximum are not emitted.
        assert!(value.get("mean_bytes_per_iteration").is_none());
        assert!(value.get("mean_allocations_per_iteration").is_none());
        assert!(value.get("std_dev_bytes_per_iteration").is_none());
        assert!(value.get("min_bytes_per_iteration").is_none());
        assert!(value.get("max_bytes_per_iteration").is_none());
        assert_eq!(
            value
                .get("slope_allocations_per_iteration")
                .and_then(Value::as_f64),
            Some(2.0)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Writes files, which is not supported under Miri isolation.
    fn writes_peak_bytes() {
        let session = session_with_recorded_work("allocate_vec");
        let directory = tempdir().unwrap();

        session.to_report().write_to_directory(directory.path());

        let value = read_json(&directory.path().join("allocate_vec.json"));
        // The fake allocation is never released, so the whole of it is outstanding at the
        // span's high-water mark. A single span pins the estimate on its own peak, and
        // carries no dispersion, so no interval is formed.
        assert_eq!(
            value.get("slope_peak_bytes").and_then(Value::as_f64),
            Some(800.0)
        );
        // One span supplies no dispersion, so both bounds are withheld while the point
        // estimate remains. This is the omission rule that differs from an absent peak.
        assert!(value.get("interval_low_peak_bytes").is_none());
        assert!(value.get("interval_high_peak_bytes").is_none());
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Writes files, which is not supported under Miri isolation.
    fn omits_peak_bytes_when_unavailable() {
        let session = Session::new().no_stdout().no_file();
        {
            let operation = session.operation("allocate_vec");
            // Process spans cannot measure a peak, so the field must be absent
            // rather than present with a misleading value.
            let _span = operation.measure_process().iterations(4);
            register_fake_allocation(800, 8);
        }
        let directory = tempdir().unwrap();

        session.to_report().write_to_directory(directory.path());

        let value = read_json(&directory.path().join("allocate_vec.json"));
        // No peak at all means all three keys go, not just the point estimate.
        assert!(value.get("slope_peak_bytes").is_none());
        assert!(value.get("interval_low_peak_bytes").is_none());
        assert!(value.get("interval_high_peak_bytes").is_none());
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Writes files, which is not supported under Miri isolation.
    fn writes_interval_when_multiple_spans_recorded() {
        // Two identical spans clear the two-span threshold with zero residual
        // dispersion, so the interval collapses onto the slope and is written out.
        let session = Session::new().no_stdout().no_file();
        for _ in 0..2 {
            let operation = session.operation("allocate_vec");
            let _span = operation.measure_thread().iterations(4);
            register_fake_allocation(800, 8);
        }
        let directory = tempdir().unwrap();

        session.to_report().write_to_directory(directory.path());

        let value = read_json(&directory.path().join("allocate_vec.json"));
        assert_eq!(value.get("span_count").and_then(Value::as_u64), Some(2));
        assert_eq!(
            value
                .get("interval_low_bytes_per_iteration")
                .and_then(Value::as_f64),
            Some(200.0)
        );
        assert_eq!(
            value
                .get("interval_high_bytes_per_iteration")
                .and_then(Value::as_f64),
            Some(200.0)
        );
        // The peak rides the same estimator, so once there is dispersion evidence its
        // point estimate and both bounds are all emitted.
        assert_eq!(
            value.get("slope_peak_bytes").and_then(Value::as_f64),
            Some(800.0)
        );
        assert_eq!(
            value.get("interval_low_peak_bytes").and_then(Value::as_f64),
            Some(800.0)
        );
        assert_eq!(
            value
                .get("interval_high_peak_bytes")
                .and_then(Value::as_f64),
            Some(800.0)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Writes files, which is not supported under Miri isolation.
    fn writes_null_slopes_for_zero_iteration_operation() {
        let session = Session::new().no_stdout().no_file();
        {
            let operation = session.operation("failed");
            // The workload could not run, so it records zero iterations.
            let _span = operation.measure_thread().iterations(0);
            register_fake_allocation(800, 8);
        }
        let directory = tempdir().unwrap();

        session.to_report().write_to_directory(directory.path());

        let value = read_json(&directory.path().join("failed.json"));
        // A zero-iteration measurement has no per-iteration rate. The bytes and allocations
        // slopes are always present, so theirs are NaN, which serde_json renders as JSON
        // null. The peak instead expresses unavailability by being absent altogether.
        assert!(
            value
                .get("slope_bytes_per_iteration")
                .expect("the bytes slope field is always present")
                .is_null()
        );
        assert!(
            value
                .get("slope_allocations_per_iteration")
                .expect("the allocations slope field is always present")
                .is_null()
        );
        assert!(value.get("slope_peak_bytes").is_none());
        assert_eq!(
            value.get("total_iterations").and_then(Value::as_u64),
            Some(0)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Writes files, which is not supported under Miri isolation.
    fn sanitizes_operation_name_in_file_name() {
        let session = session_with_recorded_work("group/case name");
        let directory = tempdir().unwrap();

        session.to_report().write_to_directory(directory.path());

        let file = directory.path().join("group_case_name.json");
        assert!(file.exists());

        // The original, unsanitized name is preserved inside the file.
        assert_eq!(
            read_json(&file).get("operation").and_then(Value::as_str),
            Some("group/case name")
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Writes files, which is not supported under Miri isolation.
    fn empty_session_writes_no_files() {
        let session = Session::new().no_stdout().no_file();
        let directory = tempdir().unwrap();
        let target = directory.path().join("nested");

        session.to_report().write_to_directory(&target);

        // Nothing is written, so the directory is not even created.
        assert!(!target.exists());
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Writes files, which is not supported under Miri isolation.
    fn skips_operations_without_iterations() {
        let session = Session::new().no_stdout().no_file();
        {
            let operation = session.operation("measured");
            let _span = operation.measure_thread().iterations(4);
            register_fake_allocation(800, 8);
        }
        // Registered but never measured, so it stays at zero iterations and must
        // be skipped rather than written.
        let _unmeasured = session.operation("unmeasured");

        let directory = tempdir().unwrap();
        session.to_report().write_to_directory(directory.path());

        assert!(directory.path().join("measured.json").exists());
        assert!(!directory.path().join("unmeasured.json").exists());
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Writes files, which is not supported under Miri isolation.
    fn overwrites_existing_files() {
        let directory = tempdir().unwrap();
        let file = directory.path().join("allocate_vec.json");
        fs::write(&file, "stale contents").unwrap();

        let session = session_with_recorded_work("allocate_vec");
        session.to_report().write_to_directory(directory.path());

        // Parsing succeeds only if the stale, non-JSON contents were replaced.
        let value = read_json(&file);
        assert_eq!(
            value.get("operation").and_then(Value::as_str),
            Some("allocate_vec")
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Writes files, which is not supported under Miri isolation.
    #[should_panic(expected = "failed to create benchmark output directory")]
    fn panics_when_output_directory_cannot_be_created() {
        let session = session_with_recorded_work("allocate_vec");
        let directory = tempdir().unwrap();

        // A regular file where a directory component is expected makes the
        // recursive directory creation fail.
        let blocker = directory.path().join("blocker");
        fs::write(&blocker, "not a directory").unwrap();

        session
            .to_report()
            .write_to_directory(blocker.join("nested"));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Writes files, which is not supported under Miri isolation.
    #[should_panic(expected = "failed to write benchmark output file")]
    fn panics_when_output_file_cannot_be_written() {
        let session = session_with_recorded_work("allocate_vec");
        let directory = tempdir().unwrap();

        // A directory occupying the output file's path makes the file write fail.
        fs::create_dir_all(directory.path().join("allocate_vec.json")).unwrap();

        session.to_report().write_to_directory(directory.path());
    }

    #[test]
    #[should_panic(expected = "after sanitization")]
    fn panics_when_operation_names_collide_after_sanitization() {
        let session = Session::new().no_stdout().no_file();

        // Both names sanitize to `group_case.json`, so writing both would silently
        // discard one operation's results.
        for name in ["group/case", "group_case"] {
            let operation = session.operation(name);
            let _span = operation.measure_thread().iterations(4);
            register_fake_allocation(800, 8);
        }

        // The collision is detected before anything is written, so this path is
        // never created.
        session
            .to_report()
            .write_to_directory("collision_is_detected_before_writing");
    }
}
