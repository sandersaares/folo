//! Memory allocation tracking reports.

use std::collections::HashMap;
use std::{fmt, iter};

use crate::OperationMetrics;

/// Thread-safe memory allocation tracking report.
///
/// A `Report` contains the captured memory allocation statistics from a [`Session`](crate::Session)
/// and can be safely sent to other threads for processing. Reports can be merged together
/// and processed independently.
///
/// # Examples
///
/// ```
/// use alloc_tracker::{Allocator, Session};
///
/// #[global_allocator]
/// static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
///
/// # fn main() {
/// let session = Session::new();
/// # let session = session.no_stdout().no_file();
/// {
///     let operation = session.operation("test_work");
///     let _span = operation.measure_thread().iterations(1);
///     let _data = vec![1, 2, 3, 4, 5]; // This allocates memory
/// }
///
/// let report = session.to_report();
///
/// // A report exposes each operation's statistics for programmatic use.
/// let total_bytes: u64 = report
///     .operations()
///     .map(|(_, op)| op.total_bytes_allocated())
///     .sum();
/// println!("Captured {total_bytes} bytes across all operations");
/// # }
/// ```
///
/// # Merging reports
///
/// ```
/// use alloc_tracker::{Allocator, Report, Session};
///
/// #[global_allocator]
/// static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
///
/// # fn main() {
/// // Create two separate sessions
/// let session1 = Session::new();
/// # let session1 = session1.no_stdout().no_file();
/// let session2 = Session::new();
/// # let session2 = session2.no_stdout().no_file();
///
/// // Record some work in each
/// {
///     let op1 = session1.operation("work");
///     let _span1 = op1.measure_thread().iterations(1);
///     let _data1 = vec![1, 2, 3]; // This allocates memory
/// }
///
/// {
///     let op2 = session2.operation("work");
///     let _span2 = op2.measure_thread().iterations(1);
///     let _data2 = vec![4, 5, 6, 7]; // This allocates more memory
/// }
///
/// // Convert to reports and merge
/// let report1 = session1.to_report();
/// let report2 = session2.to_report();
/// let merged = Report::merge(&report1, &report2);
///
/// // The merged report exposes the combined statistics for programmatic use.
/// let total_bytes: u64 = merged
///     .operations()
///     .map(|(_, op)| op.total_bytes_allocated())
///     .sum();
/// println!("Merged report captured {total_bytes} bytes across all operations");
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct Report {
    operations: HashMap<String, ReportOperation>,
}

/// Memory allocation statistics for a single operation in a report.
#[derive(Clone, Debug)]
pub struct ReportOperation {
    metrics: OperationMetrics,
}

/// Per-iteration statistics for a single allocation metric.
///
/// Every value is expressed in the metric's own per-iteration unit (bytes, or a
/// count of allocations). [`slope`](Self::slope) is the per-iteration value and
/// [`interval`](Self::interval) its 95% confidence bounds, or `None` when there
/// is not enough data to estimate them.
///
/// When the operation's spans covered zero iterations there is no per-iteration
/// rate: [`slope`](Self::slope) is then `NaN` and [`interval`](Self::interval)
/// is `None`.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct MetricStatistics {
    /// The per-iteration value, or `NaN` when the spans covered zero iterations.
    pub slope: f64,

    /// Confidence interval `(low, high)` for [`slope`](Self::slope), or `None`
    /// when it cannot be estimated.
    pub interval: Option<(f64, f64)>,
}

/// Statistics for one operation across the metrics the report exposes.
///
/// Exposed through [`ReportOperation::statistics`] so callers can consume the
/// same figures that are written to the machine-readable JSON output.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct OperationStatistics {
    /// Number of spans the statistics were derived from (distinct from the total
    /// iteration count).
    pub span_count: u64,

    /// Per-iteration byte-count statistics.
    pub bytes: MetricStatistics,

    /// Per-iteration allocation-count statistics.
    pub allocations: MetricStatistics,

    /// Per-iteration peak-outstanding-byte statistics, or `None` when the operation has no
    /// peak to report.
    ///
    /// See [`ReportOperation::peak_outstanding_bytes`] for when that is the case.
    pub peak_outstanding_bytes: Option<MetricStatistics>,
}

impl Report {
    /// Creates an empty report.
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding, not shipped behavior.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            operations: HashMap::new(),
        }
    }

    /// Creates a report from shared operation data.
    #[must_use]
    pub(crate) fn from_operation_data(operation_data: &HashMap<String, OperationMetrics>) -> Self {
        let report_operations = operation_data
            .iter()
            .map(|(name, metrics)| {
                (
                    name.clone(),
                    ReportOperation {
                        metrics: metrics.clone(),
                    },
                )
            })
            .collect();

        Self {
            operations: report_operations,
        }
    }

    /// Merges two reports into a new report.
    ///
    /// The resulting report contains the combined statistics from both input reports.
    /// Operations with the same name have their spans concatenated as if all spans
    /// had been recorded through a single session.
    ///
    /// # Examples
    ///
    /// ```
    /// use alloc_tracker::{Allocator, Report, Session};
    ///
    /// #[global_allocator]
    /// static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
    ///
    /// # fn main() {
    /// let session1 = Session::new();
    /// # let session1 = session1.no_stdout().no_file();
    /// let session2 = Session::new();
    /// # let session2 = session2.no_stdout().no_file();
    ///
    /// // Both sessions record the same operation name
    /// {
    ///     let op1 = session1.operation("common_work");
    ///     let _span1 = op1.measure_thread().iterations(1);
    ///     let _data1 = vec![1, 2, 3]; // 3 elements
    /// }
    ///
    /// {
    ///     let op2 = session2.operation("common_work");
    ///     let _span2 = op2.measure_thread().iterations(1);
    ///     let _data2 = vec![4, 5]; // 2 elements
    /// }
    ///
    /// let report1 = session1.to_report();
    /// let report2 = session2.to_report();
    ///
    /// // Merged report shows combined statistics (2 total iterations)
    /// let merged = Report::merge(&report1, &report2);
    /// # }
    /// ```
    #[must_use]
    pub fn merge(a: &Self, b: &Self) -> Self {
        let mut merged_operations = a.operations.clone();

        for (name, b_op) in &b.operations {
            merged_operations
                .entry(name.clone())
                .and_modify(|a_op| a_op.metrics.merge(&b_op.metrics))
                .or_insert_with(|| b_op.clone());
        }

        Self {
            operations: merged_operations,
        }
    }

    /// Returns the operations sorted by name.
    ///
    /// The report holds operations in an unordered map, so every output sorts
    /// them by name to present a stable, reproducible order.
    pub(crate) fn sorted_operations(&self) -> Vec<(&str, &ReportOperation)> {
        let mut operations: Vec<(&str, &ReportOperation)> = self
            .operations
            .iter()
            .map(|(name, op)| (name.as_str(), op))
            .collect();
        operations.sort_unstable_by_key(|(name, _)| *name);
        operations
    }

    /// Prints the memory allocation statistics to stdout.
    ///
    /// Prints nothing if no operations were captured. This may indicate that the
    /// session was part of a "list available benchmarks" probe run instead of
    /// some real activity, in which case printing anything might violate the
    /// output protocol the tool is speaking.
    // Excluded from coverage as an un-assertable stdout side effect, matching the
    // sibling `Display` impl. The figures it prints are covered independently via
    // `Display` and the JSON output, so nothing computational is hidden here.
    #[cfg_attr(coverage_nightly, coverage(off))]
    #[cfg_attr(test, mutants::skip)] // Too difficult to test stdout output reliably - manually tested.
    pub fn print_to_stdout(&self) {
        if self.is_empty() {
            return;
        }
        println!("{self}");
    }

    /// Whether there is any recorded activity in this report.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty() || self.operations.values().all(|op| op.metrics.is_empty())
    }

    /// Returns an iterator over the operation names and their statistics.
    ///
    /// This allows programmatic access to the same data that would be printed by
    /// [`print_to_stdout()`](Self::print_to_stdout).
    ///
    /// # Examples
    ///
    /// ```
    /// use alloc_tracker::{Allocator, Session};
    ///
    /// #[global_allocator]
    /// static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
    ///
    /// # fn main() {
    /// let session = Session::new();
    /// # let session = session.no_stdout().no_file();
    /// {
    ///     let operation = session.operation("test_work");
    ///     let _span = operation.measure_thread().iterations(1);
    ///     let _data = vec![1, 2, 3, 4, 5]; // This allocates memory
    /// }
    ///
    /// let report = session.to_report();
    /// for (name, op) in report.operations() {
    ///     println!(
    ///         "Operation '{}' had {} iterations",
    ///         name,
    ///         op.total_iterations()
    ///     );
    ///     println!("Bytes per iteration: {:?}", op.bytes());
    ///     println!("Total bytes: {}", op.total_bytes_allocated());
    /// }
    /// # }
    /// ```
    pub fn operations(&self) -> impl Iterator<Item = (&str, &ReportOperation)> {
        self.operations.iter().map(|(name, op)| (name.as_str(), op))
    }
}

impl ReportOperation {
    /// Returns the total bytes allocated across all iterations for this operation.
    #[must_use]
    pub fn total_bytes_allocated(&self) -> u64 {
        self.metrics.total_bytes_allocated()
    }

    /// Returns the total number of allocations across all iterations for this operation.
    #[must_use]
    pub fn total_allocations_count(&self) -> u64 {
        self.metrics.total_allocations_count()
    }

    /// Returns the total number of iterations recorded for this operation.
    #[must_use]
    pub fn total_iterations(&self) -> u64 {
        self.metrics.total_iterations()
    }

    /// Returns the per-iteration peak outstanding bytes for this operation.
    ///
    /// This is the most bytes the operation held allocated at any one moment during an
    /// iteration.
    ///
    /// The figure assumes every iteration in a measured batch reaches the same peak, which
    /// lets spans covering different iteration counts be combined and lets low-iteration
    /// warmup spans be down-weighted. An operation that instead accumulates memory across
    /// the iterations of a batch has no batch-size-independent peak, and reports a figure
    /// that grows with the iteration counts the benchmark harness chose.
    ///
    /// The figure counts memory requested through the allocator as seen at the boundaries
    /// of allocator calls, and is measured relative to what was already outstanding when
    /// each span started. Memory allocated before a span and freed inside it therefore does
    /// not count against that span, and a span that frees more than it allocates before
    /// allocating again under-reports what it holds.
    ///
    /// Returns `None` when no finite figure is available — when no spans were recorded, when
    /// the recorded spans covered zero iterations, or when any recorded span was created by
    /// [`Operation::measure_process`](crate::Operation::measure_process), which has no
    /// single thread's watermark to read.
    #[must_use]
    pub fn peak_outstanding_bytes(&self) -> Option<f64> {
        self.metrics
            .peak_outstanding_bytes()
            .filter(|peak| peak.is_finite())
    }

    /// Returns the per-iteration bytes allocated — the primary allocation metric
    /// for this operation.
    ///
    /// Returns `None` when no finite per-iteration rate is available — for example
    /// when no spans were recorded, or the recorded spans covered zero iterations
    /// (leaving the rate undefined).
    #[must_use]
    pub fn bytes(&self) -> Option<f64> {
        self.metrics.bytes_slope().filter(|slope| slope.is_finite())
    }

    /// Returns the per-iteration allocation count for this operation.
    ///
    /// Returns `None` when no finite per-iteration rate is available — for example
    /// when no spans were recorded, or the recorded spans covered zero iterations
    /// (leaving the rate undefined).
    #[must_use]
    pub fn allocations(&self) -> Option<f64> {
        self.metrics
            .allocations_slope()
            .filter(|slope| slope.is_finite())
    }

    /// Computes per-iteration statistics over the recorded spans.
    ///
    /// Returns `None` when no spans were recorded. The returned
    /// [`OperationStatistics`] carries the per-iteration value and its confidence
    /// interval for each metric — the same figures that are written to the
    /// machine-readable JSON output.
    ///
    /// # Examples
    ///
    /// ```
    /// use alloc_tracker::{Allocator, Session};
    ///
    /// #[global_allocator]
    /// static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
    ///
    /// # fn main() {
    /// let session = Session::new();
    /// # let session = session.no_stdout().no_file();
    /// {
    ///     let operation = session.operation("test_work");
    ///     let _span = operation.measure_thread().iterations(1);
    ///     let _data = vec![1, 2, 3, 4, 5]; // This allocates memory
    /// }
    ///
    /// let report = session.to_report();
    /// for (_name, op) in report.operations() {
    ///     if let Some(stats) = op.statistics() {
    ///         println!(
    ///             "slope: {} bytes/iter over {} spans",
    ///             stats.bytes.slope, stats.span_count
    ///         );
    ///     }
    /// }
    /// # }
    /// ```
    #[must_use]
    pub fn statistics(&self) -> Option<OperationStatistics> {
        if self.metrics.span_count() == 0 {
            return None;
        }
        Some(OperationStatistics {
            span_count: self.metrics.span_count(),
            bytes: MetricStatistics {
                slope: self.metrics.bytes_slope()?,
                interval: self.metrics.bytes_interval(),
            },
            allocations: MetricStatistics {
                slope: self.metrics.allocations_slope()?,
                interval: self.metrics.allocations_interval(),
            },
            peak_outstanding_bytes: self.peak_outstanding_bytes().map(|slope| MetricStatistics {
                slope,
                interval: self.metrics.peak_interval(),
            }),
        })
    }
}

/// Formats a per-iteration count for human-readable output.
///
/// Counts are conceptually integers but the warmup-robust slope is a real number
/// (a fitted per-iteration rate), so this rounds to two decimals and trims any
/// trailing zeros: `200.0` renders as `200` and `199.5` as `199.5`. A slope of
/// `NaN` — produced when the operation's spans covered zero iterations — renders
/// as `"NaN"` to mark the measurement as unusable.
pub(crate) fn format_count(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    let rounded = (value.max(0.0) * 100.0).round() / 100.0;
    let mut rendered = format!("{rounded:.2}");
    if rendered.contains('.') {
        // Trimming yields a prefix of what is already here, so shortening in place avoids a
        // second allocation on a path that runs once per cell of the summary table.
        let trimmed_len = rendered.trim_end_matches('0').trim_end_matches('.').len();
        rendered.truncate(trimmed_len);
    }
    rendered
}

// The exact layout carries no API contract and the formatter's error paths are unreachable
// here. Which figures appear, and the no-measurements case, are contractual and are tested.
#[cfg_attr(coverage_nightly, coverage(off))]
impl fmt::Display for ReportOperation {
    /// Renders the per-iteration byte and allocation figures.
    ///
    /// The peak and the confidence intervals are deliberately left out to keep the one-line
    /// form readable; [`ReportOperation::statistics`] exposes them all.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.metrics.bytes_slope(), self.metrics.allocations_slope()) {
            (Some(bytes), Some(allocations)) => write!(
                f,
                "{} bytes/iter, {} allocations/iter",
                format_count(bytes),
                format_count(allocations),
            ),
            _ => write!(f, "no measurements"),
        }
    }
}

// The layout carries no API contract, and the formatter's error paths are unreachable here,
// so only the figure availability the table promises is worth asserting on. Tests cover that.
#[cfg_attr(coverage_nightly, coverage(off))]
impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            writeln!(f, "No allocation statistics captured.")?;
            return Ok(());
        }

        writeln!(f, "Allocation statistics:")?;
        writeln!(f)?;

        let headers = [
            "Operation",
            "Bytes/iter",
            "Allocations/iter",
            "Peak bytes/iter",
        ];

        // The confidence interval is kept out of this summary for readability; it remains in
        // the JSON output and the `statistics()` API.
        let rows: Vec<TableRow<'_>> = self
            .sorted_operations()
            .into_iter()
            .map(|(name, operation)| {
                let figures = match operation.statistics() {
                    Some(statistics) => [
                        format_count(statistics.bytes.slope),
                        format_count(statistics.allocations.slope),
                        statistics.peak_outstanding_bytes.map_or_else(
                            || NOT_AVAILABLE.to_owned(),
                            |peak| format_count(peak.slope),
                        ),
                    ],
                    None => [
                        NOT_AVAILABLE.to_owned(),
                        NOT_AVAILABLE.to_owned(),
                        NOT_AVAILABLE.to_owned(),
                    ],
                };

                TableRow { name, figures }
            })
            .collect();

        let mut widths = headers.map(str::len);
        for row in &rows {
            for (width, cell) in widths.iter_mut().zip(row.cells()) {
                *width = (*width).max(cell.len());
            }
        }

        write_table_row(f, headers.iter().copied(), widths)?;

        for width in widths {
            let dashes = width
                .checked_add(TABLE_CELL_PADDING)
                .expect("column width fits in memory, adding the padding cannot overflow");
            write!(f, "|{:-<dashes$}", "")?;
        }
        writeln!(f, "|")?;

        for row in &rows {
            write_table_row(f, row.cells(), widths)?;
        }

        Ok(())
    }
}

/// Number of columns in the rendered summary table.
const TABLE_COLUMNS: usize = 4;

/// Number of columns holding a rendered figure: every column but the operation name.
const TABLE_FIGURE_COLUMNS: usize = TABLE_COLUMNS - 1;

/// Characters framing each cell's contents: one leading and one trailing space.
///
/// The separator row must span the same width as the cells, so it derives its dashes from
/// the same constant that describes the padding `write_table_row` writes.
const TABLE_CELL_PADDING: usize = 2;

/// Rendered in place of a figure the operation cannot supply.
const NOT_AVAILABLE: &str = "n/a";

/// One operation's pre-rendered row of the summary table.
///
/// Pre-rendering lets the column widths and the printed rows be computed from the exact
/// same strings. The name is borrowed from the report rather than copied, because a table
/// is rendered from a report that outlives it.
struct TableRow<'a> {
    name: &'a str,
    figures: [String; TABLE_FIGURE_COLUMNS],
}

impl TableRow<'_> {
    /// The row's cells in column order.
    fn cells(&self) -> impl Iterator<Item = &str> {
        iter::once(self.name).chain(self.figures.iter().map(String::as_str))
    }
}

/// Writes one row of the summary table.
///
/// The operation name is left-aligned and the figures are right-aligned, so digits line up
/// under their headers.
// The layout carries no API contract and the formatter's error paths are unreachable here.
// Which figures appear is contractual, and the table tests cover that.
#[cfg_attr(coverage_nightly, coverage(off))]
fn write_table_row<'a>(
    f: &mut fmt::Formatter<'_>,
    cells: impl Iterator<Item = &'a str>,
    widths: [usize; TABLE_COLUMNS],
) -> fmt::Result {
    let mut cells = cells.zip(widths);

    if let Some((name, width)) = cells.next() {
        write!(f, "| {name:<width$} |")?;
    }

    for (cell, width) in cells {
        write!(f, " {cell:>width$} |")?;
    }

    writeln!(f)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(
        clippy::float_cmp,
        reason = "allocation statistics are exact integer-derived values in these fixtures"
    )]

    use std::panic::{RefUnwindSafe, UnwindSafe};

    use super::*;
    use crate::Session;
    use crate::counters::register_fake_allocation;
    use crate::span_measurement::SpanMeasurement;

    /// Builds a detached [`ReportOperation`] from per-iteration deltas for tests
    /// that assert directly on the report surface without a live session.
    fn report_operation(bytes_delta: u64, count_delta: u64, iterations: u64) -> ReportOperation {
        let mut metrics = OperationMetrics::default();
        metrics.add_iterations(bytes_delta, count_delta, iterations);
        ReportOperation { metrics }
    }

    #[test]
    fn new_report_is_empty() {
        let report = Report::new();
        assert!(report.is_empty());
    }

    #[test]
    fn report_from_empty_session_is_empty() {
        let session = Session::new().no_stdout().no_file();
        let report = session.to_report();
        assert!(report.is_empty());
    }

    #[test]
    fn report_from_session_with_operations_is_not_empty() {
        let session = Session::new().no_stdout().no_file();
        {
            let operation = session.operation("test");
            let _span = operation.measure_thread().iterations(1);
            register_fake_allocation(100, 1);
        } // Span drops here, releasing the mutable borrow

        let report = session.to_report();
        assert!(!report.is_empty());
    }

    #[test]
    fn report_with_registered_but_unmeasured_operation_is_empty() {
        let session = Session::new().no_stdout().no_file();
        let _operation = session.operation("unmeasured");

        let report = session.to_report();
        assert!(report.is_empty());
    }

    #[test]
    fn report_with_only_zero_iteration_spans_is_empty() {
        // A span that covered zero iterations records a span (so statistics can be
        // fit) but no measurable work, so the report is still empty. Guards against
        // `is_empty` regressing to key off whether statistics exist.
        let session = Session::new().no_stdout().no_file();
        {
            let operation = session.operation("failed");
            let _span = operation.measure_thread().iterations(0);
            register_fake_allocation(800, 8);
        }

        let report = session.to_report();
        assert!(report.is_empty());
        // The operation still recorded a span, so it exposes statistics.
        let operations = report.sorted_operations();
        let (_name, operation) = operations.first().expect("the report has one operation");
        assert!(operation.statistics().is_some());
    }

    #[test]
    fn zero_iteration_spans_withhold_the_peak_from_statistics() {
        // `statistics().peak_outstanding_bytes` promises the availability semantics of
        // `peak_outstanding_bytes()`, so an undefined rate must be withheld rather than
        // surfaced as a `NaN` slope that the JSON and the table would then render.
        let mut metrics = OperationMetrics::default();
        metrics.add_iterations(800, 8, 0);
        let operation = ReportOperation { metrics };

        assert_eq!(operation.peak_outstanding_bytes(), None);
        assert!(
            operation
                .statistics()
                .unwrap()
                .peak_outstanding_bytes
                .is_none()
        );
    }

    #[test]
    fn operations_are_sorted_by_name() {
        let mut operations = HashMap::new();
        operations.insert("zebra".to_owned(), report_operation(10, 1, 1));
        operations.insert("alpha".to_owned(), report_operation(20, 2, 1));
        let report = Report { operations };

        let names: Vec<&str> = report
            .sorted_operations()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(names, ["alpha", "zebra"]);
    }

    #[test]
    fn merge_empty_reports() {
        let report1 = Report::new();
        let report2 = Report::new();
        let merged = Report::merge(&report1, &report2);
        assert!(merged.is_empty());
    }

    #[test]
    fn merge_empty_with_non_empty() {
        let session = Session::new().no_stdout().no_file();
        {
            let operation = session.operation("test");
            let _span = operation.measure_thread().iterations(1);
            register_fake_allocation(100, 1);
        } // Span drops here

        let report1 = Report::new();
        let report2 = session.to_report();

        let merged1 = Report::merge(&report1, &report2);
        let merged2 = Report::merge(&report2, &report1);

        assert!(!merged1.is_empty());
        assert!(!merged2.is_empty());
    }

    #[test]
    fn merge_different_operations() {
        let session1 = Session::new().no_stdout().no_file();
        let session2 = Session::new().no_stdout().no_file();

        {
            let op1 = session1.operation("test1");
            let _span1 = op1.measure_thread().iterations(1);
            register_fake_allocation(100, 1);
        } // Span drops here

        {
            let op2 = session2.operation("test2");
            let _span2 = op2.measure_thread().iterations(1);
            register_fake_allocation(200, 2);
        } // Span drops here

        let report1 = session1.to_report();
        let report2 = session2.to_report();
        let merged = Report::merge(&report1, &report2);

        assert_eq!(merged.operations.len(), 2);
        assert!(merged.operations.contains_key("test1"));
        assert!(merged.operations.contains_key("test2"));
    }

    #[test]
    fn merge_same_operations() {
        let session1 = Session::new().no_stdout().no_file();
        let session2 = Session::new().no_stdout().no_file();

        {
            let op1 = session1.operation("test");
            let _span1 = op1.measure_thread().iterations(1);
            register_fake_allocation(100, 1);
        } // Span drops here

        {
            let op2 = session2.operation("test");
            let _span2 = op2.measure_thread().iterations(1);
            register_fake_allocation(200, 2);
        } // Span drops here

        let report1 = session1.to_report();
        let report2 = session2.to_report();
        let merged = Report::merge(&report1, &report2);

        assert_eq!(merged.operations.len(), 1);
        let merged_op = merged.operations.get("test").unwrap();
        assert_eq!(merged_op.total_iterations(), 2); // 1 + 1
        assert_eq!(merged_op.total_bytes_allocated(), 300); // 100 + 200
        assert_eq!(merged_op.total_allocations_count(), 3); // 1 + 2
    }

    #[test]
    fn report_clone() {
        let session = Session::new().no_stdout().no_file();
        {
            let operation = session.operation("test");
            let _span = operation.measure_thread().iterations(1);
            register_fake_allocation(100, 1);
        } // Span drops here

        let report1 = session.to_report();
        let report2 = report1.clone();

        assert_eq!(report1.operations.len(), report2.operations.len());
    }

    #[test]
    fn report_operation_total_allocations_count_zero() {
        let operation = report_operation(0, 0, 1);
        assert_eq!(operation.total_allocations_count(), 0);
    }

    #[test]
    fn report_operation_total_allocations_count_multiple() {
        // 100 bytes and 5 allocations per iteration over 5 iterations.
        let operation = report_operation(100, 5, 5);
        assert_eq!(operation.total_allocations_count(), 25);
    }

    #[test]
    fn report_operation_total_allocations_count_consistency_with_session() {
        let session = Session::new().no_stdout().no_file();
        {
            let operation = session.operation("test_consistency");
            let _span = operation.measure_thread().iterations(1);
            // Simulate 3 allocations
            register_fake_allocation(300, 3);
        } // Span drops here

        let report = session.to_report();
        let operations: Vec<_> = report.operations().collect();
        assert_eq!(operations.len(), 1);

        let (_name, report_op) = operations.first().unwrap();
        assert_eq!(report_op.total_allocations_count(), 3);
        assert_eq!(report_op.total_bytes_allocated(), 300);
        assert_eq!(report_op.total_iterations(), 1);
    }

    #[test]
    fn statistics_are_none_without_spans() {
        let session = Session::new().no_stdout().no_file();
        let report = session.to_report();
        assert!(report.operations().next().is_none());
    }

    #[test]
    fn statistics_expose_byte_and_allocation_estimates() {
        // A single recorded span yields a span count of one and a slope equal to
        // the per-iteration mean, but carries no dispersion information, so the
        // interval is withheld.
        let operation = report_operation(200, 2, 4);
        let stats = operation.statistics().unwrap();
        assert_eq!(stats.span_count, 1);
        assert_eq!(stats.bytes.slope, 200.0);
        assert_eq!(stats.bytes.interval, None);
        assert_eq!(stats.allocations.slope, 2.0);
        assert_eq!(stats.allocations.interval, None);
    }

    #[test]
    fn repeated_identical_spans_collapse_the_interval_onto_the_slope() {
        // Two identical spans clear the two-span threshold with zero residual
        // dispersion, so the interval collapses onto the slope.
        let mut metrics = OperationMetrics::default();
        metrics.add_iterations(200, 2, 4);
        metrics.add_iterations(200, 2, 4);
        let operation = ReportOperation { metrics };

        let stats = operation.statistics().unwrap();
        assert_eq!(stats.span_count, 2);
        assert_eq!(stats.bytes.slope, 200.0);
        assert_eq!(stats.bytes.interval, Some((200.0, 200.0)));
    }

    // Static assertions for thread safety.
    static_assertions::assert_impl_all!(Report: Send, Sync);
    static_assertions::assert_impl_all!(ReportOperation: Send, Sync);
    static_assertions::assert_impl_all!(OperationStatistics: Send, Sync);
    static_assertions::assert_impl_all!(MetricStatistics: Send, Sync);

    // Static assertions for unwind safety.
    static_assertions::assert_impl_all!(Report: UnwindSafe, RefUnwindSafe);
    static_assertions::assert_impl_all!(
        ReportOperation: UnwindSafe, RefUnwindSafe
    );

    #[test]
    fn report_operation_display_shows_robust_per_iteration_estimate() {
        // 250 bytes/iter over 4 iterations → a single-span slope of 250 with the
        // interval collapsed onto it. Each figure is asserted against its own label so
        // that rendering one metric in the other's place would fail.
        let operation = report_operation(250, 3, 4);
        let display_output = operation.to_string();
        assert!(
            display_output.contains("250 bytes/iter"),
            "got {display_output}"
        );
        assert!(
            display_output.contains("3 allocations/iter"),
            "got {display_output}"
        );
    }

    #[test]
    fn report_operation_display_shows_nan_for_zero_iterations() {
        // A span that covered zero iterations has no per-iteration rate, so the
        // slopes are NaN and render as "NaN" rather than a misleading "0".
        let operation = report_operation(250, 3, 0);
        let display_output = operation.to_string();
        assert!(
            display_output.contains("NaN bytes/iter"),
            "got {display_output}"
        );
        assert!(
            display_output.contains("NaN allocations/iter"),
            "got {display_output}"
        );
    }

    #[test]
    fn report_operation_display_reports_no_measurements_when_empty() {
        // A report operation whose metrics recorded no spans has no statistics, so
        // its Display takes the `None` leg.
        let operation = ReportOperation {
            metrics: OperationMetrics::default(),
        };
        assert_eq!(operation.to_string(), "no measurements");
    }

    #[test]
    fn empty_report_display_shows_no_statistics_message() {
        let report = Report::new();
        let display_output = report.to_string();
        assert!(display_output.contains("No allocation statistics captured."));
    }

    #[test]
    fn report_display_renders_each_peak_according_to_its_availability() {
        // An operation with no peak figure renders as unavailable in the table
        // (design.md, "Reporting"). Each metric gets a distinct value so that finding one
        // in the peak column proves the column carries the peak and not a neighbour.
        const ITERATIONS: u64 = 4;
        const PEAK_PER_ITERATION: u64 = 700;

        let mut measured = OperationMetrics::default();
        measured.add_span(SpanMeasurement {
            iterations: ITERATIONS,
            bytes: 250 * ITERATIONS,
            count: 3 * ITERATIONS,
            peak_outstanding_bytes: Some(PEAK_PER_ITERATION),
        });

        // A process span withholds the peak, which makes the whole operation unable to
        // report one even though its byte and allocation rates remain well defined.
        let mut process_measured = OperationMetrics::default();
        process_measured.add_span(SpanMeasurement {
            iterations: ITERATIONS,
            bytes: 250 * ITERATIONS,
            count: 3 * ITERATIONS,
            peak_outstanding_bytes: None,
        });

        // Zero iterations leave every rate undefined, but only the peak can say so.
        let mut zero_iterations = OperationMetrics::default();
        zero_iterations.add_iterations(250, 3, 0);

        let mut operations = HashMap::new();
        operations.insert("thread".to_owned(), ReportOperation { metrics: measured });
        operations.insert(
            "process".to_owned(),
            ReportOperation {
                metrics: process_measured,
            },
        );
        operations.insert(
            "nothing".to_owned(),
            ReportOperation {
                metrics: zero_iterations,
            },
        );
        let report = Report { operations };

        let display_output = report.to_string();

        // Splitting on the cell separator addresses the peak by column without pinning the
        // widths or alignment, neither of which is contractual.
        let peak_cell = |name: &str| {
            let row = display_output
                .lines()
                .find(|line| line.contains(name))
                .unwrap_or_else(|| panic!("the table has a row for {name}, got {display_output}"));
            let cells: Vec<&str> = row.trim_matches('|').split('|').map(str::trim).collect();
            assert_eq!(cells.len(), TABLE_COLUMNS);
            cells.last().copied().unwrap().to_owned()
        };

        assert_eq!(peak_cell("thread"), PEAK_PER_ITERATION.to_string());
        assert_eq!(peak_cell("process"), NOT_AVAILABLE);
        assert_eq!(peak_cell("nothing"), NOT_AVAILABLE);
    }
}
