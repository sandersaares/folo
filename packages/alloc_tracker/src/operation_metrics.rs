//! Per-operation allocation metrics, folded into streaming statistics.
//!
//! Every measured span hands over a [`SpanMeasurement`]: its whole-span byte and
//! allocation-count deltas, the iteration count it covered, and the peak it observed
//! if its scope could observe one. The spans are not retained: each is folded on
//! arrival into running totals (for the pooled means) and into [`SpanAccumulator`]s
//! (for the warmup-robust estimates and their confidence intervals). Allocation
//! figures are not deterministic — first-run allocations and buffer resizing jitter
//! around the mean over a Criterion-chosen iteration count — so the estimates
//! down-weight low-iteration warmup spans and the intervals quantify the residual
//! noise.
//!
//! A span that could not observe a peak makes the operation's peak permanently
//! unavailable, including across the merges that combine per-thread copies of an
//! operation.

use folo_utils::SpanAccumulator;

use crate::SpanMeasurement;

/// Metrics tracked for each operation in the session.
///
/// Holds the pooled totals (bytes, allocation count, iterations) and the estimators over
/// per-iteration bytes, per-iteration allocation counts and the per-iteration peak, folded
/// in as each span is recorded. No per-span data is retained.
#[derive(Clone, Debug, Default)]
pub(crate) struct OperationMetrics {
    total_iterations: u64,
    total_bytes: u64,
    total_count: u64,

    bytes: SpanAccumulator,
    allocations: SpanAccumulator,
    peak: PeakEstimate,
}

impl OperationMetrics {
    /// Records one span's measurement.
    ///
    /// Folding a span is a handful of additions with no allocation, so it is
    /// cheap enough to run inside a measured span.
    pub(crate) fn add_span(&mut self, span: SpanMeasurement) {
        self.total_iterations = self
            .total_iterations
            .checked_add(span.iterations)
            .expect("total iterations overflows u64 - this indicates an unrealistic scenario");
        self.total_bytes = self
            .total_bytes
            .checked_add(span.bytes)
            .expect("total bytes overflows u64 - this indicates an unrealistic scenario");
        self.total_count = self
            .total_count
            .checked_add(span.count)
            .expect("total allocations overflows u64 - this indicates an unrealistic scenario");

        self.peak
            .record(span.iterations, span.peak_outstanding_bytes);

        self.bytes.add(span.iterations, span.bytes);
        self.allocations.add(span.iterations, span.count);
    }

    /// Records one span by its per-iteration deltas and iteration count.
    ///
    /// A convenience over [`add_span`](Self::add_span) used where per-iteration
    /// figures are already known; the whole-span totals are reconstituted by
    /// multiplying back out.
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding, not shipped behavior.
    pub(crate) fn add_iterations(&mut self, bytes_delta: u64, count_delta: u64, iterations: u64) {
        let bytes = bytes_delta
            .checked_mul(iterations)
            .expect("bytes * iterations overflows u64 - this indicates an unrealistic scenario");
        let count = count_delta
            .checked_mul(iterations)
            .expect("count * iterations overflows u64 - this indicates an unrealistic scenario");
        self.add_span(SpanMeasurement {
            iterations,
            bytes,
            count,
            peak_outstanding_bytes: Some(bytes_delta),
        });
    }

    /// Number of spans recorded (distinct from the total iteration count).
    pub(crate) fn span_count(&self) -> u64 {
        self.bytes.span_count()
    }

    /// Total iterations across all recorded spans.
    pub(crate) fn total_iterations(&self) -> u64 {
        self.total_iterations
    }

    /// Total bytes allocated across all recorded spans.
    pub(crate) fn total_bytes_allocated(&self) -> u64 {
        self.total_bytes
    }

    /// Total number of allocations across all recorded spans.
    pub(crate) fn total_allocations_count(&self) -> u64 {
        self.total_count
    }

    /// The warmup-robust per-iteration peak.
    ///
    /// Returns `None` when no span has been recorded, or when any recorded span was of a
    /// kind that cannot observe a peak.
    pub(crate) fn peak_outstanding_bytes(&self) -> Option<f64> {
        self.peak.slope()
    }

    /// The confidence interval of the per-iteration peak, or `None` when it cannot be
    /// estimated.
    pub(crate) fn peak_interval(&self) -> Option<(f64, f64)> {
        self.peak.interval()
    }

    /// Mean bytes allocated per iteration, pooled across all spans.
    ///
    /// Returns zero when no iterations were recorded.
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding, not shipped behavior.
    pub(crate) fn mean_bytes(&self) -> u64 {
        self.total_bytes
            .checked_div(self.total_iterations)
            .unwrap_or(0)
    }

    /// Mean number of allocations per iteration, pooled across all spans.
    ///
    /// Returns zero when no iterations were recorded.
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding, not shipped behavior.
    pub(crate) fn mean_allocations(&self) -> u64 {
        self.total_count
            .checked_div(self.total_iterations)
            .unwrap_or(0)
    }

    /// Whether the operation recorded any measurable work.
    pub(crate) fn is_empty(&self) -> bool {
        self.total_iterations == 0
    }

    /// The warmup-robust per-iteration byte slope, or `None` when no spans were
    /// recorded.
    pub(crate) fn bytes_slope(&self) -> Option<f64> {
        self.bytes.slope()
    }

    /// The warmup-robust per-iteration allocation-count slope, or `None` when no
    /// spans were recorded.
    pub(crate) fn allocations_slope(&self) -> Option<f64> {
        self.allocations.slope()
    }

    /// The confidence interval of the per-iteration byte slope, or `None` when it
    /// cannot be estimated (fewer than two spans, or a non-finite estimate).
    pub(crate) fn bytes_interval(&self) -> Option<(f64, f64)> {
        self.bytes.interval()
    }

    /// The confidence interval of the per-iteration allocation-count slope, or
    /// `None` when it cannot be estimated.
    pub(crate) fn allocations_interval(&self) -> Option<(f64, f64)> {
        self.allocations.interval()
    }

    /// Merges another operation's statistics into this one.
    pub(crate) fn merge(&mut self, other: &Self) {
        self.total_iterations = self
            .total_iterations
            .checked_add(other.total_iterations)
            .expect("total iterations overflows u64 - this indicates an unrealistic scenario");
        self.total_bytes = self
            .total_bytes
            .checked_add(other.total_bytes)
            .expect("total bytes overflows u64 - this indicates an unrealistic scenario");
        self.total_count = self
            .total_count
            .checked_add(other.total_count)
            .expect("total allocations overflows u64 - this indicates an unrealistic scenario");
        self.bytes.merge(&other.bytes);
        self.allocations.merge(&other.allocations);
        self.peak.merge(&other.peak);
    }
}
/// What is known about an operation's peak outstanding bytes.
///
/// Only some kinds of span can observe a peak, and a single span that cannot leaves the
/// operation's peak unknowable rather than merely understated
/// (`docs/design.md`, "Peak outstanding bytes"). Holding the estimator inside the
/// available variant means an operation cannot be in the contradictory state of having
/// both an accumulated estimate and a reason the estimate is meaningless.
#[derive(Clone, Debug)]
enum PeakEstimate {
    /// Every span recorded so far reported a peak, folded into the estimator.
    ///
    /// The estimator is empty until the first span arrives, which is why an operation with
    /// no spans at all still reports no peak.
    Available(SpanAccumulator),

    /// At least one recorded span could not observe a peak.
    Unavailable,
}

impl PeakEstimate {
    /// Records what one span observed, which may be nothing.
    fn record(&mut self, iterations: u64, peak: Option<u64>) {
        let Self::Available(peaks) = self else {
            return;
        };

        let Some(peak) = peak else {
            *self = Self::Unavailable;
            return;
        };

        // A peak is a level rather than a total — it does not grow with the number of
        // iterations the span covered — so the accumulator weights it as one, giving the
        // span peaks averaged with the same warmup-robust weighting the other metrics get.
        // Ref: docs/implementation.md, "Peak aggregation".
        peaks.add_level(iterations, peak);
    }

    /// Folds another operation's peak knowledge into this one.
    fn merge(&mut self, other: &Self) {
        match (&mut *self, other) {
            (Self::Available(peaks), Self::Available(other_peaks)) => peaks.merge(other_peaks),
            (Self::Available(_), Self::Unavailable) => *self = Self::Unavailable,
            (Self::Unavailable, _) => {}
        }
    }

    /// The warmup-robust typical span peak, or `None` when there is none to report.
    fn slope(&self) -> Option<f64> {
        match self {
            Self::Available(peaks) => peaks.slope(),
            Self::Unavailable => None,
        }
    }

    /// The confidence interval of the typical span peak, or `None` when it cannot be
    /// estimated.
    fn interval(&self) -> Option<(f64, f64)> {
        match self {
            Self::Available(peaks) => peaks.interval(),
            Self::Unavailable => None,
        }
    }
}

impl Default for PeakEstimate {
    fn default() -> Self {
        Self::Available(SpanAccumulator::default())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(
        clippy::float_cmp,
        reason = "slope assertions are exact integer-derived values in these fixtures"
    )]

    use super::*;

    /// A span that reports a peak, for tests concerned with the other figures.
    fn span(iterations: u64, bytes: u64, count: u64) -> SpanMeasurement {
        SpanMeasurement {
            iterations,
            bytes,
            count,
            peak_outstanding_bytes: Some(bytes),
        }
    }

    /// A span that cannot report a peak, as produced by process-scoped measurement.
    fn span_without_peak(iterations: u64, bytes: u64, count: u64) -> SpanMeasurement {
        SpanMeasurement {
            iterations,
            bytes,
            count,
            peak_outstanding_bytes: None,
        }
    }

    #[test]
    fn peak_is_absent_without_spans() {
        let metrics = OperationMetrics::default();

        assert_eq!(metrics.peak_outstanding_bytes(), None);
    }

    #[test]
    fn peak_recovers_a_span_peak_that_does_not_vary_with_batch_size() {
        // The metric's model: every iteration in a batch reaches the same peak, so the
        // span peak is the per-iteration peak however many iterations the span covered.
        let mut metrics = OperationMetrics::default();
        metrics.add_span(SpanMeasurement {
            iterations: 2,
            bytes: 100,
            count: 1,
            peak_outstanding_bytes: Some(64),
        });
        metrics.add_span(SpanMeasurement {
            iterations: 8,
            bytes: 400,
            count: 4,
            peak_outstanding_bytes: Some(64),
        });

        assert_eq!(metrics.peak_outstanding_bytes(), Some(64.0));
    }

    #[test]
    fn peak_is_the_squared_iteration_weighted_mean_of_span_peaks() {
        // Each span's peak enters the mean with the square of its own iteration count as the
        // weight, so the longer span dominates by more than its length alone.
        let mut metrics = OperationMetrics::default();
        metrics.add_span(SpanMeasurement {
            iterations: 1,
            bytes: 100,
            count: 1,
            peak_outstanding_bytes: Some(1000),
        });
        metrics.add_span(SpanMeasurement {
            iterations: 3,
            bytes: 300,
            count: 3,
            peak_outstanding_bytes: Some(100),
        });

        assert_eq!(metrics.peak_outstanding_bytes(), Some(190.0));
    }

    #[test]
    fn peak_downweights_low_iteration_warmup_spans() {
        // A warmup span allocating a hundredfold peak over a single iteration barely
        // moves the estimate away from the steady state its neighbour measured.
        let mut metrics = OperationMetrics::default();
        metrics.add_span(SpanMeasurement {
            iterations: 1,
            bytes: 10_000,
            count: 1,
            peak_outstanding_bytes: Some(10_000),
        });
        metrics.add_span(SpanMeasurement {
            iterations: 1000,
            bytes: 100_000,
            count: 1000,
            peak_outstanding_bytes: Some(100),
        });

        let peak = metrics.peak_outstanding_bytes().unwrap();
        assert!(peak > 100.0);
        assert!(peak < 101.0);
    }

    #[test]
    fn one_span_without_a_peak_suppresses_the_operation_peak() {
        let mut metrics = OperationMetrics::default();
        metrics.add_span(span(1, 100, 1));
        metrics.add_span(span_without_peak(1, 100, 1));
        metrics.add_span(span(1, 100, 1));

        assert_eq!(metrics.peak_outstanding_bytes(), None);
        assert_eq!(metrics.peak_interval(), None);
    }

    #[test]
    fn peak_interval_collapses_onto_an_unvarying_peak() {
        let mut metrics = OperationMetrics::default();
        metrics.add_span(SpanMeasurement {
            iterations: 2,
            bytes: 100,
            count: 1,
            peak_outstanding_bytes: Some(64),
        });
        metrics.add_span(SpanMeasurement {
            iterations: 4,
            bytes: 200,
            count: 2,
            peak_outstanding_bytes: Some(64),
        });

        assert_eq!(metrics.peak_interval(), Some((64.0, 64.0)));
    }

    #[test]
    fn peak_interval_absent_with_a_single_span() {
        let mut metrics = OperationMetrics::default();
        metrics.add_span(span(4, 20, 4));

        assert!(metrics.peak_interval().is_none());
    }

    #[test]
    fn merging_folds_peak_spans_as_if_recorded_together() {
        let mut first = OperationMetrics::default();
        first.add_span(SpanMeasurement {
            iterations: 1,
            bytes: 100,
            count: 1,
            peak_outstanding_bytes: Some(1000),
        });

        let mut second = OperationMetrics::default();
        second.add_span(SpanMeasurement {
            iterations: 3,
            bytes: 300,
            count: 3,
            peak_outstanding_bytes: Some(100),
        });

        first.merge(&second);

        assert_eq!(first.peak_outstanding_bytes(), Some(190.0));
    }

    #[test]
    fn merging_with_an_unmeasured_operation_keeps_the_known_peak() {
        let mut measured = OperationMetrics::default();
        measured.add_span(span(1, 100, 1));

        measured.merge(&OperationMetrics::default());

        assert_eq!(measured.peak_outstanding_bytes(), Some(100.0));
    }

    #[test]
    fn merging_in_an_unavailable_peak_suppresses_the_result() {
        let mut measured = OperationMetrics::default();
        measured.add_span(span(1, 100, 1));

        let mut unavailable = OperationMetrics::default();
        unavailable.add_span(span_without_peak(1, 100, 1));

        measured.merge(&unavailable);

        assert_eq!(measured.peak_outstanding_bytes(), None);
    }

    #[test]
    fn merging_into_an_unavailable_peak_leaves_it_suppressed() {
        // `Report::merge` merges its argument into a clone of the receiver, so which
        // report is passed first decides which `PeakEstimate::merge` arm runs. The
        // unavailable state must absorb an available one just as it survives being
        // merged into, or the peak would come back for one argument order only.
        let mut unavailable = OperationMetrics::default();
        unavailable.add_span(span_without_peak(1, 100, 1));

        let mut measured = OperationMetrics::default();
        measured.add_span(span(1, 100, 1));

        unavailable.merge(&measured);

        assert_eq!(unavailable.peak_outstanding_bytes(), None);
    }

    #[test]
    fn default_has_no_spans() {
        let metrics = OperationMetrics::default();
        assert_eq!(metrics.total_bytes_allocated(), 0);
        assert_eq!(metrics.total_allocations_count(), 0);
        assert_eq!(metrics.total_iterations(), 0);
        assert_eq!(metrics.span_count(), 0);
        assert!(metrics.is_empty());
    }

    #[test]
    fn add_iterations_basic() {
        let mut metrics = OperationMetrics::default();
        metrics.add_iterations(100, 5, 5);

        assert_eq!(metrics.total_iterations(), 5);
        assert_eq!(metrics.span_count(), 1);
        assert_eq!(metrics.total_bytes_allocated(), 500);
        assert_eq!(metrics.total_allocations_count(), 25);
    }

    #[test]
    fn add_iterations_zero_iterations() {
        let mut metrics = OperationMetrics::default();
        metrics.add_iterations(100, 2, 0);

        assert_eq!(metrics.total_iterations(), 0);
        assert_eq!(metrics.total_bytes_allocated(), 0);
        assert_eq!(metrics.total_allocations_count(), 0);
    }

    #[test]
    fn zero_iteration_span_yields_nan_slopes() {
        // A span that covered zero iterations (e.g. a workload that failed to run)
        // has no per-iteration rate, so every slope reports NaN rather than a
        // misleading zero.
        let mut metrics = OperationMetrics::default();
        metrics.add_iterations(100, 2, 0);

        assert!(metrics.bytes_slope().unwrap().is_nan());
        assert!(metrics.allocations_slope().unwrap().is_nan());
        assert_eq!(metrics.bytes_interval(), None);
        assert_eq!(metrics.allocations_interval(), None);
    }

    #[test]
    fn add_iterations_zero_allocation() {
        let mut metrics = OperationMetrics::default();
        metrics.add_iterations(0, 0, 1000);

        assert_eq!(metrics.total_iterations(), 1000);
        assert_eq!(metrics.total_bytes_allocated(), 0);
        assert_eq!(metrics.total_allocations_count(), 0);
    }

    #[test]
    fn add_iterations_accumulates() {
        let mut metrics = OperationMetrics::default();
        metrics.add_iterations(100, 2, 2); // 200 bytes, 4 allocations, 2 iterations
        metrics.add_iterations(200, 3, 3); // 600 bytes, 9 allocations, 3 iterations

        assert_eq!(metrics.total_iterations(), 5);
        assert_eq!(metrics.span_count(), 2);
        assert_eq!(metrics.total_bytes_allocated(), 800);
        assert_eq!(metrics.total_allocations_count(), 13);
    }

    #[test]
    fn pooled_means_divide_totals_by_iterations() {
        let mut metrics = OperationMetrics::default();
        metrics.add_iterations(100, 1, 1);
        metrics.add_iterations(200, 2, 1);
        metrics.add_iterations(300, 3, 1);

        // (100 + 200 + 300) / 3 = 200; (1 + 2 + 3) / 3 = 2.
        assert_eq!(metrics.mean_bytes(), 200);
        assert_eq!(metrics.mean_allocations(), 2);
    }

    #[test]
    fn means_of_empty_metrics_are_zero() {
        let metrics = OperationMetrics::default();
        assert_eq!(metrics.mean_bytes(), 0);
        assert_eq!(metrics.mean_allocations(), 0);
    }

    #[test]
    fn merge_combines_metrics() {
        let mut first = OperationMetrics::default();
        first.add_iterations(100, 1, 2);

        let mut second = OperationMetrics::default();
        second.add_iterations(50, 1, 3);

        first.merge(&second);
        assert_eq!(first.span_count(), 2);
        assert_eq!(first.total_iterations(), 5);
        assert_eq!(first.total_bytes_allocated(), 350); // 200 + 150
    }

    #[test]
    fn empty_metrics_have_no_slope() {
        let metrics = OperationMetrics::default();
        assert!(metrics.bytes_slope().is_none());
        assert!(metrics.allocations_slope().is_none());
    }

    #[test]
    fn slope_weights_spans_by_iteration_count() {
        // A perfectly linear byte series at 5 bytes/iter: the slope recovers 5
        // regardless of the differing iteration counts across spans.
        let mut metrics = OperationMetrics::default();
        metrics.add_span(span(2, 10, 2));
        metrics.add_span(span(8, 40, 8));

        assert_eq!(metrics.span_count(), 2);
        assert_eq!(metrics.bytes_slope(), Some(5.0));
    }

    #[test]
    fn intervals_reported_once_two_spans_recorded() {
        // Two spans at a constant per-iteration rate (5 bytes/iter, 1 alloc/iter):
        // with zero residual dispersion each interval collapses onto its slope.
        let mut metrics = OperationMetrics::default();
        metrics.add_span(span(2, 10, 2));
        metrics.add_span(span(4, 20, 4));

        assert_eq!(metrics.bytes_interval(), Some((5.0, 5.0)));
        assert_eq!(metrics.allocations_interval(), Some((1.0, 1.0)));
    }

    #[test]
    fn intervals_absent_with_a_single_span() {
        // One span pins the slopes but carries no dispersion, so neither the byte
        // nor the allocation-count interval is formed.
        let mut metrics = OperationMetrics::default();
        metrics.add_span(span(4, 20, 4));

        assert!(metrics.bytes_interval().is_none());
        assert!(metrics.allocations_interval().is_none());
    }
}
