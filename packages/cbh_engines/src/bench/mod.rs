//! Benchmark-engine adapters: per-engine environment injection, the WSL env
//! propagation rule, and parsing of each engine's output into the model.
//!
//! Four engines are supported: Callgrind (via Gungraun, low-noise instruction
//! counts), Criterion (wall-clock timings), `alloc_tracker` (allocation counts
//! and bytes) and `all_the_time` (processor time). None is exact — every metric
//! carries run-to-run noise.

mod all_the_time;
mod alloc_tracker;
mod callgrind;
mod criterion;
mod env;
mod paths;
#[cfg(test)]
mod schema_roundtrip;

pub use all_the_time::{AllTheTimeParseError, parse_all_the_time_operation};
pub use alloc_tracker::{AllocTrackerParseError, parse_alloc_tracker_operation};
pub use callgrind::{CallgrindParseError, parse_callgrind_summary};
pub use criterion::{CriterionParseError, parse_criterion_case};
pub use env::injected_bench_env;
pub(crate) use env::usable_slope;
pub(crate) use paths::*;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use cbh_model::{BenchmarkResult, Engine, IntervalSupport};

    use crate::bench::{
        parse_all_the_time_operation, parse_alloc_tracker_operation, parse_callgrind_summary,
        parse_criterion_case,
    };
    use crate::testing::{
        ALL_THE_TIME_READ_CELL as ALL_THE_TIME_SINGLE_SPAN,
        ALL_THE_TIME_READ_CELL_DISPERSION as ALL_THE_TIME_MULTI_SPAN,
        ALLOC_TRACKER_ALLOCATE_VEC as ALLOC_TRACKER_SINGLE_SPAN,
        ALLOC_TRACKER_ALLOCATE_VEC_DISPERSION as ALLOC_TRACKER_MULTI_SPAN,
        CALLGRIND_SINGLE_UNPARAMETRIZED as CALLGRIND_SUMMARY,
        CRITERION_STD_INSTANT_BENCHMARK as CRITERION_BENCHMARK,
        CRITERION_STD_INSTANT_ESTIMATES as CRITERION_ESTIMATES,
    };

    #[test]
    fn criterion_interval_support_matches_adapter_output() {
        assert_eq!(
            Engine::Criterion.interval_support(),
            IntervalSupport::Always
        );
        let record = parse_criterion_case(CRITERION_BENCHMARK, CRITERION_ESTIMATES).unwrap();
        assert_all_metrics_have_intervals(&record);
    }

    #[test]
    fn callgrind_interval_support_matches_adapter_output() {
        assert_eq!(Engine::Callgrind.interval_support(), IntervalSupport::Never);
        let record = parse_callgrind_summary(CALLGRIND_SUMMARY).unwrap();
        assert_no_metrics_have_intervals(&record);
    }

    #[test]
    fn alloc_tracker_interval_support_matches_adapter_output() {
        assert_eq!(
            Engine::AllocTracker.interval_support(),
            IntervalSupport::MultiSpanOnly
        );
        let single_span = parse_alloc_tracker_operation(ALLOC_TRACKER_SINGLE_SPAN)
            .unwrap()
            .unwrap();
        let multi_span = parse_alloc_tracker_operation(ALLOC_TRACKER_MULTI_SPAN)
            .unwrap()
            .unwrap();
        assert_no_metrics_have_intervals(&single_span);
        assert_all_metrics_have_intervals(&multi_span);
    }

    #[test]
    fn all_the_time_interval_support_matches_adapter_output() {
        assert_eq!(
            Engine::AllTheTime.interval_support(),
            IntervalSupport::MultiSpanOnly
        );
        let single_span = parse_all_the_time_operation(ALL_THE_TIME_SINGLE_SPAN)
            .unwrap()
            .unwrap();
        let multi_span = parse_all_the_time_operation(ALL_THE_TIME_MULTI_SPAN)
            .unwrap()
            .unwrap();
        assert_no_metrics_have_intervals(&single_span);
        assert_all_metrics_have_intervals(&multi_span);
    }

    fn assert_all_metrics_have_intervals(record: &BenchmarkResult) {
        assert!(!record.metrics.is_empty());
        for metric in &record.metrics {
            assert!(metric.interval_low.is_some());
            assert!(metric.interval_high.is_some());
        }
    }

    fn assert_no_metrics_have_intervals(record: &BenchmarkResult) {
        assert!(!record.metrics.is_empty());
        for metric in &record.metrics {
            assert!(metric.interval_low.is_none());
            assert!(metric.interval_high.is_none());
        }
    }
}
