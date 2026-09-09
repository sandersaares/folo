//! Engine-output fixtures for parser and orchestration tests.
//!
//! This crate owns the parsers, so it also owns the compile-time copy of each
//! parser fixture. In-crate tests and in-workspace command tests (via the
//! `private-test-util` feature) share these strings instead of embedding a second
//! copy. End-user builds never compile this module.
//!
//! Callgrind and Criterion files are real producer output and act as schema-drift
//! canaries — do not hand-edit them to make a test pass; regenerate from a real
//! run. `alloc_tracker` and `all_the_time` files are representative samples of the
//! current schema; the round-trip test is the drift canary.
//! Orchestration tests use the minimal synthetic documents below instead of
//! repeatedly parsing unrelated producer metadata.

#![cfg_attr(coverage_nightly, coverage(off))]

/// A small synthetic Callgrind result for orchestration tests.
///
/// Contains an identity and each tracked metric, without unrelated producer
/// metadata. Real-output parser tests use the committed fixtures instead.
pub const CALLGRIND_MINIMAL: &str = concat!(
    r#"{"version":"6","module_path":"m","function_name":"f","id":null,"package_dir":"/pkg","#,
    r#""profiles":[{"summaries":{"total":{"summary":{"Callgrind":{"#,
    r#""Ir":{"metrics":{"Left":{"Int":36}}},"#,
    r#""Bc":{"metrics":{"Left":{"Int":4}}},"#,
    r#""Bi":{"metrics":{"Left":{"Int":2}}}"#,
    r#"}}}}}]}"#,
);

/// A small synthetic Criterion identity for orchestration tests.
pub const CRITERION_MINIMAL_BENCHMARK: &str = r#"{"group_id":"g","function_id":"f"}"#;

/// A small synthetic Criterion estimate with an interval for orchestration tests.
pub const CRITERION_MINIMAL_ESTIMATES: &str = concat!(
    r#"{"mean":{"point_estimate":2.0,"#,
    r#""confidence_interval":{"lower_bound":1.0,"upper_bound":3.0}}}"#,
);

/// Real Gungraun `summary.json` for a single unparametrized benchmark.
///
/// Do not hand-edit; regenerate from a real run.
pub const CALLGRIND_SINGLE_UNPARAMETRIZED: &str =
    include_str!("../tests/fixtures/callgrind/single_unparametrized.summary.json");

/// Real Gungraun `summary.json` for a parametrized benchmark.
///
/// Do not hand-edit; regenerate from a real run.
pub const CALLGRIND_PARAMETRIZED: &str =
    include_str!("../tests/fixtures/callgrind/parametrized.summary.json");

/// Real Criterion `benchmark.json` for the `std_instant` case.
///
/// Do not hand-edit; regenerate from a real run.
pub const CRITERION_STD_INSTANT_BENCHMARK: &str =
    include_str!("../tests/fixtures/criterion/std_instant/benchmark.json");

/// Real Criterion `estimates.json` for the `std_instant` case.
///
/// Do not hand-edit; regenerate from a real run.
pub const CRITERION_STD_INSTANT_ESTIMATES: &str =
    include_str!("../tests/fixtures/criterion/std_instant/estimates.json");

/// Real Criterion `benchmark.json` for the `fast_time_clock` case.
///
/// Do not hand-edit; regenerate from a real run.
pub const CRITERION_FAST_TIME_CLOCK_BENCHMARK: &str =
    include_str!("../tests/fixtures/criterion/fast_time_clock/benchmark.json");

/// Real Criterion `estimates.json` for the `fast_time_clock` case.
///
/// Do not hand-edit; regenerate from a real run.
pub const CRITERION_FAST_TIME_CLOCK_ESTIMATES: &str =
    include_str!("../tests/fixtures/criterion/fast_time_clock/estimates.json");

/// Representative `alloc_tracker` single-span operation output.
pub const ALLOC_TRACKER_ALLOCATE_VEC: &str =
    include_str!("../tests/fixtures/alloc_tracker/allocate_vec.json");

/// Representative `alloc_tracker` multi-span operation output.
pub const ALLOC_TRACKER_ALLOCATE_VEC_DISPERSION: &str =
    include_str!("../tests/fixtures/alloc_tracker/allocate_vec_dispersion.json");

/// Representative `all_the_time` single-span operation output.
pub const ALL_THE_TIME_READ_CELL: &str =
    include_str!("../tests/fixtures/all_the_time/read_cell.json");

/// Representative `all_the_time` multi-span operation output.
pub const ALL_THE_TIME_READ_CELL_DISPERSION: &str =
    include_str!("../tests/fixtures/all_the_time/read_cell_dispersion.json");
