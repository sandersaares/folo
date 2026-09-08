//! Unit tests for the observation-bag storage types, organized by topic so each module
//! stays focused: single-observation recording, bag and snapshot merging, push-time copying,
//! and concurrent access.

#[cfg_attr(coverage_nightly, coverage(off))]
mod concurrency;
#[cfg_attr(coverage_nightly, coverage(off))]
mod copy_from;
#[cfg_attr(coverage_nightly, coverage(off))]
mod merge;
#[cfg_attr(coverage_nightly, coverage(off))]
mod recording;
