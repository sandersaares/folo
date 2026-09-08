//! Thread-safe one-shot event implementation.

mod event;
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

pub use event::*;
