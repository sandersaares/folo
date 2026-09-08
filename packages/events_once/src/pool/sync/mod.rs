//! Thread-safe event pool implementation.

mod pool;
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

pub use pool::*;
