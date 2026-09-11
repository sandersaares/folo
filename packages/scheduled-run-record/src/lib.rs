//! Pure JSON record operations shared by scheduled automation controllers.
//!
//! Callers establish API ownership and provenance before supplying observations.
//! Operations return prepared or restored records, never external-write authority.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub use console::run;
pub use protocol::execute;

mod canonical;
mod console;
mod document;
mod evidence;
mod gap_paths;
mod pages;
mod protocol;
mod record;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod invalid_evidence_tests;
