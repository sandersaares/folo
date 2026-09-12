pub(crate) use input::read_report;
pub(crate) use write::*;

mod input;
mod write;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
pub(crate) mod fixture;
