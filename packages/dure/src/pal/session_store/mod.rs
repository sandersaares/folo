//! Session store PAL.

mod abstractions;
mod facade;
mod fs_store;
// A fake used only to drive unit tests, so it is test infrastructure rather
// than product code and carries no coverage expectations of its own.
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod memory;

pub(crate) use abstractions::*;
pub(crate) use facade::*;
pub(crate) use fs_store::*;
#[cfg(test)]
pub(crate) use memory::*;
