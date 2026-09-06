//! Process PAL.

mod abstractions;
mod command_line;
mod facade;
// Turning a command into an executable image is Windows path and search-order
// mechanics, so it stays beside the implementation that spawns with the result.
#[cfg_attr(coverage_nightly, coverage(off))]
mod resolve;
// The Windows PAL is the operating-system boundary: a thin binding layer over
// Win32 whose failure paths need real operating-system faults to reach. It is
// exercised end to end by the integration tests rather than line by line, and
// is excluded from mutation testing for the same reason (scripts/build/Mutants.psm1).
#[cfg_attr(coverage_nightly, coverage(off))]
mod windows;

pub(crate) use abstractions::*;
pub(crate) use facade::*;
pub(crate) use resolve::{HowResolved, ResolvedCommand};
// Job topology is only chosen explicitly by the integration harness; production
// logic asks for the standard lifetime job and never names a policy.
#[cfg(any(test, feature = "private-test-util"))]
pub(crate) use windows::Breakaway;
pub(crate) use windows::BuildTargetProcesses;
