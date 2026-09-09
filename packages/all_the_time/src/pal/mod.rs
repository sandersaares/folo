//! Platform abstraction layer for processor time tracking.
//!
//! This module provides a platform abstraction that allows switching between
//! real processor time tracking (using the `cpu_time` package) and fake implementations
//! for testing purposes.

mod abstractions;
mod facade;
#[cfg(test)]
mod fake;
mod real;

pub(crate) use abstractions::*;
pub(crate) use facade::*;
#[cfg(test)]
pub(crate) use fake::*;
