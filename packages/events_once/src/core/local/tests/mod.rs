//! Tests for thread-local event behavior.

#[cfg(debug_assertions)]
mod diagnostics;
mod lifecycle;
mod reentrancy;
mod support;

pub(crate) use support::*;
