//! Tests for thread-safe event behavior.

#[cfg(debug_assertions)]
mod diagnostics;
mod lifecycle;
mod races;
mod reentrancy;
mod support;

pub(crate) use support::*;
