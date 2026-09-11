#![allow(
    clippy::indexing_slicing,
    reason = "fixed synthetic JSON fixtures make missing protocol fields test failures"
)]

mod analysis;
mod basis;
mod comparison;
mod lifecycle;
mod support;
