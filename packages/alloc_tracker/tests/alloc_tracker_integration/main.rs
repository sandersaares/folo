//! Integration tests for `alloc_tracker` with real memory allocations.
//!
//! These tests install the package's allocator as the global allocator, which a process can
//! only do once, so every scenario that needs real allocation lives in this one binary. The
//! scenarios are grouped by the behavior they characterize; this root holds only the
//! allocator and the helpers that read results the way a user does.

use alloc_tracker::{Allocator, ReportOperation, Session};

#[global_allocator]
static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();

/// Reads an operation's total bytes allocated the way a user inspects results:
/// through the session report rather than the live `Operation` handle.
fn report_total_bytes(session: &Session, operation_name: &str) -> u64 {
    report_operation(
        session,
        operation_name,
        ReportOperation::total_bytes_allocated,
    )
}

/// Reads an operation's peak the way a user inspects results: through the session report.
fn report_peak(session: &Session, operation_name: &str) -> Option<f64> {
    report_operation(
        session,
        operation_name,
        ReportOperation::peak_outstanding_bytes,
    )
}

fn report_operation<R>(
    session: &Session,
    operation_name: &str,
    read: impl FnOnce(&ReportOperation) -> R,
) -> R {
    let report = session.to_report();
    let (_, operation) = report
        .operations()
        .find(|&(name, _)| name == operation_name)
        .unwrap();

    read(operation)
}

mod cumulative;
mod lifecycle;
mod multithreading;
mod peak;
