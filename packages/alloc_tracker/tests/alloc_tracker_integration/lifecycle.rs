//! Session and report lifecycle: what counts as an empty session and when spans change that.

use alloc_tracker::Session;

#[test]
#[cfg_attr(miri, ignore)] // Test uses the real platform which cannot be executed under Miri.
fn no_span_is_empty_session() {
    let session = Session::new().no_stdout().no_file();

    let _op = session.operation("test_no_span");

    assert!(session.is_empty());
}

#[test]
#[cfg_attr(miri, ignore)] // Test uses the real platform which cannot be executed under Miri.
fn span_with_no_allocation_is_not_empty_session() {
    let session = Session::new().no_stdout().no_file();

    {
        let op = session.operation("test_no_allocation");
        drop(op.measure_process().iterations(1));
    } // op is dropped here, merging data to session

    assert!(!session.is_empty());
}

#[test]
#[cfg_attr(miri, ignore)] // Test uses the real platform which cannot be executed under Miri.
fn report_is_empty_matches_session_is_empty() {
    let session = Session::new().no_stdout().no_file();

    // Test 1: Both empty initially
    let report = session.to_report();
    assert_eq!(session.is_empty(), report.is_empty());
    assert!(session.is_empty());
    assert!(report.is_empty());

    // Test 2: Create operation without spans - both should still be empty
    let _operation = session.operation("test");
    let report = session.to_report();
    assert_eq!(session.is_empty(), report.is_empty());
    assert!(session.is_empty());
    assert!(report.is_empty());

    // Test 3: Add spans - both should be non-empty
    {
        let operation = session.operation("test_with_spans");
        let _span = operation.measure_process().iterations(1);
        // No actual allocation needed for span to exist
    } // Operation is dropped here, merging data to session

    let report = session.to_report();
    assert_eq!(session.is_empty(), report.is_empty());
    assert!(!session.is_empty());
    assert!(!report.is_empty());
}
