// Deliberately failing input for capturing the pinned Miri/libtest diagnostic format.
// This standalone fixture is not a workspace test and requires no external services.
#[test]
fn fixture_failure() {
    panic!("scheduled Miri fixture canary");
}
