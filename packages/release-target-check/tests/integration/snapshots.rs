use std::fs;

use testing::with_watchdog;

use crate::fixture::Fixture;

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn accepts_later_workflow_only_snapshot_without_mutating_inputs() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        fixture.write(".github/workflows/release.yml", "name: updated fixture\n");
        let commit = fixture.commit("workflow maintenance");
        fixture.write("target/ordinary-build-output", "ignored");
        let lockfile = fs::read(fixture.root().join("Cargo.lock")).unwrap();
        let output = fixture
            .verifier(&commit, &commit)
            .args(["--package", "widget@1.0.0", "--verbose"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
        assert_eq!(fixture.head(), commit);
        assert!(fixture.git(&["status", "--porcelain"]).is_empty());
        assert_eq!(
            fs::read(fixture.root().join("Cargo.lock")).unwrap(),
            lockfile
        );
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn rejects_same_version_released_source_change() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        fixture.write("packages/widget/src/lib.rs", "pub fn value() -> u8 { 2 }\n");
        let commit = fixture.commit("unreleased source");
        let output = fixture.verify(&commit, &commit, "widget@1.0.0");
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn rejects_changed_inherited_value_using_the_release_checker() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        fixture.write_workspace("Apache-2.0");
        let commit = fixture.commit("unreleased inherited license");
        assert!(
            !fixture
                .verify(&commit, &commit, "widget@1.0.0")
                .status
                .success()
        );
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn accepts_old_existing_tag_before_main_version_advances() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        let old = fixture.head();
        fixture.git(&["tag", "-a", "widget-v1.0.0", "-m", "existing release"]);
        fixture.write_package("1.1.0", true);
        fixture.write("packages/widget/src/lib.rs", "pub fn value() -> u8 { 2 }\n");
        let main = fixture.commit("next release");
        fixture.git(&["checkout", "--detach", "widget-v1.0.0"]);
        let output = fixture.verify(&old, &main, "widget@1.0.0");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn rejects_requested_version_after_main_advances() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        fixture.write_package("1.1.0", true);
        let main = fixture.commit("next release");
        assert!(
            !fixture
                .verify(&main, &main, "widget@1.0.0")
                .status
                .success()
        );
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn rejects_mismatched_head_and_noncommit_object_ids() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        let old = fixture.head();
        fixture.write(".github/workflows/release.yml", "name: updated fixture\n");
        let main = fixture.commit("workflow maintenance");
        assert!(!fixture.verify(&old, &main, "widget@1.0.0").status.success());
        fixture.git(&["tag", "-a", "release", "-m", "annotated"]);
        let tag = fixture.git(&["rev-parse", "release"]);
        assert!(
            !fixture
                .verify(&main, tag.trim(), "widget@1.0.0")
                .status
                .success()
        );
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn rejects_dirty_staged_and_untracked_source() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        let commit = fixture.head();
        fixture.write("packages/widget/src/lib.rs", "pub fn value() -> u8 { 2 }\n");
        assert!(
            !fixture
                .verify(&commit, &commit, "widget@1.0.0")
                .status
                .success()
        );
        fixture.git(&["add", "packages/widget/src/lib.rs"]);
        assert!(
            !fixture
                .verify(&commit, &commit, "widget@1.0.0")
                .status
                .success()
        );
        fixture.write("packages/widget/src/lib.rs", "pub fn value() -> u8 { 1 }\n");
        fixture.git(&["add", "packages/widget/src/lib.rs"]);
        fixture.write(
            "packages/widget/src/untracked.rs",
            "pub struct Unreleased;\n",
        );
        assert!(
            !fixture
                .verify(&commit, &commit, "widget@1.0.0")
                .status
                .success()
        );
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn rejects_index_flags_that_conceal_source_changes() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        let commit = fixture.head();
        fixture.git(&[
            "update-index",
            "--assume-unchanged",
            "packages/widget/src/lib.rs",
        ]);
        fixture.write("packages/widget/src/lib.rs", "pub fn value() -> u8 { 2 }\n");
        assert!(
            !fixture
                .verify(&commit, &commit, "widget@1.0.0")
                .status
                .success()
        );
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn rejects_side_branch_even_after_a_merge_to_main() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        fixture.git(&["checkout", "-b", "feature"]);
        fixture.write("feature-notes", "not released\n");
        let candidate = fixture.commit("feature work");
        fixture.git(&["checkout", "main"]);
        fixture.git(&["merge", "--no-ff", "feature", "-m", "merge feature"]);
        let main = fixture.head();
        fixture.git(&["checkout", "--detach", &candidate]);
        assert!(
            !fixture
                .verify(&candidate, &main, "widget@1.0.0")
                .status
                .success()
        );
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn rejects_missing_and_nonpublishable_packages() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        let commit = fixture.head();
        assert!(
            !fixture
                .verify(&commit, &commit, "absent@1.0.0")
                .status
                .success()
        );
        fixture.write_package("1.0.0", false);
        let commit = fixture.commit("private package");
        assert!(
            !fixture
                .verify(&commit, &commit, "widget@1.0.0")
                .status
                .success()
        );
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn rejects_duplicate_and_empty_package_selection_at_executable_boundary() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        let commit = fixture.head();
        let output = fixture
            .verifier(&commit, &commit)
            .args(["--package", "widget@1.0.0", "--package", "widget@1.0.0"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            !fixture
                .verifier(&commit, &commit)
                .output()
                .unwrap()
                .status
                .success()
        );
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn missing_lockfile_is_not_generated_as_a_repair() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        fixture.write("packages/widget/src/main.rs", "fn main() {}\n");
        fixture.git(&["rm", "Cargo.lock"]);
        let commit = fixture.commit("missing lockfile");
        assert!(
            !fixture
                .verify(&commit, &commit, "widget@1.0.0")
                .status
                .success()
        );
        assert!(!fixture.root().join("Cargo.lock").exists());
        assert!(fixture.git(&["status", "--porcelain"]).is_empty());
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git and Cargo against filesystem fixtures")]
fn library_only_workspace_does_not_require_or_generate_a_lockfile() {
    with_watchdog(|| {
        let fixture = Fixture::new();
        fixture.git(&["rm", "Cargo.lock"]);
        let commit = fixture.commit("library without lockfile");
        let output = fixture.verify(&commit, &commit, "widget@1.0.0");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!fixture.root().join("Cargo.lock").exists());
    });
}
