use std::fs;
use std::path::Path;
use std::process::Command;

use tempfile::{Builder, TempDir};
use testing::with_watchdog;

use super::*;

#[test]
#[cfg_attr(miri, ignore = "Executes Git against filesystem fixtures")]
fn rejects_evidence_changed_during_successful_or_failed_operations() {
    with_watchdog(|| {
        let (_directory, repository) = fixture();
        repository.checked(|| Ok(())).unwrap();
        for succeeds in [true, false] {
            let result = repository.checked(|| {
                fs::write(repository.root.join("Cargo.toml"), "changed input").unwrap();
                if succeeds {
                    Ok(())
                } else {
                    Err(OperationError::new().into())
                }
            });
            let error = result.unwrap_err();
            assert!(error.find_source::<VerificationError>().is_some());
            assert!(error.find_source::<OperationError>().is_none());
            fs::write(repository.root.join("Cargo.toml"), "tracked input").unwrap();
        }
    });
}

#[test]
#[cfg_attr(miri, ignore = "Executes Git against filesystem fixtures")]
fn rejects_head_changed_during_verification() {
    with_watchdog(|| {
        let (directory, repository) = fixture();
        let error = repository
            .checked(|| {
                command(
                    &directory.path().join("repository"),
                    &["commit", "--allow-empty", "-m", "new head"],
                );
                Ok(())
            })
            .unwrap_err();
        assert!(error.find_source::<VerificationError>().is_some());
    });
}

fn fixture() -> (TempDir, Repository) {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("fixtures");
    fs::create_dir_all(&fixtures).unwrap();
    let directory = Builder::new()
        .prefix("evidence-")
        .tempdir_in(fixtures)
        .unwrap();
    let root = directory.path().join("repository");
    fs::create_dir_all(&root).unwrap();
    fs::write(directory.path().join("global-config"), "").unwrap();
    command(&root, &["init", "-b", "main"]);
    fs::write(root.join("Cargo.toml"), "tracked input").unwrap();
    command(&root, &["add", "-A"]);
    command(&root, &["commit", "-m", "initial"]);
    let head = command(&root, &["rev-parse", "HEAD"]).trim().to_owned();
    let repository = Repository::discover(&root.join("Cargo.toml"), &head).unwrap();
    (directory, repository)
}

fn command(root: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .args([
            "-c",
            "user.name=Release Target Test",
            "-c",
            "user.email=release-target@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "gc.auto=0",
            "-c",
            "core.autocrlf=false",
        ])
        .args(arguments)
        .current_dir(root)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env(
            "GIT_CONFIG_GLOBAL",
            root.parent().unwrap().join("global-config"),
        )
        .env_remove("GIT_CONFIG_COUNT")
        .env_remove("GIT_CONFIG_PARAMETERS")
        .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

/// Models a failed verification subprocess without running or replacing Cargo.
#[ohno::error]
struct OperationError;
