//! Shared real-Git fixtures for repository and metadata boundary unit tests.

use std::fs;
use std::path::Path;
use std::process::Command;

use tempfile::TempDir;

use crate::repository::Repository;

pub(crate) fn fixture() -> (TempDir, Repository) {
    // Native temporary storage keeps subprocess-heavy fixtures off cross-platform mounts.
    let directory = TempDir::new().unwrap();
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

pub(crate) fn command(root: &Path, arguments: &[&str]) -> String {
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
        // Resolve from the child working directory instead of passing Windows verbatim-path
        // syntax through Git's configuration-file environment variable.
        .env("GIT_CONFIG_GLOBAL", Path::new("..").join("global-config"))
        .env_remove("GIT_CONFIG_COUNT")
        .env_remove("GIT_CONFIG_PARAMETERS")
        // Fixed timestamps keep fixture history independent of the wall clock.
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
