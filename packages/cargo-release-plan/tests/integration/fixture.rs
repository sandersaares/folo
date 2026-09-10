//! Hermetic Git helpers for cargo-release-plan integration tests.
//!
//! Git configuration is pinned by `Fixture` so tests do not depend on host or
//! user settings.

use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

/// A temporary Git repository that is also a Cargo workspace.
pub(crate) struct Fixture {
    dir: TempDir,
    manifest_path: PathBuf,
}

impl Fixture {
    /// Creates the repository and writes the workspace manifest.
    ///
    /// `extra` is appended to the root manifest, so a caller can add tables such
    /// as `[workspace.dependencies]` or `[workspace.package]`.
    pub(crate) fn new(extra: &str) -> Self {
        let fixture = Self::empty("Cargo.toml");
        fixture.write_workspace(extra);
        fixture
    }

    /// Creates the repository with a workspace manifest at `manifest_path`.
    pub(crate) fn with_workspace_manifest(manifest_path: &str, content: &str) -> Self {
        let fixture = Self::empty(manifest_path);
        if let Some(parent) = fixture.manifest_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&fixture.manifest_path, content).unwrap();
        fixture
    }

    fn empty(manifest_path: &str) -> Self {
        let dir = TempDir::new().unwrap();
        let manifest_path = dir.path().join(manifest_path);
        let fixture = Self { dir, manifest_path };
        fixture.git(&["init", "-b", "main"]);
        fixture
    }

    /// Rewrites the root manifest, replacing the tables `new` appended.
    pub(crate) fn write_workspace(&self, extra: &str) {
        // Ordinary supported manifest revisions so `cargo metadata` accepts the
        // generated workspace. Tests do not cover resolver or edition behavior.
        if let Some(parent) = self.manifest_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(
            &self.manifest_path,
            format!(
                r#"[workspace]
members = ["packages/*"]
resolver = "2"
{extra}
"#
            ),
        )
        .unwrap();
    }

    pub(crate) fn path(&self) -> &Path {
        self.dir.path()
    }

    pub(crate) fn manifest(&self) -> PathBuf {
        self.manifest_path.clone()
    }

    pub(crate) fn write(&self, rel: &str, contents: &str) {
        let path = self.path().join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, contents).unwrap();
    }

    pub(crate) fn read(&self, rel: &str) -> String {
        fs::read_to_string(self.path().join(rel)).unwrap()
    }

    pub(crate) fn git(&self, args: &[&str]) -> String {
        let mut command = hermetic_git();
        command.arg("-C");
        command.arg(self.path());
        command.args(args);
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    pub(crate) fn commit(&self, message: &str) {
        self.git(&["add", "-A"]);
        self.git(&["commit", "-m", message]);
    }

    /// Runs Cargo against the fixture workspace.
    ///
    /// Offline throughout, since the fixture packages never depend on anything
    /// outside the workspace and a registry lookup would make tests non-hermetic.
    pub(crate) fn cargo(&self, args: &[&str]) -> String {
        let output = Command::new("cargo")
            .current_dir(self.path())
            .args(args)
            .arg("--manifest-path")
            .arg(self.manifest())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "cargo {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    pub(crate) fn sha(&self, rev: &str) -> String {
        self.git(&["rev-parse", rev]).trim().to_string()
    }
}

/// Git settings pinned for every invocation.
///
/// No test may inherit host or user configuration: an unset identity, a signing
/// key, or a background `gc` would all make a test depend on the machine it
/// runs on.
const HERMETIC_CONFIG: &[&str] = &[
    "-c",
    "user.email=release-plan@example.invalid",
    "-c",
    "user.name=Release Plan Test",
    "-c",
    "commit.gpgsign=false",
    "-c",
    "gc.auto=0",
    "-c",
    "core.autocrlf=false",
];

/// A Git command that keeps its empty global configuration file alive.
///
/// Git for Windows on ARM64 rejects the `NUL` device as a configuration path, so
/// every command receives a real empty file instead of a platform-specific null
/// device. The owning temporary directory removes the file after the command is
/// dropped.
pub(crate) struct HermeticGit {
    command: Command,
    _global_config_dir: TempDir,
}

impl HermeticGit {
    fn new() -> Self {
        let global_config_dir = TempDir::new().unwrap();
        let global_config = global_config_dir.path().join("config");
        fs::write(&global_config, "").unwrap();

        let mut command = Command::new("git");
        command
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", global_config)
            .env_remove("GIT_CONFIG")
            .env_remove("GIT_CONFIG_COUNT")
            .env_remove("GIT_CONFIG_PARAMETERS");
        command.args(HERMETIC_CONFIG);

        Self {
            command,
            _global_config_dir: global_config_dir,
        }
    }
}

impl Deref for HermeticGit {
    type Target = Command;

    fn deref(&self) -> &Self::Target {
        &self.command
    }
}

impl DerefMut for HermeticGit {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.command
    }
}

/// A `git` command carrying the pinned configuration and no working directory.
///
/// `Fixture::git` runs inside an existing fixture; a test that creates a
/// repository somewhere else, such as a clone, needs the same settings without
/// one.
pub(crate) fn hermetic_git() -> HermeticGit {
    HermeticGit::new()
}

#[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
#[test]
fn hermetic_git_ignores_the_users_global_configuration() {
    let home = tempfile::tempdir().unwrap();
    fs::write(
        home.path().join(".gitconfig"),
        "[core]\nhooksPath = unwanted-hooks\n",
    )
    .unwrap();
    let output = hermetic_git()
        .env("HOME", home.path())
        .args(["config", "--global", "--get", "core.hooksPath"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
}

/// Writes a package whose only target is an executable.
///
/// A binary package releases the dependency closure its archive's lockfile
/// records, which a library package does not.
/// Ref: docs/design.md, "Relevant lockfile closures".
pub(crate) fn write_binary_package(fixture: &Fixture, name: &str, version: &str, extra: &str) {
    write_package(fixture, name, version, extra);
    fs::remove_file(fixture.path().join(format!("packages/{name}/src/lib.rs"))).unwrap();
    fixture.write(&format!("packages/{name}/src/main.rs"), "fn main() {}\n");
}

pub(crate) fn write_package(fixture: &Fixture, name: &str, version: &str, extra: &str) {
    fixture.write(
        &format!("packages/{name}/Cargo.toml"),
        &format!(
            r#"[package]
name = "{name}"
version = "{version}"
edition = "2021"
{extra}
"#
        ),
    );
    fixture.write(&format!("packages/{name}/src/lib.rs"), "pub fn f() {}\n");
}
