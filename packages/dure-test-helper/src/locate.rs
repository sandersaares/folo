//! On-demand build and path resolution for the helper binary.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;
use std::{env, str};

/// Returns the absolute path to the freshly built `dure-test-helper` binary.
///
/// The binary is built on demand — a fast no-op once it is up to date — so a
/// test never spawns a stale helper, and the path is resolved once per process.
/// A test runner can provide a prebuilt binary through `DURE_TEST_HELPER` so no
/// build runs after the tests start.
///
/// # Panics
///
/// Panics when a configured helper path is invalid, when the helper cannot be
/// built, or when Cargo reports no executable for it. Each means the integration
/// tests have nothing to drive, so failing loudly beats spawning something else.
#[must_use]
pub fn binary_path() -> &'static Path {
    static PATH: LazyLock<PathBuf> = LazyLock::new(locate_or_build);
    PATH.as_path()
}

/// Selects a prebuilt helper when available, otherwise builds it on demand.
fn locate_or_build() -> PathBuf {
    resolve(
        env::var_os("DURE_TEST_HELPER"),
        Path::is_file,
        run_cargo_build,
    )
}

/// Resolves a prebuilt helper or invokes `build` to create one.
fn resolve(
    prebuilt: Option<OsString>,
    is_file: impl FnOnce(&Path) -> bool,
    build: impl FnOnce() -> BuildOutput,
) -> PathBuf {
    if let Some(path) = prebuilt {
        let path = PathBuf::from(path);
        assert!(
            path.is_absolute(),
            "DURE_TEST_HELPER must be an absolute path: {}",
            path.display()
        );
        assert!(
            is_file(&path),
            "DURE_TEST_HELPER does not name an existing file: {}",
            path.display()
        );
        // Kept as a path rather than narrowed to a `String`: a Windows path may
        // hold an unpaired surrogate, and the lossy form names a different file
        // than the one just validated.
        return path;
    }
    interpret_build(&build())
}

/// The outcome of spawning `cargo build`, reduced to the fields the resolver
/// inspects.
///
/// Modeling it as a plain struct rather than [`std::process::Output`] keeps
/// [`interpret_build`] unit-testable: an `ExitStatus` cannot be constructed
/// portably, but these fields can be filled in directly to exercise every
/// branch without spawning a real process.
struct BuildOutput {
    /// Whether the build process exited successfully.
    success: bool,
    /// The process exit code, if it terminated normally with one.
    code: Option<i32>,
    /// The captured standard output (Cargo's JSON build messages).
    stdout: Vec<u8>,
    /// The captured standard error (Cargo's rendered diagnostics).
    stderr: Vec<u8>,
}

/// Spawns `cargo build` for this crate and captures its outcome.
fn run_cargo_build() -> BuildOutput {
    // `--manifest-path` (absolute, derived from this crate's own manifest
    // directory) makes the build independent of the current working directory,
    // which the integration tests change per scenario. `--locked` refuses to
    // silently rewrite the lockfile.
    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let output = Command::new(env!("CARGO"))
        .args([
            "build",
            "--locked",
            "--message-format=json-render-diagnostics",
            "--manifest-path",
        ])
        .arg(&manifest_path)
        .arg("--target-dir")
        .arg(helper_target_dir(env::var_os("CARGO_TARGET_DIR")))
        .output()
        .expect("spawning `cargo build` for dure-test-helper should succeed");
    BuildOutput {
        success: output.status.success(),
        code: output.status.code(),
        stdout: output.stdout,
        stderr: output.stderr,
    }
}

/// The dedicated target directory the on-demand `cargo build` writes into.
///
/// A build into the shared workspace tree would contend with whatever Cargo
/// invocation is already running the tests: Cargo's per-directory build lock is
/// held by that outer invocation, and its binary swap (unlink, then hard-link)
/// briefly exposes a missing file to anything reading the tree. A private
/// directory means only builds of this crate ever touch it, so neither applies.
/// The same reasoning gives `cargo-bench-history-faker` its own tree.
///
/// It nests under the active target directory — the ambient `CARGO_TARGET_DIR`
/// when one is set, as coverage runs do, otherwise the workspace `target/` — so
/// it stays git-ignored and is removed by `cargo clean`. A *relative* ambient
/// value is resolved against the workspace root rather than the process working
/// directory, because callers change directories between scenarios. The ambient
/// value is taken as a parameter rather than read here so the mapping stays
/// unit-testable without mutating process-wide environment.
fn helper_target_dir(ambient_target_dir: Option<OsString>) -> PathBuf {
    // The workspace root, two levels above this crate's own manifest
    // (`<root>/packages/dure-test-helper`).
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    let base = ambient_target_dir.map_or_else(
        || workspace_root.join("target"),
        |configured| {
            let configured = PathBuf::from(configured);
            if configured.is_absolute() {
                configured
            } else {
                workspace_root.join(configured)
            }
        },
    );
    base.join("dure-test-helper")
}

/// Reads the built binary's path from Cargo's JSON build output, panicking with
/// the captured diagnostics (exit code, stdout, and stderr) when the build
/// failed or reported no executable.
fn interpret_build(output: &BuildOutput) -> PathBuf {
    assert!(
        output.success,
        "building dure-test-helper failed (exit code: {}):\nstdout:\n{}\nstderr:\n{}",
        output
            .code
            .map_or_else(|| "unknown".to_owned(), |code| code.to_string()),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout =
        str::from_utf8(&output.stdout).expect("cargo build JSON output should be valid UTF-8");
    for line in stdout.lines() {
        let Ok(message) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let is_artifact =
            message.get("reason").and_then(serde_json::Value::as_str) == Some("compiler-artifact");
        let is_helper = message
            .get("target")
            .and_then(|target| target.get("name"))
            .and_then(serde_json::Value::as_str)
            == Some("dure-test-helper");
        if is_artifact
            && is_helper
            && let Some(executable) = message
                .get("executable")
                .and_then(serde_json::Value::as_str)
        {
            return PathBuf::from(executable);
        }
    }
    panic!("cargo build did not report an executable path for dure-test-helper");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A single `compiler-artifact` JSON line for the named target with the
    /// given executable path, matching the shape Cargo emits.
    fn artifact_line(target_name: &str, executable: &str) -> String {
        serde_json::json!({
            "reason": "compiler-artifact",
            "target": { "name": target_name },
            "executable": executable,
        })
        .to_string()
    }

    /// A successful build whose stdout is `stdout`.
    fn ok_build(stdout: String) -> BuildOutput {
        BuildOutput {
            success: true,
            code: Some(0),
            stdout: stdout.into_bytes(),
            stderr: Vec::new(),
        }
    }

    /// A failed build carrying a known exit code and stderr, for the
    /// failure-path assertions.
    fn failed_build() -> BuildOutput {
        BuildOutput {
            success: false,
            code: Some(101),
            stdout: b"some compiler chatter".to_vec(),
            stderr: b"error: linker exploded".to_vec(),
        }
    }

    #[test]
    fn picks_the_executable_of_the_bin_artifact() {
        let resolved = interpret_build(&ok_build(artifact_line(
            "dure-test-helper",
            "/tmp/dure-test-helper",
        )));
        assert_eq!(resolved, Path::new("/tmp/dure-test-helper"));
    }

    #[test]
    fn ignores_non_json_lines_other_packages_and_the_lib_artifact() {
        // The lib artifact has no executable; an unrelated package and a stray
        // non-JSON line must both be skipped before the helper bin is found.
        let lib_artifact = serde_json::json!({
            "reason": "compiler-artifact",
            "target": { "name": "dure-test-helper" },
            "executable": serde_json::Value::Null,
        })
        .to_string();
        let stdout = format!(
            "{}\n{lib_artifact}\nthis is not json\n{}\n",
            artifact_line("serde_json", "/tmp/serde_json"),
            artifact_line("dure-test-helper", "/tmp/dure-test-helper"),
        );

        assert_eq!(
            interpret_build(&ok_build(stdout)),
            Path::new("/tmp/dure-test-helper")
        );
    }

    #[test]
    #[should_panic(expected = "did not report an executable path")]
    fn panics_when_no_executable_is_reported() {
        let lib_only = serde_json::json!({
            "reason": "compiler-artifact",
            "target": { "name": "dure-test-helper" },
            "executable": serde_json::Value::Null,
        })
        .to_string();
        drop(interpret_build(&ok_build(lib_only)));
    }

    #[test]
    #[should_panic(expected = "exit code: 101")]
    fn build_failure_reports_the_exit_code() {
        drop(interpret_build(&failed_build()));
    }

    #[test]
    #[should_panic(expected = "error: linker exploded")]
    fn build_failure_reports_stderr() {
        drop(interpret_build(&failed_build()));
    }

    #[test]
    #[should_panic(expected = "exit code: unknown")]
    fn build_failure_without_an_exit_code_reports_unknown() {
        let signalled = BuildOutput {
            success: false,
            code: None,
            stdout: Vec::new(),
            stderr: b"killed by signal".to_vec(),
        };
        drop(interpret_build(&signalled));
    }

    #[test]
    #[should_panic(expected = "valid UTF-8")]
    fn panics_on_non_utf8_output() {
        let garbled = BuildOutput {
            success: true,
            code: Some(0),
            stdout: vec![0xff, 0xfe, 0xfd],
            stderr: Vec::new(),
        };
        drop(interpret_build(&garbled));
    }

    #[test]
    fn target_dir_uses_an_absolute_ambient_target_dir_as_is() {
        let ambient = Path::new(env!("CARGO_MANIFEST_DIR")).join("custom-target");
        assert!(ambient.is_absolute());
        let dir = helper_target_dir(Some(ambient.clone().into_os_string()));
        assert_eq!(dir, ambient.join("dure-test-helper"));
    }

    #[test]
    fn target_dir_absolutizes_a_relative_ambient_target_dir_against_the_workspace() {
        let dir = helper_target_dir(Some(OsString::from("rel-target")));
        let expected = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("rel-target")
            .join("dure-test-helper");
        assert_eq!(dir, expected);
    }

    #[test]
    fn target_dir_defaults_under_the_workspace_target() {
        let dir = helper_target_dir(None);
        let expected = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("target")
            .join("dure-test-helper");
        assert_eq!(dir, expected);
        assert!(
            dir.is_absolute(),
            "the default target dir should be absolute: {dir:?}"
        );
    }

    #[test]
    fn a_prebuilt_helper_skips_the_build() {
        let helper = PathBuf::from(r"C:\target\dure-test-helper.exe");

        let resolved = resolve(
            Some(helper.clone().into_os_string()),
            |_path| true,
            || panic!("a prebuilt helper must skip the build"),
        );

        assert_eq!(resolved, helper);
    }

    #[test]
    #[should_panic(expected = "does not name an existing file")]
    fn a_missing_prebuilt_helper_is_rejected() {
        drop(resolve(
            Some(OsString::from(r"C:\target\missing-helper.exe")),
            |_path| false,
            || panic!("a configured helper must skip the build"),
        ));
    }

    #[test]
    #[should_panic(expected = "must be an absolute path")]
    fn a_relative_prebuilt_helper_is_rejected() {
        drop(resolve(
            Some(OsString::from("dure-test-helper.exe")),
            |_path| true,
            || panic!("a configured helper must skip the build"),
        ));
    }

    // Drives the real build seam rather than an injected fake: it spawns
    // `cargo build` when no prebuilt path is present and reads the filesystem,
    // so it is native-only.
    #[cfg(not(miri))]
    #[test]
    fn binary_path_returns_an_existing_absolute_file() {
        let resolved = Path::new(binary_path());
        assert!(
            resolved.is_absolute(),
            "binary_path should be absolute: {resolved:?}"
        );
        assert!(
            resolved.is_file(),
            "binary_path should name an existing file: {resolved:?}"
        );
    }
}
