use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use tempfile::TempDir;

/// Owns a small repository and isolated Git configuration for executable-boundary tests.
pub(crate) struct Fixture {
    directory: TempDir,
    root: PathBuf,
}

impl Fixture {
    pub(crate) fn new() -> Self {
        // Use an owned native temporary directory: Git-heavy fixtures on a Windows checkout
        // mounted into WSL otherwise pay cross-filesystem overhead for every subprocess.
        let directory = TempDir::new().unwrap();
        fs::write(directory.path().join("global-config"), "").unwrap();
        let root = directory.path().join("repository");
        fs::create_dir_all(&root).unwrap();
        let fixture = Self { directory, root };
        fixture.git(&["init", "-b", "main", "--object-format=sha1"]);
        for (name, value) in [
            ("user.name", "Release Target Test"),
            ("user.email", "release-target@example.invalid"),
            ("commit.gpgsign", "false"),
            ("tag.gpgsign", "false"),
            ("gc.auto", "0"),
            ("core.autocrlf", "false"),
        ] {
            fixture.git(&["config", name, value]);
        }
        fixture.write(".gitignore", "target/\n");
        fixture.write(".github/workflows/release.yml", "name: fixture\n");
        fixture.write_workspace("MIT");
        fixture.write_package("1.0.0", true);
        fixture.write("packages/widget/src/lib.rs", "pub fn value() -> u8 { 1 }\n");
        fixture.commit("initial release");
        fixture
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn write_workspace(&self, license: &str) {
        // Ordinary Cargo versions; the fixtures exercise release policy, not resolver behavior.
        self.write(
            "Cargo.toml",
            &format!(
                "[workspace]\nmembers = [\"packages/*\"]\nresolver = \"2\"\n\
                 [workspace.package]\nlicense = \"{license}\"\n"
            ),
        );
    }

    pub(crate) fn write_package(&self, version: &str, publish: bool) {
        self.write(
            "packages/widget/Cargo.toml",
            &format!(
                "[package]\nname = \"widget\"\nversion = \"{version}\"\nedition = \"2021\"\n\
                 license.workspace = true\npublish = {publish}\ninclude = [\"src/**\"]\n"
            ),
        );
        self.write(
            "Cargo.lock",
            &format!("version = 4\n\n[[package]]\nname = \"widget\"\nversion = \"{version}\"\n"),
        );
    }

    pub(crate) fn write(&self, relative: &str, text: &str) {
        let path = self.root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }

    pub(crate) fn commit(&self, message: &str) -> String {
        self.git(&["add", "-A"]);
        self.git(&["commit", "-m", message]);
        self.head()
    }

    pub(crate) fn head(&self) -> String {
        self.git(&["rev-parse", "HEAD"]).trim().to_owned()
    }

    pub(crate) fn git(&self, arguments: &[&str]) -> String {
        let output = self.command("git").args(arguments).output().unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap()
    }

    pub(crate) fn verify(&self, commit: &str, release_line: &str, package: &str) -> Output {
        self.verifier(commit, release_line)
            .args(["--package", package])
            .output()
            .unwrap()
    }

    pub(crate) fn verifier(&self, commit: &str, release_line: &str) -> Command {
        let mut command = self.command(env!("CARGO_BIN_EXE_release-target-check"));
        command
            .arg("--manifest-path")
            .arg(self.root.join("Cargo.toml"))
            .args(["--commit", commit, "--release-line", release_line]);
        command
    }

    fn command(&self, executable: &str) -> Command {
        let mut command = Command::new(executable);
        command
            .current_dir(&self.root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env(
                "GIT_CONFIG_GLOBAL",
                self.directory.path().join("global-config"),
            )
            .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z")
            .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z")
            .env("CARGO_NET_OFFLINE", "true");
        for name in [
            "GIT_CONFIG",
            "GIT_CONFIG_COUNT",
            "GIT_CONFIG_PARAMETERS",
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
        ] {
            command.env_remove(name);
        }
        command
    }
}
