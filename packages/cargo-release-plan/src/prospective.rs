// Disposable local workspaces isolate offline resolution from the live checkout.

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use ohno::AppError;

use crate::command::{run_capture, run_capture_input, run_capture_ok, run_capture_os};
use crate::metadata::load_tracked_work_tree;
use crate::resolved::{Artifact, Inputs, relative};
use crate::verbose::Verbose;
use crate::{ReadFileError, WriteFileError, quote_path};

/// Owns a clone until it is discarded or retained for final compatibility evidence.
pub(crate) struct Prospective {
    pub(crate) root: PathBuf,
    pub(crate) manifest: PathBuf,
    retained: bool,
}

/// Marker stored outside the candidate's tracked files to identify tool-owned workspaces.
const EVIDENCE_MARKER: &str = ".git/cargo-release-plan-preview";

impl Prospective {
    #[expect(
        clippy::create_dir,
        reason = "creation must fail when another invocation owns the path"
    )]
    pub(crate) fn new(output: &Path, inputs: &Inputs) -> Result<Self, AppError> {
        fs::create_dir_all(output).map_err(|error| WriteFileError::caused_by(output, error))?;
        let root = output.join(".prospective");
        // Claim ownership atomically so a concurrent invocation cannot lose its clone to Drop.
        fs::create_dir(&root).map_err(|error| WriteFileError::caused_by(&root, error))?;
        let prospective = Self {
            manifest: root.join(&inputs.manifest),
            root,
            retained: false,
        };
        _ = run_capture_os(
            "git",
            [
                OsStr::new("clone"),
                OsStr::new("--quiet"),
                OsStr::new("--shared"),
                OsStr::new("--no-checkout"),
                OsStr::new("--"),
                inputs.root().as_os_str(),
                prospective.root.as_os_str(),
            ],
            inputs.root(),
        )?;
        for key in [
            "core.autocrlf",
            "core.eol",
            "core.filemode",
            "core.ignorecase",
        ] {
            if let Some(value) = run_capture_ok("git", &["config", "--get", key], inputs.root())? {
                _ = run_capture("git", &["config", key, value.trim()], &prospective.root)?;
            }
        }
        _ = run_capture(
            "git",
            &["update-ref", "HEAD", &inputs.head],
            &prospective.root,
        )?;
        // Tree objects omit intent-to-add entries. Reconstruct the captured index entries
        // directly so every tracked input participates in prospective classification.
        _ = run_capture("git", &["read-tree", "--empty"], &prospective.root)?;
        _ = run_capture_input(
            "git",
            &["update-index", "-z", "--index-info"],
            inputs.index().as_bytes(),
            &prospective.root,
        )?;
        for path in &inputs.paths {
            let source = inputs.root().join(path);
            if !source.exists() {
                continue;
            }
            let destination = prospective.root.join(path);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)
                    .map_err(|error| WriteFileError::caused_by(parent, error))?;
            }
            fs::copy(&source, &destination)
                .map_err(|error| WriteFileError::caused_by(&destination, error))?;
        }
        Ok(prospective)
    }

    pub(crate) fn retain(mut self, output: &Path, owner: &Path) -> Result<PathBuf, AppError> {
        let manifest = relative(&self.root, &self.manifest)?;
        let destination = output.join("workspace");
        let owner = owner.to_string_lossy();
        if destination.exists() {
            let marker = destination.join(EVIDENCE_MARKER);
            if fs::read_to_string(&marker).ok().as_deref() != Some(owner.as_ref()) {
                return Err(EvidenceWorkspaceOccupied::new().into());
            }
            fs::remove_dir_all(&destination)
                .map_err(|error| WriteFileError::caused_by(&destination, error))?;
        }
        let marker = self.root.join(EVIDENCE_MARKER);
        fs::write(&marker, owner.as_bytes())
            .map_err(|error| WriteFileError::caused_by(&marker, error))?;
        fs::rename(&self.root, &destination)
            .map_err(|error| WriteFileError::caused_by(&destination, error))?;
        self.retained = true;
        Ok(destination.join(manifest))
    }

    pub(crate) fn resolve(&self, verbose: Verbose) -> Result<(), AppError> {
        verbose.note(|| {
            format!(
                "resolving {} with cargo update --offline --workspace before release decisions; \
             existing third-party locks are retained where Cargo permits, but dependency edges \
             can be reselected and must be classified",
                quote_path(&self.manifest.to_string_lossy())
            )
        });
        _ = run_capture_os(
            "cargo",
            [
                OsStr::new("update"),
                OsStr::new("--offline"),
                OsStr::new("--workspace"),
                OsStr::new("--manifest-path"),
                self.manifest.as_os_str(),
            ],
            self.manifest.parent().expect("a manifest has a parent"),
        )?;
        Ok(())
    }

    pub(crate) fn artifacts(&self, inputs: &Inputs) -> Result<Vec<Artifact>, AppError> {
        let (work_tree, _) = load_tracked_work_tree(&self.manifest)?;
        let mut paths: BTreeSet<PathBuf> = work_tree.member_manifests.into_iter().collect();
        paths.insert(work_tree.workspace_root.join("Cargo.toml"));
        paths.insert(work_tree.workspace_root.join("Cargo.lock"));
        let mut artifacts = Vec::new();
        for path in paths {
            let relative = relative(&self.root, &path)?;
            let contents = fs::read_to_string(&path)
                .map_err(|error| ReadFileError::caused_by(&path, error))?;
            if fs::read_to_string(inputs.root().join(&relative))
                .ok()
                .as_ref()
                != Some(&contents)
            {
                artifacts.push(Artifact {
                    path: relative,
                    contents,
                });
            }
        }
        Ok(artifacts)
    }
}

impl Drop for Prospective {
    fn drop(&mut self) {
        // A resolver error remains the useful diagnostic even if cleanup also fails.
        if !self.retained {
            _ = fs::remove_dir_all(&self.root);
        }
    }
}

/// A preview may replace only workspaces it previously created.
#[ohno::error]
#[display(
    "the retained workspace path is not owned by this preview; choose another output directory"
)]
struct EvidenceWorkspaceOccupied;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn candidate(root: &Path) -> Prospective {
        fs::create_dir_all(root).unwrap();
        let manifest = root.join("nested/Cargo.toml");
        fs::create_dir_all(manifest.parent().unwrap()).unwrap();
        fs::write(&manifest, "captured manifest").unwrap();
        Prospective {
            root: root.to_path_buf(),
            manifest,
            retained: false,
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "moves an owned filesystem fixture")]
    fn retain_preserves_the_manifest_location_and_replaces_only_its_owner() {
        let directory = tempdir().unwrap();
        let output = directory.path().join("output");
        fs::create_dir_all(&output).unwrap();
        let owner = directory.path().join("original");
        let prospective = candidate(&directory.path().join("candidate"));
        fs::create_dir_all(prospective.root.join(".git")).unwrap();
        let manifest = prospective.retain(&output, &owner).unwrap();
        assert_eq!(manifest, output.join("workspace/nested/Cargo.toml"));
        assert_eq!(fs::read_to_string(&manifest).unwrap(), "captured manifest");
        assert_eq!(
            fs::read_to_string(output.join("workspace").join(EVIDENCE_MARKER)).unwrap(),
            owner.to_string_lossy()
        );

        let prospective = candidate(&directory.path().join("replacement"));
        fs::create_dir_all(prospective.root.join(".git")).unwrap();
        prospective.retain(&output, &owner).unwrap();
        let prospective = candidate(&directory.path().join("foreign"));
        let error = prospective
            .retain(&output, &directory.path().join("another-owner"))
            .unwrap_err();
        assert!(error.find_source::<EvidenceWorkspaceOccupied>().is_some());
        assert_eq!(fs::read_to_string(manifest).unwrap(), "captured manifest");
    }

    #[test]
    #[cfg_attr(miri, ignore = "exercises owned filesystem failure paths")]
    fn retaining_failure_cleans_the_candidate_without_creating_evidence() {
        let directory = tempdir().unwrap();
        let owner = directory.path().join("original");
        for marker_available in [false, true] {
            let root = directory.path().join("candidate");
            let prospective = candidate(&root);
            if marker_available {
                fs::create_dir_all(root.join(".git")).unwrap();
            }
            // A missing marker directory fails before rename. A missing destination parent
            // fails after marker creation. Both errors must discard only the owned candidate.
            let output = directory.path().join("absent-parent");
            let error = prospective.retain(&output, &owner).unwrap_err();
            assert!(error.find_source::<WriteFileError>().is_some());
            assert!(!root.exists());
            assert!(!output.exists());
        }
    }
}
