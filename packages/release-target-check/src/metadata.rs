use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ohno::AppError;
use semver::Version;
use serde::Deserialize;

use crate::repository::{Repository, VerificationError, canonicalize};

/// Validates Cargo's identity response before the release checker assesses package content.
#[derive(Debug, Deserialize)]
pub(crate) struct Metadata {
    packages: Vec<Package>,
    workspace_members: Vec<String>,
    workspace_root: PathBuf,
}

impl Metadata {
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self, AppError> {
        serde_json::from_slice(bytes).map_err(|error| {
            VerificationError::caused_by("cannot decode candidate Cargo metadata", error).into()
        })
    }

    pub(crate) fn validate_inputs(
        &self,
        repository: &Repository,
        manifest: &Path,
    ) -> Result<(), AppError> {
        _ = repository.require_tracked(&self.workspace_root.join("Cargo.toml"))?;
        let lockfile = self.workspace_root.join("Cargo.lock");
        if lockfile.try_exists().map_err(|error| {
            VerificationError::caused_by("cannot inspect candidate workspace lockfile", error)
        })? {
            _ = repository.require_tracked(&lockfile)?;
        }
        for package in &self.packages {
            _ = repository.require_tracked(&package.manifest_path)?;
        }
        let workspace_root = canonicalize(&self.workspace_root)?;
        if !manifest.starts_with(&workspace_root) {
            return Err(VerificationError::new(
                "candidate manifest is outside the metadata workspace root",
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn validate_packages(
        &self,
        required: &BTreeMap<String, Version>,
        verbose: bool,
    ) -> Result<(), AppError> {
        for (name, version) in required {
            let mut matches = self.packages.iter().filter(|package| package.name == *name);
            let package = matches.next().ok_or_else(|| {
                VerificationError::new(format!(
                    "requested package is absent from candidate metadata: {name}"
                ))
            })?;
            if matches.next().is_some()
                || !self.workspace_members.contains(&package.id)
                || package.publish.as_ref().is_some_and(Vec::is_empty)
                || package.version != version.to_string()
            {
                return Err(VerificationError::new(format!(
                    "candidate must contain one publishable workspace member {name}@{version}; \
                     metadata reports version {}",
                    package.version
                ))
                .into());
            }
            if verbose {
                eprintln!(
                    "[release-target-check] {name}@{version} matches a tracked publishable workspace \
                     member; locked, offline, no-deps metadata supplies identity without refreshing \
                     dependency resolution"
                );
            }
        }
        Ok(())
    }
}

/// Carries the declared identity and publication eligibility of a metadata package.
#[derive(Debug, Deserialize)]
struct Package {
    name: String,
    version: String,
    id: String,
    manifest_path: PathBuf,
    /// Cargo uses null for unrestricted publication and an empty array for disabled publication.
    publish: Option<Vec<String>>,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::fs;
    #[cfg(unix)]
    use std::io::Error;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    use serde_json::json;
    use testing::with_watchdog;

    use super::*;
    use crate::repository::fixture::{command, fixture};

    fn sample(root: &Path) -> Metadata {
        Metadata {
            workspace_root: root.to_owned(),
            workspace_members: vec!["widget-id".into()],
            packages: vec![package(root, "widget")],
        }
    }

    fn package(root: &Path, name: &str) -> Package {
        Package {
            name: name.into(),
            id: format!("{name}-id"),
            version: "1.0.0".into(),
            manifest_path: root.join("Cargo.toml"),
            publish: None,
        }
    }

    fn required() -> BTreeMap<String, Version> {
        BTreeMap::from([("widget".into(), Version::new(1, 0, 0))])
    }

    #[test]
    fn decodes_required_identity_fields_and_tolerates_unrelated_fields() {
        let input = json!({
            "workspace_root": "workspace",
            "workspace_members": ["widget-id"],
            "packages": [{
                "name": "widget",
                "version": "1.0.0",
                "id": "widget-id",
                "manifest_path": "workspace/Cargo.toml",
                "publish": null,
                "unrelated": true
            }],
            "unrelated": true
        });
        let metadata = Metadata::parse(&serde_json::to_vec(&input).unwrap()).unwrap();
        assert_eq!(metadata.workspace_root, PathBuf::from("workspace"));
        assert_eq!(
            metadata.packages.first().unwrap().manifest_path,
            PathBuf::from("workspace").join("Cargo.toml")
        );
        metadata.validate_packages(&required(), false).unwrap();
    }

    #[test]
    fn rejects_malformed_metadata_and_missing_required_fields() {
        for bytes in [
            b"not json".as_slice(),
            b"[]",
            b"{}",
            br#"{"workspace_root":null,"packages":[],"workspace_members":[]}"#,
            br#"{"workspace_root":".","packages":[{}],"workspace_members":[]}"#,
        ] {
            let error = Metadata::parse(bytes).unwrap_err();
            assert!(error.find_source::<VerificationError>().is_some());
            assert!(error.find_source::<serde_json::Error>().is_some());
        }
    }

    #[test]
    fn accepts_publishable_package_selections_independently_of_metadata_order() {
        for publication in [None, Some(vec!["crates-io".into()])] {
            let root = Path::new("workspace");
            let mut metadata = sample(root);
            metadata.packages.first_mut().unwrap().publish = publication;
            metadata.packages.insert(0, package(root, "another"));
            metadata.workspace_members.push("another-id".into());
            let mut required = required();
            required.insert("another".into(), Version::new(1, 0, 0));
            metadata.validate_packages(&required, false).unwrap();
            metadata.validate_packages(&required, true).unwrap();
        }
    }

    #[test]
    fn rejects_absent_duplicate_nonmember_private_and_mismatched_packages() {
        let root = Path::new("workspace");
        let required = required();

        let mut absent = sample(root);
        absent.packages.clear();
        let mut duplicate = sample(root);
        duplicate.packages.push(package(root, "widget"));
        let mut nonmember = sample(root);
        nonmember.workspace_members.clear();
        let mut private = sample(root);
        private.packages.first_mut().unwrap().publish = Some(Vec::new());
        let mut mismatch = sample(root);
        mismatch.packages.first_mut().unwrap().version = "1.1.0".into();
        let mut build_mismatch = sample(root);
        build_mismatch.packages.first_mut().unwrap().version = "1.0.0+other".into();

        for metadata in [
            absent,
            duplicate,
            nonmember,
            private,
            mismatch,
            build_mismatch,
        ] {
            let error = metadata.validate_packages(&required, false).unwrap_err();
            assert!(error.find_source::<VerificationError>().is_some());
        }
    }

    #[test]
    fn checks_every_requested_package() {
        let root = Path::new("workspace");
        let mut metadata = sample(root);
        metadata.packages.push(package(root, "other"));
        metadata.workspace_members.push("other-id".into());
        let mut required = required();
        required.insert("other".into(), Version::new(1, 0, 0));
        required.insert("widget".into(), Version::new(2, 0, 0));
        _ = metadata.validate_packages(&required, false).unwrap_err();
    }

    #[test]
    #[cfg_attr(miri, ignore = "Executes Git against filesystem fixtures")]
    fn validates_tracked_inputs_with_and_without_a_lockfile() {
        with_watchdog(|| {
            let (_directory, repository) = fixture();
            let metadata = sample(&repository.root);
            let manifest = repository.root.join("Cargo.toml");
            metadata.validate_inputs(&repository, &manifest).unwrap();

            fs::write(repository.root.join("Cargo.lock"), "tracked lockfile").unwrap();
            command(&repository.root, &["add", "Cargo.lock"]);
            metadata.validate_inputs(&repository, &manifest).unwrap();
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Executes Git against filesystem fixtures")]
    fn rejects_an_untracked_lockfile_or_member_manifest() {
        with_watchdog(|| {
            let (_directory, repository) = fixture();
            let mut metadata = sample(&repository.root);
            let manifest = repository.root.join("Cargo.toml");
            let lockfile = repository.root.join("Cargo.lock");
            fs::write(&lockfile, "untracked lockfile").unwrap();
            _ = metadata
                .validate_inputs(&repository, &manifest)
                .unwrap_err();

            fs::remove_file(lockfile).unwrap();
            let member = repository.root.join("member.toml");
            fs::write(&member, "untracked member manifest").unwrap();
            metadata.packages.first_mut().unwrap().manifest_path = member;
            _ = metadata
                .validate_inputs(&repository, &manifest)
                .unwrap_err();
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Executes Git against filesystem fixtures")]
    fn rejects_a_manifest_outside_the_reported_workspace() {
        with_watchdog(|| {
            let (_directory, repository) = fixture();
            let nested = repository.root.join("nested");
            fs::create_dir_all(&nested).unwrap();
            fs::write(nested.join("Cargo.toml"), "nested workspace").unwrap();
            command(&repository.root, &["add", "nested/Cargo.toml"]);
            let metadata = sample(&nested);
            let error = metadata
                .validate_inputs(&repository, &repository.root.join("Cargo.toml"))
                .unwrap_err();
            assert!(error.find_source::<VerificationError>().is_some());
        });
    }

    #[cfg(unix)]
    #[test]
    #[cfg_attr(miri, ignore = "Creates a filesystem symlink loop and executes Git")]
    fn propagates_a_lockfile_lookup_error() {
        with_watchdog(|| {
            let (_directory, repository) = fixture();
            // An owned symlink loop produces a deterministic lookup error without races or
            // permission assumptions that change when a runner is privileged.
            symlink("Cargo.lock", repository.root.join("Cargo.lock")).unwrap();
            let metadata = sample(&repository.root);
            let error = metadata
                .validate_inputs(&repository, &repository.root.join("Cargo.toml"))
                .unwrap_err();
            assert!(error.find_source::<VerificationError>().is_some());
            assert!(error.find_source::<Error>().is_some());
        });
    }
}
