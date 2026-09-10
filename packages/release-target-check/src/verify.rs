use std::ffi::OsStr;
use std::path::PathBuf;

use cargo_release_plan::{CheckFormat, RunInput, RunOutcome, run};
use ohno::AppError;
use serde::Deserialize;

use crate::cli::Cli;
use crate::command::capture;
use crate::repository::{Repository, VerificationError, canonicalize};

pub(crate) fn verify(cli: &Cli) -> Result<String, AppError> {
    let repository = Repository::discover(&cli.manifest_path, &cli.commit)?;
    repository.ensure_clean_head()?;
    repository.ensure_first_parent(&cli.release_line)?;
    let manifest = repository.require_tracked(&cli.manifest_path)?;
    if cli.verbose {
        eprintln!(
            "[release-target-check] candidate {} equals clean HEAD and belongs to the first-parent \
             history of supplied main tip {}; side-branch ancestry alone is not sufficient",
            cli.commit, cli.release_line
        );
    }
    let metadata = repository.checked(|| {
        capture(
            "cargo",
            [
                OsStr::new("metadata"),
                OsStr::new("--format-version"),
                OsStr::new("1"),
                OsStr::new("--locked"),
                OsStr::new("--offline"),
                OsStr::new("--no-deps"),
                OsStr::new("--manifest-path"),
                manifest.as_os_str(),
            ],
            &repository.root,
        )
    })?;
    let metadata: Metadata = serde_json::from_slice(&metadata).map_err(|error| {
        VerificationError::caused_by("cannot decode candidate Cargo metadata", error)
    })?;
    _ = repository.require_tracked(&metadata.workspace_root.join("Cargo.toml"))?;
    let lockfile = metadata.workspace_root.join("Cargo.lock");
    if lockfile.try_exists().map_err(|error| {
        VerificationError::caused_by("cannot inspect candidate workspace lockfile", error)
    })? {
        _ = repository.require_tracked(&lockfile)?;
    }
    for package in &metadata.packages {
        _ = repository.require_tracked(&package.manifest_path)?;
    }
    let workspace_root = canonicalize(&metadata.workspace_root)?;
    if !manifest.starts_with(&workspace_root) {
        return Err(VerificationError::new(
            "candidate manifest is outside the metadata workspace root",
        )
        .into());
    }
    for (name, version) in &cli.packages {
        let mut matches = metadata
            .packages
            .iter()
            .filter(|package| package.name == *name);
        let package = matches.next().ok_or_else(|| {
            VerificationError::new(format!(
                "requested package is absent from candidate metadata: {name}"
            ))
        })?;
        if matches.next().is_some()
            || !metadata.workspace_members.contains(&package.id)
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
        if cli.verbose {
            eprintln!(
                "[release-target-check] {name}@{version} matches a tracked publishable workspace \
                 member; locked, offline, no-deps metadata supplies identity without refreshing \
                 dependency resolution"
            );
        }
    }
    if cli.verbose {
        eprintln!(
            "[release-target-check] release invariants use candidate {} as their baseline, not \
             later main tip {}; each version retains its original first-parent anchor, including \
             inherited values and locked binary closures",
            cli.commit, cli.release_line
        );
    }
    let outcome = repository.checked(|| {
        run(&RunInput::Check {
            base: Some(cli.commit.clone()),
            manifest_path: manifest,
            format: CheckFormat::Text,
            verify_packaging: false,
            verbose: cli.verbose,
        })
    })?;
    match outcome {
        RunOutcome::Check {
            passed,
            message,
            warnings,
        } => {
            if !warnings.is_empty() {
                eprintln!("{warnings}");
            }
            if !passed {
                return Err(VerificationError::new(message).into());
            }
            if cli.verbose {
                eprintln!(
                    "[release-target-check] {message} HEAD and cleanliness still match the \
                     candidate after metadata and release verification"
                );
            }
        }
        _ => {
            return Err(VerificationError::new(
                "release checker did not return the requested check outcome",
            )
            .into());
        }
    }
    let packages = cli
        .packages
        .iter()
        .map(|(name, version)| format!("{name}@{version}"))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "Verified release target {}: {packages}.",
        cli.commit
    ))
}

/// Captures only the Cargo identity fields needed before release-policy checking.
#[derive(Debug, Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    workspace_members: Vec<String>,
    workspace_root: PathBuf,
}

/// Carries the candidate's declared identity and Cargo publication eligibility.
#[derive(Debug, Deserialize)]
struct Package {
    name: String,
    version: String,
    id: String,
    manifest_path: PathBuf,
    /// Cargo uses null for unrestricted publication and an empty array for disabled publication.
    publish: Option<Vec<String>>,
}
