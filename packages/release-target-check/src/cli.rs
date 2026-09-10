use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;

use ohno::AppError;
use semver::Version;

/// Captures an unambiguous request before any candidate workspace is inspected.
#[derive(Debug)]
pub(crate) struct Cli {
    pub(crate) manifest_path: PathBuf,
    pub(crate) commit: String,
    pub(crate) release_line: String,
    pub(crate) packages: BTreeMap<String, Version>,
    pub(crate) verbose: bool,
}

impl Cli {
    pub(crate) fn parse(arguments: impl IntoIterator<Item = OsString>) -> Result<Self, AppError> {
        let mut arguments = arguments.into_iter();
        let mut options = BTreeMap::new();
        let mut packages = BTreeMap::new();
        let mut verbose = false;
        while let Some(argument) = arguments.next() {
            if argument == "--verbose" && !verbose {
                verbose = true;
                continue;
            }
            let Some(option) = argument.to_str().filter(|option| {
                matches!(
                    *option,
                    "--manifest-path" | "--commit" | "--release-line" | "--package"
                )
            }) else {
                return Err(InvalidArgumentsError::new(format!(
                    "unknown or repeated option: {}",
                    argument.to_string_lossy()
                ))
                .into());
            };
            let value = arguments
                .next()
                .ok_or_else(|| InvalidArgumentsError::new(format!("missing value for {option}")))?;
            if value.is_empty() || value.to_string_lossy().starts_with("--") {
                return Err(
                    InvalidArgumentsError::new(format!("missing value for {option}")).into(),
                );
            }
            if option == "--package" {
                let (name, version) = package_request(&value)?;
                if packages.insert(name.clone(), version).is_some() {
                    return Err(InvalidArgumentsError::new(format!(
                        "package name requested more than once: {name}"
                    ))
                    .into());
                }
            } else if options.insert(option.to_owned(), value).is_some() {
                return Err(
                    InvalidArgumentsError::new(format!("repeated option: {option}")).into(),
                );
            }
        }
        let mut required = |name| {
            options
                .remove(name)
                .ok_or_else(|| InvalidArgumentsError::new(format!("missing {name}")))
        };
        let manifest_path = PathBuf::from(required("--manifest-path")?);
        let commit = immutable_id(required("--commit")?)?;
        let release_line = immutable_id(required("--release-line")?)?;
        if packages.is_empty() {
            return Err(InvalidArgumentsError::new("at least one --package is required").into());
        }
        Ok(Self {
            manifest_path,
            commit,
            release_line,
            packages,
            verbose,
        })
    }
}

fn package_request(value: &OsStr) -> Result<(String, Version), AppError> {
    let invalid = || {
        InvalidArgumentsError::new(format!(
            "expected a Cargo package name and exact SemVer version as name@version: {}",
            value.to_string_lossy()
        ))
    };
    let (name, version) = value
        .to_str()
        .and_then(|value| value.split_once('@'))
        .ok_or_else(invalid)?;
    // Cargo package names use ASCII alphanumerics, hyphens and underscores.
    // Exclude path/registry-qualified selectors: the request identifies a workspace member.
    if !name
        .bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(invalid().into());
    }
    let version = Version::parse(version).map_err(|error| {
        InvalidArgumentsError::caused_by("package version must be an exact SemVer version", error)
    })?;
    Ok((name.to_owned(), version))
}

fn immutable_id(value: OsString) -> Result<String, AppError> {
    // Full hexadecimal object names for Git's supported storage object formats.
    const SHA1_HEX_LENGTH: usize = 40;
    const SHA256_HEX_LENGTH: usize = 64;
    let invalid = || {
        InvalidArgumentsError::new(
            "commit arguments must be full lowercase immutable Git object IDs",
        )
    };
    let value = value.into_string().map_err(|_non_utf8| invalid())?;
    if !matches!(value.len(), SHA1_HEX_LENGTH | SHA256_HEX_LENGTH)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(invalid().into());
    }
    Ok(value)
}

/// Identifies malformed or conflicting orchestration input.
#[ohno::error]
#[display("{reason}")]
struct InvalidArgumentsError {
    reason: String,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    fn arguments() -> Vec<OsString> {
        [
            "--manifest-path",
            "Cargo.toml",
            "--commit",
            &"a".repeat(40),
            "--release-line",
            &"b".repeat(40),
            "--package",
            "example-crate@1.2.3",
        ]
        .map(OsString::from)
        .to_vec()
    }

    #[test]
    fn accepts_exact_package_requests() {
        let mut arguments = arguments();
        arguments.extend(
            ["--package", "another_crate@2.0.0-beta.1+build", "--verbose"].map(OsString::from),
        );
        let cli = Cli::parse(arguments).unwrap();
        assert!(cli.verbose);
        assert_eq!(
            cli.packages.get("example-crate").unwrap(),
            &Version::new(1, 2, 3)
        );
        assert_eq!(cli.packages.len(), 2);
    }

    #[test]
    fn rejects_missing_and_conflicting_options() {
        for extra in [
            vec!["--package", "example-crate@1.2.3"],
            vec!["--package", "example-crate@2.0.0"],
            vec!["--commit", &"c".repeat(40)],
            vec!["--verbose", "--verbose"],
            vec!["--unknown"],
            vec!["--package"],
        ] {
            let mut arguments = arguments();
            arguments.extend(extra.into_iter().map(OsString::from));
            _ = Cli::parse(arguments).unwrap_err();
        }
        let mut arguments = arguments();
        arguments.truncate(6);
        _ = Cli::parse(arguments).unwrap_err();
        _ = Cli::parse([]).unwrap_err();
    }

    #[test]
    fn rejects_invalid_package_requests() {
        for value in [
            "",
            "example",
            "@1.0.0",
            "example@",
            "example@1",
            "example@^1.0.0",
            "example@1.0.0@other",
            "../example@1.0.0",
            "name space@1.0.0",
            "1example@1.0.0",
            "-example@1.0.0",
        ] {
            _ = package_request(&OsString::from(value)).unwrap_err();
        }
    }

    #[test]
    fn accepts_only_full_immutable_ids() {
        _ = immutable_id("a".repeat(40).into()).unwrap();
        _ = immutable_id("a".repeat(64).into()).unwrap();
        for value in [
            "HEAD",
            "origin/main",
            "abc1234",
            &"a".repeat(39),
            &"a".repeat(41),
            &"A".repeat(40),
            &"g".repeat(40),
        ] {
            _ = immutable_id(value.into()).unwrap_err();
        }
    }
}
