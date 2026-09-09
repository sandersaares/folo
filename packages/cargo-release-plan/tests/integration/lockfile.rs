//! Released-content consequences of the workspace lockfile.
//!
//! Cargo puts a lockfile into every package archive, but only the resolution of
//! an installable binary target is operationally relevant. Library consumers
//! resolve the library in their own dependency graph.
//! Ref: docs/design.md, "Relevant lockfile closures".

use std::fmt::Write as _;
use std::fs;

use serde_json::{Value, json};

use crate::fixture::{Fixture, write_binary_package, write_package};
use crate::harness::{check, check_verbose, report_json};

/// Declarations matching the hand-written lockfile, without registry resolution.
const INSTALLATION_DEPENDENCIES: &str = r#"
[dependencies]
helper = { path = "../helper", version = "0.1.0" }
widget = "1"
"#;

/// Writes a workspace lockfile resolving `tool` onto `helper` and `widget`.
///
/// The lockfile is written by hand rather than resolved, because a test that
/// let Cargo resolve one would need a registry. Only `widget`'s version varies
/// between the two revisions a test compares, so the closure is the only thing
/// that can explain a verdict.
fn write_lockfile(fixture: &Fixture, widget_version: &str) {
    fixture.write(
        "Cargo.lock",
        &format!(
            r#"version = 4

[[package]]
name = "helper"
version = "0.1.0"

[[package]]
name = "tool"
version = "0.1.0"
dependencies = [
 "helper",
 "widget",
]

[[package]]
name = "widget"
version = "{widget_version}"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "1111111111111111111111111111111111111111111111111111111111111111"
"#
        ),
    );
}

fn locked_workspace() -> Fixture {
    let fixture = Fixture::new("");
    write_binary_package(&fixture, "tool", "0.1.0", INSTALLATION_DEPENDENCIES);
    write_package(&fixture, "helper", "0.1.0", "");
    write_lockfile(&fixture, "1.0.0");
    fixture.commit("seed");
    fixture
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_moved_dependency_needs_an_increment_for_a_binary_package() {
    let fixture = locked_workspace();
    let base = fixture.sha("HEAD");

    write_lockfile(&fixture, "1.0.1");
    fixture.commit("update the locked widget");

    let (passed, message) = check_verbose(&fixture, &base);
    assert!(!passed, "{message}");
    assert!(message.contains("tool: needs-increment"), "{message}");
    assert!(message.contains("widget"), "{message}");
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_moved_dependency_leaves_a_library_package_unchanged() {
    // `helper` publishes no binary target, so consumers do not use
    // the package archive's lockfile to resolve it.
    let fixture = locked_workspace();
    let base = fixture.sha("HEAD");

    write_lockfile(&fixture, "1.0.1");
    fixture.commit("update the locked widget");

    let (_, message) = check(&fixture, &base);
    assert!(!message.contains("helper: needs-increment"), "{message}");
}

/// An untracked conventional binary does not make the released artifact carry a lockfile.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn an_untracked_default_binary_does_not_require_a_lockfile() {
    let fixture = Fixture::new("");
    write_package(&fixture, "tool", "0.1.0", "");
    fixture.commit("seed library");
    let base = fixture.sha("HEAD");
    fixture.write("packages/tool/src/main.rs", "fn main() {}\n");

    let (passed, message) = check(&fixture, &base);

    assert!(passed, "{message}");
}

/// An ignored conventional example does not make the released artifact carry a lockfile.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn an_ignored_example_does_not_require_a_lockfile() {
    let fixture = Fixture::new("");
    write_package(&fixture, "tool", "0.1.0", "");
    fixture.write(".gitignore", "packages/tool/examples/\n");
    fixture.commit("seed library");
    let base = fixture.sha("HEAD");
    fixture.write("packages/tool/examples/demo.rs", "fn main() {}\n");

    let (passed, message) = check(&fixture, &base);

    assert!(passed, "{message}");
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn an_incremented_binary_package_settles() {
    // The package's own entry is not part of its closure, so incrementing it
    // must not register as a further change to its dependencies.
    let fixture = locked_workspace();
    let base = fixture.sha("HEAD");

    write_binary_package(&fixture, "tool", "0.2.0", INSTALLATION_DEPENDENCIES);
    fixture.write(
        "Cargo.lock",
        &fixture.read("Cargo.lock").replace(
            "name = \"tool\"\nversion = \"0.1.0\"",
            "name = \"tool\"\nversion = \"0.2.0\"",
        ),
    );
    fixture.commit("increment tool");

    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn an_untouched_lockfile_leaves_a_binary_package_unchanged() {
    let fixture = locked_workspace();
    let base = fixture.sha("HEAD");

    fixture.write("packages/helper/src/lib.rs", "pub fn f() { let _ = 3; }\n");
    fixture.commit("edit helper only");

    let (_, message) = check(&fixture, &base);
    assert!(!message.contains("tool: needs-increment"), "{message}");
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn multiple_binary_packages_are_classified_from_one_lockfile() {
    let fixture = Fixture::new("");
    write_binary_package(&fixture, "first", "0.1.0", "");
    write_binary_package(&fixture, "second", "0.1.0", "");
    fixture.write(
        "Cargo.lock",
        r#"version = 4

[[package]]
name = "first"
version = "0.1.0"

[[package]]
name = "second"
version = "0.1.0"
"#,
    );
    fixture.commit("seed");
    let base = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &base);

    assert!(passed, "{message}");
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_new_binary_package_needs_no_historical_lockfile() {
    let fixture = Fixture::new("");
    write_package(&fixture, "existing", "0.1.0", "");
    fixture.commit("seed");
    let base = fixture.sha("HEAD");
    write_binary_package(&fixture, "tool", "0.1.0", "");
    fixture.commit("add binary package");

    let (passed, message) = check(&fixture, &base);

    assert!(passed, "{message}");
}

/// Adding the first binary starts the released lockfile closure at an empty endpoint.
///
/// The anchor is a library-only package, so no anchor lockfile is required and
/// every dependency in the work-tree closure is reported as added.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_library_that_adds_its_first_binary_needs_no_anchor_lockfile() {
    let fixture = Fixture::new("");
    write_package(&fixture, "tool", "0.1.0", INSTALLATION_DEPENDENCIES);
    write_package(&fixture, "helper", "0.1.0", "");
    fixture.commit("seed library");
    let base = fixture.sha("HEAD");

    fixture.write("packages/tool/src/main.rs", "fn main() {}\n");
    write_lockfile(&fixture, "1.0.0");
    fixture.commit("add binary");

    let (passed, message) = check_verbose(&fixture, &base);
    assert!(!passed, "{message}");
    assert!(message.contains("tool: needs-increment"), "{message}");
    assert_lockfile_change(&fixture, &base, "added");
}

/// A workspace lockfile does not invent a library-only anchor closure.
///
/// The anchor lockfile exists for another binary and happens to resolve the
/// library package too. The library's endpoint is nevertheless empty.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn an_unrelated_anchor_lockfile_does_not_create_a_library_closure() {
    let fixture = Fixture::new("");
    write_package(&fixture, "tool", "0.1.0", INSTALLATION_DEPENDENCIES);
    write_package(&fixture, "helper", "0.1.0", "");
    write_binary_package(&fixture, "runner", "0.1.0", "");
    write_lockfile(&fixture, "1.0.0");
    fixture.write(
        "Cargo.lock",
        &format!(
            "{}\n[[package]]\nname = \"runner\"\nversion = \"0.1.0\"\n",
            fixture.read("Cargo.lock")
        ),
    );
    fixture.commit("seed library beside a binary");
    let base = fixture.sha("HEAD");

    fixture.write("packages/tool/src/main.rs", "fn main() {}\n");
    fixture.commit("add binary");

    let (passed, message) = check_verbose(&fixture, &base);
    assert!(!passed, "{message}");
    assert_lockfile_change(&fixture, &base, "added");
}

/// Removing the last binary ends the released lockfile closure.
///
/// The work-tree endpoint is library-only, so it needs no lockfile and reports
/// the anchor closure as removed.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn removing_the_last_binary_needs_no_work_tree_lockfile() {
    let fixture = locked_workspace();
    let base = fixture.sha("HEAD");

    fs::remove_file(fixture.path().join("packages/tool/src/main.rs")).unwrap();
    fixture.write("packages/tool/src/lib.rs", "pub fn f() {}\n");
    fs::remove_file(fixture.path().join("Cargo.lock")).unwrap();
    fixture.commit("replace binary with library");

    let (passed, message) = check_verbose(&fixture, &base);
    assert!(!passed, "{message}");
    assert_lockfile_change(&fixture, &base, "deleted");
}

/// Executable examples are not installed applications, even when published.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_moved_dependency_does_not_increment_a_library_with_an_example() {
    let fixture = Fixture::new("");
    write_package(&fixture, "tool", "0.1.0", INSTALLATION_DEPENDENCIES);
    fixture.write("packages/tool/examples/demo.rs", "fn main() {}\n");
    write_package(&fixture, "helper", "0.1.0", "");
    write_lockfile(&fixture, "1.0.0");
    fixture.commit("seed example");
    let base = fixture.sha("HEAD");

    write_lockfile(&fixture, "1.0.1");
    fixture.commit("update the locked widget");

    let (passed, message) = check_verbose(&fixture, &base);
    assert!(passed, "{message}");
    assert_no_lockfile_changes(&fixture, &base);
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn library_auxiliary_targets_never_require_or_compare_a_lockfile() {
    let fixture = Fixture::new("");
    for (name, selection, explicit) in [
        ("implicit", "", false),
        ("implicit-excluded", "include = [\"src/**\"]\n", false),
        ("explicit", "", true),
        ("explicit-excluded", "include = [\"src/**\"]\n", true),
    ] {
        let target = if explicit {
            "autoexamples = false\n[[example]]\nname = \"demo\"\n\
             path = \"custom/demo.rs\"\ncrate-type = [\"bin\"]\n"
        } else {
            ""
        };
        write_package(
            &fixture,
            name,
            "0.1.0",
            &format!("{selection}{target}\n[dependencies]\nwidget = \"1\"\n"),
        );
        let example = if explicit {
            "custom/demo.rs"
        } else {
            "examples/demo/main.rs"
        };
        fixture.write(&format!("packages/{name}/{example}"), "fn main() {}\n");
    }
    write_package(
        &fixture,
        "auxiliary",
        "0.1.0",
        "build = \"custom/build.rs\"\n\
         [[bench]]\nname = \"timing\"\npath = \"custom/timing.rs\"\nharness = false\n\
         [[test]]\nname = \"test\"\npath = \"custom/test.rs\"\nharness = false\n",
    );
    for target in ["build", "timing", "test"] {
        fixture.write(
            &format!("packages/auxiliary/custom/{target}.rs"),
            "fn main() {}\n",
        );
    }
    fixture.commit("seed libraries and auxiliary targets without a lockfile");
    let base = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
    assert!(!fixture.path().join("Cargo.lock").exists());

    write_lockfile(&fixture, "1.0.0");
    fixture.commit("record a lockfile");
    write_lockfile(&fixture, "1.0.1");
    fixture.commit("change only resolution");
    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
    assert_no_lockfile_changes(&fixture, &base);

    // Even unreadable resolution is irrelevant when no installed binary exists.
    fixture.write("Cargo.lock", "not = = toml");
    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn example_appearance_and_removal_do_not_create_lockfile_endpoints() {
    let fixture = Fixture::new("");
    write_package(&fixture, "tool", "0.1.0", "include = [\"src/**\"]");
    fixture.commit("seed library");
    let library = fixture.sha("HEAD");
    fixture.write("packages/tool/examples/demo.rs", "fn main() {}\n");
    fixture.commit("add an excluded executable example");
    let example = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &library);
    assert!(passed, "{message}");

    fs::remove_file(fixture.path().join("packages/tool/examples/demo.rs")).unwrap();
    fixture.commit("remove the example");
    let (passed, message) = check(&fixture, &example);
    assert!(passed, "{message}");
    assert!(!fixture.path().join("Cargo.lock").exists());
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn library_source_and_requirements_still_need_increments_without_a_lockfile() {
    let fixture = Fixture::new("");
    write_package(&fixture, "source", "0.1.0", "");
    write_package(
        &fixture,
        "requirement",
        "0.1.0",
        "[dependencies]\nwidget = \"1\"",
    );
    fixture.write("packages/source/examples/demo.rs", "fn main() {}\n");
    fixture.write("packages/requirement/examples/demo.rs", "fn main() {}\n");
    fixture.commit("seed libraries with examples");
    let base = fixture.sha("HEAD");

    fixture.write("packages/source/src/lib.rs", "pub fn updated() {}\n");
    write_package(
        &fixture,
        "requirement",
        "0.1.0",
        "[dependencies]\nwidget = \"2\"",
    );
    fixture.commit("change published source and dependency requirements");

    let (passed, message) = check(&fixture, &base);
    assert!(!passed, "{message}");
    assert!(message.contains("source: needs-increment"), "{message}");
    assert!(
        message.contains("requirement: needs-increment"),
        "{message}"
    );
    assert_no_lockfile_changes(&fixture, &base);
    assert!(!fixture.path().join("Cargo.lock").exists());
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn explicit_custom_binary_with_disabled_autobins_keeps_its_installation_closure() {
    let fixture = Fixture::new("");
    write_package(
        &fixture,
        "tool",
        "0.1.0",
        &format!(
            "autobins = false\n[[bin]]\nname = \"custom\"\npath = \"custom/entry.rs\"\n\
             {INSTALLATION_DEPENDENCIES}"
        ),
    );
    fixture.write("packages/tool/custom/entry.rs", "fn main() {}\n");
    write_package(&fixture, "helper", "0.1.0", "");
    write_lockfile(&fixture, "1.0.0");
    fixture.commit("seed mixed package with a custom binary");
    let base = fixture.sha("HEAD");

    write_lockfile(&fixture, "1.0.1");
    fixture.commit("change the binary installation");

    assert_lockfile_change(&fixture, &base, "modified");
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn disabled_autobins_ignores_conventional_binary_files() {
    let fixture = Fixture::new("");
    write_package(&fixture, "tool", "0.1.0", "autobins = false");
    for target in ["src/main.rs", "src/bin/other.rs", "src/bin/nested/main.rs"] {
        fixture.write(&format!("packages/tool/{target}"), "fn main() {}\n");
    }
    fixture.commit("seed library with disabled conventional binaries");
    let base = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
    assert!(!fixture.path().join("Cargo.lock").exists());
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn binary_declaration_and_discovery_controls_are_endpoint_specific() {
    for explicit in [false, true] {
        let fixture = Fixture::new("");
        let disabled = format!("autobins = false\n{INSTALLATION_DEPENDENCIES}");
        let enabled = if explicit {
            format!(
                "autobins = false\n[[bin]]\nname = \"custom\"\npath = \"custom/main.rs\"\n\
                 {INSTALLATION_DEPENDENCIES}"
            )
        } else {
            format!("autobins = true\n{INSTALLATION_DEPENDENCIES}")
        };
        write_package(&fixture, "tool", "0.1.0", &disabled);
        write_package(&fixture, "helper", "0.1.0", "");
        let target = if explicit {
            "custom/main.rs"
        } else {
            "src/main.rs"
        };
        fixture.write(&format!("packages/tool/{target}"), "fn main() {}\n");
        fixture.write("packages/tool/examples/demo.rs", "fn main() {}\n");
        fixture.commit("seed a library with a disabled binary and an example");
        let library = fixture.sha("HEAD");

        write_package(&fixture, "tool", "0.1.0", &enabled);
        write_lockfile(&fixture, "1.0.0");
        fixture.commit("enable the binary");
        assert_lockfile_change(&fixture, &library, "added");

        // A version change makes the binary-bearing commit the next anchor.
        write_package(&fixture, "tool", "0.1.1", &enabled);
        fixture.write(
            "Cargo.lock",
            &fixture.read("Cargo.lock").replace(
                "name = \"tool\"\nversion = \"0.1.0\"",
                "name = \"tool\"\nversion = \"0.1.1\"",
            ),
        );
        fixture.commit("anchor the binary release");
        let binary = fixture.sha("HEAD");

        write_package(&fixture, "tool", "0.1.1", &disabled);
        fs::remove_file(fixture.path().join("Cargo.lock")).unwrap();
        fixture.commit("disable the binary while retaining the example");
        assert_lockfile_change(&fixture, &binary, "deleted");
    }
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn installation_closures_exclude_root_and_transitive_workspace_development_dependencies() {
    let fixture = Fixture::new(
        "[workspace.dependencies]\n\
         shared-builder = { package = \"transitive-builder\", version = \"1\" }\n\
         shared-dev = { package = \"transitive-dev\", version = \"1\" }\n",
    );
    write_binary_package(
        &fixture,
        "tool",
        "0.1.0",
        "[dependencies]\nhelper = { path = \"../helper\", version = \"=0.1.0\" }\n\
         [build-dependencies]\nroot-builder = \"1\"\n\
         [dev-dependencies]\nroot-dev = \"1\"\n",
    );
    write_package(
        &fixture,
        "helper",
        "0.1.0",
        "publish = false\n\
         [target.'cfg(unix)'.build-dependencies]\nshared-builder.workspace = true\n\
         [target.'cfg(windows)'.dev-dependencies]\nshared-dev.workspace = true\n",
    );
    let original = r#"version = 4
[[package]]
name = "tool"
version = "0.1.0"
dependencies = ["helper", "root-builder", "root-dev"]
[[package]]
name = "helper"
version = "0.1.0"
dependencies = ["transitive-builder", "transitive-dev"]
[[package]]
name = "root-builder"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
dependencies = ["installed-leaf"]
[[package]]
name = "transitive-builder"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
[[package]]
name = "installed-leaf"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
[[package]]
name = "root-dev"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
dependencies = ["dev-leaf"]
[[package]]
name = "transitive-dev"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
dependencies = ["dev-leaf"]
[[package]]
name = "dev-leaf"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"#;
    fixture.write("Cargo.lock", original);
    fixture.commit("seed an executable and an unpublished exact dependency");
    let base = fixture.sha("HEAD");
    let development = ["root-dev", "transitive-dev", "dev-leaf"].into_iter().fold(
        original.to_owned(),
        |lockfile, name| {
            lockfile.replace(
                &format!("name = \"{name}\"\nversion = \"1.0.0\""),
                &format!("name = \"{name}\"\nversion = \"1.0.1\""),
            )
        },
    );
    fixture.write("Cargo.lock", &development);
    fixture.commit("change development-only resolution");

    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
    assert_no_lockfile_changes(&fixture, &base);
    assert_eq!(fixture.read("Cargo.lock"), development);

    for dependency in ["root-builder", "transitive-builder", "installed-leaf"] {
        fixture.write(
            "Cargo.lock",
            &development.replace(
                &format!("name = \"{dependency}\"\nversion = \"1.0.0\""),
                &format!("name = \"{dependency}\"\nversion = \"1.0.1\""),
            ),
        );
        let report: Value = serde_json::from_str(&report_json(&fixture, &base)).unwrap();
        let tool = report
            .get("packages")
            .and_then(Value::as_array)
            .unwrap()
            .iter()
            .find(|package| package.get("name").and_then(Value::as_str) == Some("tool"))
            .unwrap();
        assert_eq!(
            tool.get("status").and_then(Value::as_str),
            Some("needs-increment")
        );
        assert_eq!(
            tool.get("changed").unwrap(),
            &json!([{"source": "lockfile", "dependency": dependency, "change": "modified"}])
        );
    }

    // The declarations belong to each endpoint, not to the entire comparison.
    // Reclassifying a helper edge alone changes the installation without touching
    // the lockfile or any publishable manifest.
    fixture.write("Cargo.lock", &development);
    fixture.write(
        "packages/helper/Cargo.toml",
        &fixture.read("packages/helper/Cargo.toml").replace(
            "[target.'cfg(unix)'.build-dependencies]",
            "[target.'cfg(unix)'.dev-dependencies]",
        ),
    );
    let report: Value = serde_json::from_str(&report_json(&fixture, &base)).unwrap();
    let tool = report
        .get("packages")
        .and_then(Value::as_array)
        .unwrap()
        .iter()
        .find(|package| package.get("name").and_then(Value::as_str) == Some("tool"))
        .unwrap();
    assert_eq!(
        tool.get("changed").unwrap(),
        &json!([{"source": "lockfile", "dependency": "transitive-builder", "change": "deleted"}])
    );
}

/// Historical `src/bin/*.rs` discovery feeds lockfile classification.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_moved_dependency_needs_an_increment_for_an_auto_discovered_binary() {
    let fixture = Fixture::new("");
    write_package(&fixture, "tool", "0.1.0", INSTALLATION_DEPENDENCIES);
    fixture.write("packages/tool/src/bin/secondary.rs", "fn main() {}\n");
    write_package(&fixture, "helper", "0.1.0", "");
    write_lockfile(&fixture, "1.0.0");
    fixture.commit("seed auto-discovered binary");
    let base = fixture.sha("HEAD");

    write_lockfile(&fixture, "1.0.1");
    fixture.commit("update the locked widget");

    let (passed, message) = check(&fixture, &base);
    assert!(!passed, "{message}");
    assert!(message.contains("tool: needs-increment"), "{message}");
    assert_lockfile_change(&fixture, &base, "modified");
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_binary_anchor_without_a_lockfile_is_an_error() {
    let fixture = Fixture::new("");
    write_binary_package(&fixture, "tool", "0.1.0", "");
    fixture.commit("seed");
    let base = fixture.sha("HEAD");

    fixture.write("packages/tool/src/main.rs", "fn main() { let _ = 4; }\n");
    fixture.commit("edit the binary");

    let error = check_error(&fixture, &base);
    assert!(error.contains("anchor commit"), "{error}");
    assert!(error.contains("Cargo.lock"), "{error}");
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_missing_work_tree_lockfile_is_an_error() {
    let fixture = locked_workspace();
    let base = fixture.sha("HEAD");
    fs::remove_file(fixture.path().join("Cargo.lock")).unwrap();

    let error = check_error(&fixture, &base);
    assert!(error.contains("work tree"), "{error}");
    assert!(error.contains("Cargo.lock"), "{error}");
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_malformed_work_tree_lockfile_is_an_error() {
    let fixture = locked_workspace();
    let base = fixture.sha("HEAD");
    fixture.write("Cargo.lock", "not = = toml");

    let error = check_error(&fixture, &base);

    assert!(error.contains("Cargo.lock"), "{error}");
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn an_anchor_lockfile_with_an_unresolved_dependency_is_an_error() {
    let fixture = Fixture::new("");
    write_binary_package(&fixture, "tool", "0.1.0", "");
    fixture.write(
        "Cargo.lock",
        r#"version = 4

[[package]]
name = "tool"
version = "0.1.0"
dependencies = ["absent"]
"#,
    );
    fixture.commit("seed an incomplete lockfile");
    let base = fixture.sha("HEAD");

    fixture.write(
        "Cargo.lock",
        r#"version = 4

[[package]]
name = "tool"
version = "0.1.0"
"#,
    );
    fixture.write("packages/tool/src/main.rs", "fn main() { let _ = 4; }\n");
    fixture.commit("restore the lockfile and edit the binary");

    let error = check_error(&fixture, &base);
    assert!(error.contains("Cargo.lock"), "{error}");
    assert!(error.contains("resolved dependencies"), "{error}");
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn an_anchor_lockfile_that_does_not_resolve_the_binary_is_an_error() {
    let fixture = Fixture::new("");
    write_binary_package(&fixture, "tool", "0.1.0", "");
    fixture.write(
        "Cargo.lock",
        r#"version = 4

[[package]]
name = "widget"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"#,
    );
    fixture.commit("seed");
    let base = fixture.sha("HEAD");

    fixture.write("packages/tool/src/main.rs", "fn main() { let _ = 4; }\n");
    fixture.commit("edit the binary");

    let error = check_error(&fixture, &base);
    assert!(error.contains("anchor Cargo.lock"), "{error}");
    assert!(error.contains("declared version"), "{error}");
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_work_tree_lockfile_that_does_not_resolve_the_binary_is_an_error() {
    let fixture = locked_workspace();
    let base = fixture.sha("HEAD");
    fixture.write(
        "Cargo.lock",
        r#"version = 4

[[package]]
name = "widget"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"#,
    );

    let error = check_error(&fixture, &base);
    assert!(error.contains("work-tree Cargo.lock"), "{error}");
    assert!(error.contains("declared version"), "{error}");
}

fn check_error(fixture: &Fixture, base: &str) -> String {
    crate::harness::check_result(fixture, base)
        .expect_err("classification must stop when a released closure is unavailable")
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn registry_and_path_collisions_preserve_only_the_installed_source() {
    for install_path in [false, true] {
        let fixture = Fixture::new("");
        let registry = "{ package = \"foo\", version = \"1\" }";
        let path = "{ package = \"foo\", version = \"1.0.0\", path = \"../foo\" }";
        let (installed, development, installed_leaf, dev_leaf) = if install_path {
            (path, registry, "path-leaf", "registry-leaf")
        } else {
            (registry, path, "registry-leaf", "path-leaf")
        };
        let dependencies = format!(
            "[dependencies]\nselected = {installed}\n\
             helper = {{ path = \"../helper\", version = \"0.1.0\" }}\n\
             [dev-dependencies]\ndevelopment = {development}\n"
        );
        write_binary_package(&fixture, "tool", "0.1.0", &dependencies);
        write_package(
            &fixture,
            "helper",
            "0.1.0",
            &format!(
                "publish = false\n[dependencies]\nselected = {installed}\n\
                 [dev-dependencies]\ndevelopment = {development}\n"
            ),
        );
        write_package(
            &fixture,
            "foo",
            "1.0.0",
            "publish = false\n[dependencies]\npath-leaf = \"1\"",
        );
        let lockfile = r#"version = 4
[[package]]
name = "tool"
version = "0.1.0"
dependencies = ["helper", "foo 1.0.0", "foo 1.0.0 (registry+https://github.com/rust-lang/crates.io-index)"]
[[package]]
name = "helper"
version = "0.1.0"
dependencies = ["foo 1.0.0", "foo 1.0.0 (registry+https://github.com/rust-lang/crates.io-index)"]
[[package]]
name = "foo"
version = "1.0.0"
dependencies = ["path-leaf"]
[[package]]
name = "foo"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
dependencies = ["registry-leaf"]
[[package]]
name = "path-leaf"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
[[package]]
name = "registry-leaf"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"#;
        fixture.write("Cargo.lock", lockfile);
        fixture.commit("seed colliding installation and development sources");
        let base = fixture.sha("HEAD");

        fixture.write("Cargo.lock", &move_locked_package(lockfile, dev_leaf));
        let (passed, message) = check(&fixture, &base);
        assert!(passed, "{message}");
        assert_no_lockfile_changes(&fixture, &base);

        fixture.write("Cargo.lock", &move_locked_package(lockfile, installed_leaf));
        assert_dependency_change(&fixture, &base, installed_leaf, "modified");
    }
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn direct_paths_select_exact_endpoint_versions_across_excluded_and_workspace_packages() {
    for install_workspace in [false, true] {
        let (directory, local_path, development_path, installed_leaf, dev_leaf, version, next) =
            if install_workspace {
                (
                    "packages/foo",
                    "../foo",
                    "../../external/foo",
                    "workspace-leaf",
                    "external-leaf",
                    "1.1.0",
                    "1.1.1",
                )
            } else {
                (
                    "external/foo",
                    "../../external/foo",
                    "../foo",
                    "external-leaf",
                    "workspace-leaf",
                    "1.0.0",
                    "1.0.1",
                )
            };
        let fixture = Fixture::new(&format!(
            "exclude = [\"external/foo\"]\n\
             [workspace.dependencies]\nselected = {{ package = \"foo\", path = \"{directory}\", version = \"1\" }}\n"
        ));
        write_binary_package(
            &fixture,
            "tool",
            "0.1.0",
            &format!(
                "[dependencies]\nselected = {{ package = \"foo\", path = \"{local_path}\", version = \"1\" }}\n\
                 helper = {{ path = \"../helper\", version = \"0.1.0\" }}\n\
                 [dev-dependencies]\ndevelopment = {{ package = \"foo\", path = \"{development_path}\" }}\n"
            ),
        );
        write_package(
            &fixture,
            "helper",
            "0.1.0",
            &format!(
                "publish = false\n[dependencies]\nselected.workspace = true\n\
                 [dev-dependencies]\ndevelopment = {{ package = \"foo\", path = \"{development_path}\" }}\n"
            ),
        );
        write_package(
            &fixture,
            "foo",
            "1.1.0",
            "publish = false\n[dependencies]\nworkspace-leaf = \"1\"",
        );
        fixture.write(
            "external/foo/Cargo.toml",
            "[package]\nname = \"foo\"\nversion = \"1.0.0\"\nedition = \"2021\"\n\
             [dependencies]\nexternal-leaf = \"1\"\n",
        );
        fixture.write("external/foo/src/lib.rs", "pub fn f() {}\n");
        let lockfile = r#"version = 4
[[package]]
name = "tool"
version = "0.1.0"
dependencies = ["helper", "foo 1.0.0", "foo 1.1.0"]
[[package]]
name = "helper"
version = "0.1.0"
dependencies = ["foo 1.0.0", "foo 1.1.0"]
[[package]]
name = "foo"
version = "1.0.0"
dependencies = ["external-leaf"]
[[package]]
name = "foo"
version = "1.1.0"
dependencies = ["workspace-leaf"]
[[package]]
name = "external-leaf"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
[[package]]
name = "workspace-leaf"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"#;
        fixture.write("Cargo.lock", lockfile);
        fixture.commit("seed distinct path packages satisfying the same requirement");
        let base = fixture.sha("HEAD");

        fixture.write("Cargo.lock", &move_locked_package(lockfile, dev_leaf));
        assert_no_lockfile_changes(&fixture, &base);

        fixture.write("Cargo.lock", &move_locked_package(lockfile, installed_leaf));
        assert_dependency_change(&fixture, &base, installed_leaf, "modified");

        let manifest = format!("{directory}/Cargo.toml");
        fixture.write(
            &manifest,
            &fixture.read(&manifest).replace(
                &format!("version = \"{version}\""),
                &format!("version = \"{next}\""),
            ),
        );
        fixture.write(
            "Cargo.lock",
            &lockfile
                .replace(&format!("foo {version}"), &format!("foo {next}"))
                .replace(
                    &format!("name = \"foo\"\nversion = \"{version}\""),
                    &format!("name = \"foo\"\nversion = \"{next}\""),
                ),
        );
        assert_dependency_change(&fixture, &base, "foo", "modified");
    }
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn an_unavailable_direct_path_identity_fails_at_either_endpoint() {
    for missing_at_anchor in [false, true] {
        let fixture = Fixture::new("exclude = [\"external/foo\"]");
        write_binary_package(
            &fixture,
            "tool",
            "0.1.0",
            "[dependencies]\nfoo = { path = \"../../external/foo\", version = \"1\" }\n",
        );
        fixture.write(
            "external/foo/Cargo.toml",
            "[package]\nname = \"foo\"\nversion = \"1.0.0\"\nedition = \"2021\"\n",
        );
        fixture.write("external/foo/src/lib.rs", "pub fn f() {}\n");
        fixture.write(
            "Cargo.lock",
            "version = 4\n[[package]]\nname = \"tool\"\nversion = \"0.1.0\"\n\
             dependencies = [\"foo\"]\n[[package]]\nname = \"foo\"\nversion = \"1.0.0\"\n",
        );
        if missing_at_anchor {
            fixture.write(".gitignore", "external/foo/Cargo.toml\n");
        }
        fixture.commit("seed a direct path declaration");
        let base = fixture.sha("HEAD");
        if missing_at_anchor {
            fixture.git(&["add", "-f", "external/foo/Cargo.toml"]);
        } else {
            fixture.git(&["rm", "--cached", "external/foo/Cargo.toml"]);
        }
        _ = crate::harness::check_result(&fixture, &base).unwrap_err();
    }
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn unavailable_library_paths_do_not_block_an_unrelated_binary_closure() {
    let fixture = Fixture::new("exclude = [\"external/foo\"]");
    write_binary_package(&fixture, "tool", "0.1.0", "");
    write_package(
        &fixture,
        "library",
        "0.1.0",
        "[dependencies]\nfoo = { path = \"../../external/foo\", version = \"1\" }\n",
    );
    fixture.write(
        "external/foo/Cargo.toml",
        "[package]\nname = \"foo\"\nversion = \"1.0.0\"\nedition = \"2021\"\n",
    );
    fixture.write("external/foo/src/lib.rs", "pub fn f() {}\n");
    fixture.write(".gitignore", "external/\n");
    fixture.write(
        "Cargo.lock",
        "version = 4\n[[package]]\nname = \"tool\"\nversion = \"0.1.0\"\n",
    );
    fixture.commit("seed an unrelated binary beside a library with untracked path inputs");
    let base = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn historical_path_read_errors_only_block_binaries_that_reach_them() {
    for (binary, consumes_library) in [(false, false), (true, false), (true, true)] {
        let fixture = Fixture::new("exclude = [\"external/foo\"]");
        write_package(
            &fixture,
            "library",
            "0.1.0",
            "[dependencies]\nfoo = { path = \"../../external/foo\", version = \"1\" }\n",
        );
        if binary {
            let dependencies = if consumes_library {
                "[dependencies]\nlibrary = { path = \"../library\", version = \"0.1.0\" }\n"
            } else {
                ""
            };
            write_binary_package(&fixture, "tool", "0.1.0", dependencies);
            let tool_dependencies = if consumes_library {
                "dependencies = [\"library\"]\n"
            } else {
                ""
            };
            fixture.write("Cargo.lock", &format!(
                "version = 4\n[[package]]\nname = \"tool\"\nversion = \"0.1.0\"\n{tool_dependencies}\
                 [[package]]\nname = \"library\"\nversion = \"0.1.0\"\ndependencies = [\"foo\"]\n\
                 [[package]]\nname = \"foo\"\nversion = \"1.0.0\"\n"
            ));
        }
        fixture.write("external/foo/Cargo.toml", "not = = toml");
        fixture.write("external/foo/src/lib.rs", "pub fn f() {}\n");
        fixture.commit("seed an unavailable historical installation identity");
        let base = fixture.sha("HEAD");
        fixture.write(
            "external/foo/Cargo.toml",
            "[package]\nname = \"foo\"\nversion = \"1.0.0\"\nedition = \"2021\"\n",
        );

        if consumes_library {
            _ = crate::harness::check_result(&fixture, &base).unwrap_err();
        } else {
            let (passed, message) = check(&fixture, &base);
            assert!(passed, "{message}");
            fixture.write("packages/library/src/lib.rs", "pub fn changed() {}\n");
            let (passed, message) = check(&fixture, &base);
            assert!(!passed, "{message}");
            assert!(message.contains("library: needs-increment"), "{message}");
        }
    }
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn historical_installation_declaration_errors_do_not_replace_library_assessment() {
    for (binary, consumes_library) in [(false, false), (true, false), (true, true)] {
        let fixture = Fixture::new("");
        write_package(
            &fixture,
            "library",
            "0.1.0",
            "[dependencies]\nwidget = \"not a version requirement\"\n",
        );
        if binary {
            let dependencies = if consumes_library {
                "[dependencies]\nlibrary = { path = \"../library\", version = \"0.1.0\" }\n"
            } else {
                ""
            };
            write_binary_package(&fixture, "tool", "0.1.0", dependencies);
            let tool_dependencies = if consumes_library {
                "dependencies = [\"library\"]\n"
            } else {
                ""
            };
            fixture.write("Cargo.lock", &format!(
                "version = 4\n[[package]]\nname = \"tool\"\nversion = \"0.1.0\"\n{tool_dependencies}\
                 [[package]]\nname = \"library\"\nversion = \"0.1.0\"\ndependencies = [\"widget\"]\n\
                 [[package]]\nname = \"widget\"\nversion = \"1.0.0\"\n\
                 source = \"registry+https://github.com/rust-lang/crates.io-index\"\n"
            ));
        }
        fixture.commit("seed historical installation declarations");
        let base = fixture.sha("HEAD");
        write_package(
            &fixture,
            "library",
            "0.1.0",
            "[dependencies]\nwidget = \"1\"\n",
        );

        if consumes_library {
            _ = crate::harness::check_result(&fixture, &base).unwrap_err();
        } else {
            let (passed, message) = check(&fixture, &base);
            assert!(!passed, "{message}");
            assert!(message.contains("library: needs-increment"), "{message}");
            assert_no_lockfile_changes(&fixture, &base);
        }
    }
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn git_sources_and_patches_do_not_import_development_revisions() {
    for patched in [false, true] {
        let git = "{ package = \"foo\", git = \"https://example.invalid/foo\", branch = \"release/next\" }";
        let workspace = if patched {
            format!("[patch.crates-io]\ninstalled = {git}\n")
        } else {
            String::new()
        };
        let fixture = Fixture::new(&workspace);
        let installed = if patched {
            "{ package = \"foo\", version = \"1\" }"
        } else {
            git
        };
        let dependencies = format!(
            "[dependencies]\ninstalled = {installed}\n\
             [dev-dependencies]\n\
             development = {{ package = \"foo\", git = \"https://example.invalid/foo.git\", branch = \"development\" }}\n"
        );
        write_binary_package(&fixture, "tool", "0.1.0", &dependencies);
        let lockfile = r#"version = 4
[[package]]
name = "tool"
version = "0.1.0"
dependencies = [
 "foo 1.0.0 (git+https://example.invalid/foo?branch=release%2Fnext#aaaa)",
 "foo 1.0.0 (git+https://example.invalid/foo.git?branch=development#bbbb)",
]
[[package]]
name = "foo"
version = "1.0.0"
source = "git+https://example.invalid/foo?branch=release%2Fnext#aaaa"
[[package]]
name = "foo"
version = "1.0.0"
source = "git+https://example.invalid/foo.git?branch=development#bbbb"
"#;
        fixture.write("Cargo.lock", lockfile);
        fixture.commit("seed colliding Git references");
        let base = fixture.sha("HEAD");

        fixture.write("Cargo.lock", &lockfile.replace("#bbbb", "#cccc"));
        let (passed, message) = check(&fixture, &base);
        assert!(passed, "{message}");
        assert_no_lockfile_changes(&fixture, &base);

        fixture.write("Cargo.lock", &lockfile.replace("#aaaa", "#dddd"));
        assert_dependency_change(&fixture, &base, "foo", "modified");
    }
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn named_registry_sources_use_each_endpoints_configuration() {
    let fixture = Fixture::new("");
    fixture.write(
        ".cargo/config",
        "[registries.private]\nindex = \"https://example.invalid/original-index\"\n",
    );
    fixture.write(
        ".cargo/config.toml",
        "[registries.private]\nindex = \"https://example.invalid/ignored-index\"\n",
    );
    write_binary_package(
        &fixture,
        "tool",
        "0.1.0",
        "[dependencies]\ninstalled = { package = \"foo\", version = \"1\", registry = \"private\" }\n\
         [dev-dependencies]\nfoo = \"1\"\n",
    );
    let lockfile = r#"version = 4
[[package]]
name = "tool"
version = "0.1.0"
dependencies = [
 "foo 1.0.0 (registry+https://example.invalid/original-index)",
 "foo 1.0.0 (registry+https://github.com/rust-lang/crates.io-index)",
]
[[package]]
name = "foo"
version = "1.0.0"
source = "registry+https://example.invalid/original-index"
[[package]]
name = "foo"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
dependencies = ["dev-leaf"]
[[package]]
name = "dev-leaf"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"#;
    fixture.write("Cargo.lock", lockfile);
    fixture.commit("seed distinct registry sources");
    let base = fixture.sha("HEAD");

    fixture.write("Cargo.lock", &move_locked_package(lockfile, "dev-leaf"));
    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
    assert_no_lockfile_changes(&fixture, &base);

    fixture.write(
        ".cargo/config",
        "[registries.private]\nindex = \"https://example.invalid/current-index\"\n",
    );
    fixture.write(
        "Cargo.lock",
        &lockfile.replace("original-index", "current-index"),
    );
    assert_dependency_change(&fixture, &base, "foo", "modified");
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn sparse_and_classic_registry_sources_remain_distinct() {
    let fixture = Fixture::new("");
    fixture.write(
        ".cargo/config.toml",
        "[registries.sparse]\nindex = \"sparse+https://example.invalid/index/\"\n\
         [registries.classic]\nindex = \"https://example.invalid/index/\"\n",
    );
    write_binary_package(
        &fixture,
        "tool",
        "0.1.0",
        "[dependencies]\ninstalled = { package = \"foo\", version = \"1\", registry = \"sparse\" }\n\
         [dev-dependencies]\ndevelopment = { package = \"foo\", version = \"1\", registry = \"classic\" }\n",
    );
    let lockfile = r#"version = 4
[[package]]
name = "tool"
version = "0.1.0"
dependencies = [
 "foo 1.0.0 (sparse+https://example.invalid/index/)",
 "foo 1.0.0 (registry+https://example.invalid/index/)",
]
[[package]]
name = "foo"
version = "1.0.0"
source = "sparse+https://example.invalid/index/"
dependencies = ["installed-leaf"]
[[package]]
name = "foo"
version = "1.0.0"
source = "registry+https://example.invalid/index/"
dependencies = ["dev-leaf"]
[[package]]
name = "installed-leaf"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
[[package]]
name = "dev-leaf"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"#;
    fixture.write("Cargo.lock", lockfile);
    fixture.commit("seed registry kinds sharing an index URL");
    let base = fixture.sha("HEAD");

    fixture.write("Cargo.lock", &move_locked_package(lockfile, "dev-leaf"));
    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
    assert_no_lockfile_changes(&fixture, &base);

    fixture.write(
        "Cargo.lock",
        &move_locked_package(lockfile, "installed-leaf"),
    );
    assert_dependency_change(&fixture, &base, "installed-leaf", "modified");
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn workspace_path_patches_use_the_replacement_manifest_identity() {
    let fixture = Fixture::new(
        "[patch.crates-io]\nreplacement = { package = \"foo\", path = \"patches/foo\" }\n",
    );
    write_binary_package(
        &fixture,
        "tool",
        "0.1.0",
        "[dependencies]\nfoo = \"1\"\n\
         [dev-dependencies]\nlocal = { package = \"foo\", path = \"../foo\" }\n",
    );
    write_package(
        &fixture,
        "foo",
        "1.0.0",
        "publish = false\n[dependencies]\ndev-leaf = \"1\"",
    );
    fixture.write(
        "patches/foo/Cargo.toml",
        "[package]\nname = \"foo\"\nversion = \"1.1.0\"\nedition = \"2021\"\n\
         [dependencies]\ninstalled-leaf = \"1\"\n",
    );
    fixture.write("patches/foo/src/lib.rs", "pub fn f() {}\n");
    let lockfile = r#"version = 4
[[package]]
name = "tool"
version = "0.1.0"
dependencies = ["foo 1.0.0", "foo 1.1.0"]
[[package]]
name = "foo"
version = "1.0.0"
dependencies = ["dev-leaf"]
[[package]]
name = "foo"
version = "1.1.0"
dependencies = ["installed-leaf"]
[[package]]
name = "dev-leaf"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
[[package]]
name = "installed-leaf"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"#;
    fixture.write("Cargo.lock", lockfile);
    fixture.commit("seed a path patch beside a distinct development path package");
    let base = fixture.sha("HEAD");

    fixture.write("Cargo.lock", &move_locked_package(lockfile, "dev-leaf"));
    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
    assert_no_lockfile_changes(&fixture, &base);

    fixture.write(
        "Cargo.lock",
        &move_locked_package(lockfile, "installed-leaf"),
    );
    assert_dependency_change(&fixture, &base, "installed-leaf", "modified");
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn unavailable_git_normalization_fails_closed_for_direct_and_patched_edges() {
    for patched in [false, true] {
        let workspace = if patched {
            "[patch.'https://example.invalid/a/../foo']\nfoo = { path = \"packages/foo\" }\n"
        } else {
            ""
        };
        let fixture = Fixture::new(workspace);
        let repository = if patched {
            "https://example.invalid/foo"
        } else {
            "https://example.invalid/a/../foo"
        };
        write_binary_package(
            &fixture,
            "tool",
            "0.1.0",
            &format!("[dependencies]\nfoo = {{ git = \"{repository}\", version = \"1\" }}\n"),
        );
        let source = if patched {
            write_package(&fixture, "foo", "1.0.0", "publish = false");
            ""
        } else {
            "source = \"git+https://example.invalid/foo#aaaa\"\n"
        };
        let lockfile = format!(
            "version = 4\n[[package]]\nname = \"tool\"\nversion = \"0.1.0\"\n\
             dependencies = [\"foo\"]\n\
             [[package]]\nname = \"foo\"\nversion = \"1.0.0\"\n{source}"
        );
        fixture.write("Cargo.lock", &lockfile);
        fixture.commit("seed a source requiring unsupported URL normalization");
        let base = fixture.sha("HEAD");

        _ = crate::harness::check_result(&fixture, &base).unwrap_err();
        assert_eq!(fixture.read("Cargo.lock"), lockfile);
    }
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn an_unavailable_historical_registry_mapping_fails_closed() {
    // The anchor's ambient registry configuration is not stored in this repository.
    // Its name cannot be reconstructed from a lockfile URL alone.
    let fixture = Fixture::new("");
    write_binary_package(
        &fixture,
        "tool",
        "0.1.0",
        "[dependencies]\nfoo = { version = \"1\", registry = \"historical\" }\n",
    );
    let lockfile = "version = 4\n[[package]]\nname = \"tool\"\nversion = \"0.1.0\"\n\
        dependencies = [\"foo\"]\n\
        [[package]]\nname = \"foo\"\nversion = \"1.0.0\"\n\
        source = \"registry+https://example.invalid/historical-index\"\n";
    fixture.write("Cargo.lock", lockfile);
    fixture.commit("seed an anchor requiring unavailable ambient configuration");
    let base = fixture.sha("HEAD");

    write_binary_package(&fixture, "tool", "0.1.0", "[dependencies]\nfoo = \"1\"\n");
    fixture.write(
        "Cargo.lock",
        &lockfile.replace(
            "https://example.invalid/historical-index",
            "https://github.com/rust-lang/crates.io-index",
        ),
    );
    _ = crate::harness::check_result(&fixture, &base).unwrap_err();
}

#[cfg_attr(miri, ignore = "Spawns git and cargo and reads fixture files.")]
#[test]
fn legacy_build_tables_are_installed_and_legacy_development_tables_are_not() {
    let fixture = Fixture::new("");
    write_binary_package(
        &fixture,
        "tool",
        "0.1.0",
        "[build_dependencies]\nroot-builder = \"1\"\n\
         [target.'cfg(unix)'.build_dependencies]\ntarget-builder = \"1\"\n\
         [dev_dependencies]\nroot-dev = \"1\"\nignored-builder = \"1\"\n\
         [target.'cfg(windows)'.dev_dependencies]\ntarget-dev = \"1\"\n\
         [target.'cfg(windows)'.build_dependencies]\nignored-builder = \"1\"\n\
         [target.'cfg(windows)'.build-dependencies]\n",
    );
    let mut lockfile = "version = 4\n[[package]]\nname = \"tool\"\nversion = \"0.1.0\"\n\
        dependencies = [\"root-builder\", \"target-builder\", \"root-dev\", \"target-dev\", \"ignored-builder\"]\n".to_owned();
    for name in [
        "root-builder",
        "target-builder",
        "root-dev",
        "target-dev",
        "ignored-builder",
    ] {
        write!(
            lockfile,
            "[[package]]\nname = \"{name}\"\nversion = \"1.0.0\"\n\
             source = \"registry+https://github.com/rust-lang/crates.io-index\"\n"
        )
        .unwrap();
    }
    fixture.write("Cargo.lock", &lockfile);
    fixture.commit("seed legacy dependency spellings");
    let base = fixture.sha("HEAD");

    let development = ["root-dev", "target-dev", "ignored-builder"]
        .into_iter()
        .fold(lockfile.clone(), |lockfile, name| {
            move_locked_package(&lockfile, name)
        });
    fixture.write("Cargo.lock", &development);
    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
    assert_no_lockfile_changes(&fixture, &base);

    for builder in ["root-builder", "target-builder"] {
        fixture.write("Cargo.lock", &move_locked_package(&lockfile, builder));
        assert_dependency_change(&fixture, &base, builder, "modified");
    }
}

fn move_locked_package(lockfile: &str, name: &str) -> String {
    lockfile.replace(
        &format!("name = \"{name}\"\nversion = \"1.0.0\""),
        &format!("name = \"{name}\"\nversion = \"1.0.1\""),
    )
}

fn assert_lockfile_change(fixture: &Fixture, base: &str, change: &str) {
    assert_dependency_change(fixture, base, "widget", change);
}

fn assert_dependency_change(fixture: &Fixture, base: &str, dependency: &str, change: &str) {
    let report: Value = serde_json::from_str(&report_json(fixture, base)).unwrap();
    let changed = report
        .get("packages")
        .and_then(Value::as_array)
        .unwrap()
        .iter()
        .find(|package| package.get("name").and_then(Value::as_str) == Some("tool"))
        .and_then(|package| package.get("changed"))
        .and_then(Value::as_array)
        .unwrap();

    assert!(
        changed.contains(&json!({
            "dependency": dependency,
            "change": change,
            "source": "lockfile"
        })),
        "{changed:?}"
    );
}

fn assert_no_lockfile_changes(fixture: &Fixture, base: &str) {
    let report: Value = serde_json::from_str(&report_json(fixture, base)).unwrap();
    for package in report.get("packages").and_then(Value::as_array).unwrap() {
        assert!(
            package
                .get("changed")
                .and_then(Value::as_array)
                .unwrap()
                .iter()
                .all(|changed| changed.get("source").and_then(Value::as_str) != Some("lockfile")),
            "{package}"
        );
    }
}
