// Attribution of inherited workspace values to packages.
//
// A root-manifest edit is in scope for a package only when that package actually
// inherits the changed `[workspace.package]` key or `[workspace.dependencies]`
// entry. `[workspace.lints]` is out of scope.
// Ref: docs/design.md, "Inherited workspace values".

use std::collections::{BTreeMap, BTreeSet};

use toml_edit::{DocumentMut, Item, Value};

use crate::manifest::for_each_dependency_table;

/// One inherited field that changed between the package's anchor and the work tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InheritedChange {
    pub(crate) field: String,
}

/// Package-level keys inherited via `.workspace = true`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct InheritedKeys {
    pub(crate) package: Vec<String>,
    pub(crate) dependencies: Vec<String>,
    /// Dependencies inherited exclusively through dev-dependency tables.
    ///
    /// Cargo omits these from a published manifest while their shared workspace
    /// declarations remain versionless.
    dev_only_dependencies: Vec<String>,
}

/// Compares inherited workspace values at the package's anchor vs the work tree.
pub(crate) fn inherited_changes(
    keys: &InheritedKeys,
    workspace_at_anchor: &DocumentMut,
    workspace_at_work_tree: &DocumentMut,
) -> Vec<InheritedChange> {
    let mut changes = Vec::new();

    for key in &keys.package {
        let old = workspace_package_value(workspace_at_anchor, key);
        let new = workspace_package_value(workspace_at_work_tree, key);
        if old != new {
            changes.push(InheritedChange {
                field: format!("workspace.package.{key}"),
            });
        }
    }

    for dep in &keys.dependencies {
        let old = workspace_dependency_fields(workspace_at_anchor, dep);
        let new = workspace_dependency_fields(workspace_at_work_tree, dep);
        if keys.dev_only_dependencies.binary_search(dep).is_ok()
            && !old.contains_key("version")
            && !new.contains_key("version")
        {
            continue;
        }
        if old == new {
            continue;
        }
        let names: Vec<String> = old
            .keys()
            .chain(new.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        for field in &names {
            if old.get(field) != new.get(field) {
                changes.push(InheritedChange {
                    field: format!("workspace.dependencies.{dep}.{field}"),
                });
            }
        }
    }

    changes
}

/// Collects `.workspace = true` keys from a package manifest.
pub(crate) fn collect_inherited_keys(doc: &DocumentMut) -> InheritedKeys {
    let mut keys = InheritedKeys::default();
    if let Some(package) = doc.get("package").and_then(Item::as_table_like) {
        for (key, item) in package.iter() {
            if is_workspace_inherit(item) {
                keys.package.push(key.to_string());
            }
        }
    }
    let mut dependency_usage = BTreeMap::new();
    for_each_dependency_table(doc.as_table(), &mut |kind, dependencies| {
        for (name, item) in dependencies.iter() {
            if !is_workspace_inherit(item) {
                continue;
            }
            let is_dev = kind == "dev-dependencies";
            dependency_usage
                .entry(name.to_string())
                .and_modify(|dev_only| *dev_only &= is_dev)
                .or_insert(is_dev);
        }
    });
    keys.package.sort();
    keys.package.dedup();
    keys.dependencies = dependency_usage.keys().cloned().collect();
    keys.dev_only_dependencies = dependency_usage
        .into_iter()
        .filter_map(|(name, dev_only)| dev_only.then_some(name))
        .collect();
    keys
}

pub(crate) fn is_workspace_inherit(item: &Item) -> bool {
    match item {
        Item::Value(Value::InlineTable(table)) => table
            .get("workspace")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        Item::Table(table) => table
            .get("workspace")
            .and_then(Item::as_bool)
            .unwrap_or(false),
        _ => false,
    }
}

fn workspace_package_value(doc: &DocumentMut, key: &str) -> Option<CanonicalValue> {
    let package = doc
        .get("workspace")?
        .as_table_like()?
        .get("package")?
        .as_table_like()?;
    Some(canonical_item(package.get(key)?))
}

fn workspace_dependency_fields(doc: &DocumentMut, name: &str) -> BTreeMap<String, CanonicalValue> {
    let mut fields = BTreeMap::new();
    let Some(deps) = doc
        .get("workspace")
        .and_then(Item::as_table_like)
        .and_then(|workspace| workspace.get("dependencies"))
        .and_then(Item::as_table_like)
    else {
        return fields;
    };
    let Some(item) = deps.get(name) else {
        return fields;
    };
    match item {
        Item::Value(Value::String(text)) => {
            fields.insert(
                "version".to_string(),
                CanonicalValue::Text(text.value().clone()),
            );
        }
        Item::Value(Value::InlineTable(table)) => {
            for (key, value) in table {
                if is_unpublished_dependency_key(key) {
                    continue;
                }
                fields.insert(key.to_string(), canonical_value(value));
            }
        }
        Item::Table(table) => {
            for (key, value) in table {
                if is_unpublished_dependency_key(key) {
                    continue;
                }
                fields.insert(key.to_string(), canonical_item(value));
            }
        }
        other => {
            fields.insert("value".to_string(), canonical_item(other));
        }
    }
    fields
}

/// Whether a `[workspace.dependencies]` key is stripped from published manifests.
///
/// Cargo removes `path` when it normalises a member's manifest for packaging,
/// and the root manifest is not shipped at all, so a root edit that only moves a
/// path leaves every inheriting package's released content byte-identical.
/// Attributing it would be a false `needs-increment` verdict. Ref:
/// docs/design.md, "Inherited workspace values".
fn is_unpublished_dependency_key(key: &str) -> bool {
    key == "path"
}

/// A TOML value reduced to the structure that matters for change detection.
///
/// Comparison drives whether an inherited value changed, so the representation
/// must keep values of different shapes distinguishable: a flattened text
/// rendering would make `["a,b"]` and `["a", "b"]` compare equal and hide a real
/// root-manifest edit. Formatting, comments, and key order are deliberately
/// discarded because they do not alter what a package inherits.
#[derive(Clone, Debug, Eq, PartialEq)]
enum CanonicalValue {
    Text(String),
    Integer(i64),
    /// Raw bits, so that equal values compare equal without float ordering rules.
    Float(u64),
    Boolean(bool),
    Datetime(String),
    Array(Vec<Self>),
    Table(BTreeMap<String, Self>),
    ArrayOfTables(Vec<Self>),
    Absent,
}

fn canonical_item(item: &Item) -> CanonicalValue {
    match item {
        Item::Value(value) => canonical_value(value),
        Item::Table(table) => CanonicalValue::Table(
            table
                .iter()
                .map(|(key, value)| (key.to_string(), canonical_item(value)))
                .collect(),
        ),
        Item::ArrayOfTables(array) => CanonicalValue::ArrayOfTables(
            array
                .iter()
                .map(|table| {
                    CanonicalValue::Table(
                        table
                            .iter()
                            .map(|(key, value)| (key.to_string(), canonical_item(value)))
                            .collect(),
                    )
                })
                .collect(),
        ),
        Item::None => CanonicalValue::Absent,
    }
}

fn canonical_value(value: &Value) -> CanonicalValue {
    match value {
        Value::String(text) => CanonicalValue::Text(text.value().clone()),
        Value::Integer(number) => CanonicalValue::Integer(*number.value()),
        Value::Float(number) => CanonicalValue::Float(number.value().to_bits()),
        Value::Boolean(flag) => CanonicalValue::Boolean(*flag.value()),
        Value::Datetime(stamp) => CanonicalValue::Datetime(stamp.value().to_string()),
        Value::Array(array) => CanonicalValue::Array(array.iter().map(canonical_value).collect()),
        Value::InlineTable(table) => CanonicalValue::Table(
            table
                .iter()
                .map(|(key, value)| (key.to_string(), canonical_value(value)))
                .collect(),
        ),
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    fn doc(text: &str) -> DocumentMut {
        text.parse().unwrap()
    }

    #[test]
    fn collects_workspace_inherited_package_and_dep_keys() {
        let package = doc(r#"
[package]
name = "foo"
edition.workspace = true
license.workspace = true

[dependencies]
bar.workspace = true
semver = "1.0"
"#);
        let keys = collect_inherited_keys(&package);
        assert_eq!(keys.package, vec!["edition", "license"]);
        assert_eq!(keys.dependencies, vec!["bar"]);
    }

    #[test]
    fn inline_table_workspace_true_is_inherited() {
        let package = doc(r#"
[package]
name = "foo"
version = "0.1.0"

[dependencies]
bar = { workspace = true }
semver = { version = "1.0.0" }
"#);
        let keys = collect_inherited_keys(&package);
        assert_eq!(keys.dependencies, vec!["bar"]);
    }

    #[test]
    fn distinct_array_shapes_are_not_conflated() {
        // A flattened text rendering would make these compare equal and hide a
        // real change to an inherited dependency's feature list.
        let keys = InheritedKeys {
            package: vec![],
            dependencies: vec!["bar".to_string()],
            ..InheritedKeys::default()
        };
        let old =
            doc("[workspace.dependencies]\nbar = { version = \"1\", features = [\"a,b\"] }\n");
        let new =
            doc("[workspace.dependencies]\nbar = { version = \"1\", features = [\"a\", \"b\"] }\n");
        let changes = inherited_changes(&keys, &old, &new);
        assert_eq!(
            changes,
            vec![InheritedChange {
                field: "workspace.dependencies.bar.features".to_string(),
            }]
        );
    }

    #[test]
    fn attributes_changed_workspace_package_key() {
        let keys = InheritedKeys {
            package: vec!["edition".to_string()],
            dependencies: vec![],
            ..InheritedKeys::default()
        };
        let old = doc("[workspace.package]\nedition = \"2021\"\n");
        let new = doc("[workspace.package]\nedition = \"2024\"\n");
        let changes = inherited_changes(&keys, &old, &new);
        assert_eq!(
            changes,
            vec![InheritedChange {
                field: "workspace.package.edition".to_string(),
            }]
        );
    }

    #[test]
    fn ignores_uninherited_workspace_package_key() {
        let keys = InheritedKeys::default();
        let old = doc("[workspace.package]\nedition = \"2021\"\n");
        let new = doc("[workspace.package]\nedition = \"2024\"\n");
        assert!(inherited_changes(&keys, &old, &new).is_empty());
    }

    #[test]
    fn attributes_changed_workspace_dependency_version() {
        let keys = InheritedKeys {
            package: vec![],
            dependencies: vec!["bar".to_string()],
            ..InheritedKeys::default()
        };
        let old = doc(
            "[workspace.dependencies]\nbar = { version = \"1.0.0\", path = \"packages/bar\" }\n",
        );
        let new = doc(
            "[workspace.dependencies]\nbar = { version = \"1.1.0\", path = \"packages/bar\" }\n",
        );
        let changes = inherited_changes(&keys, &old, &new);
        assert_eq!(
            changes,
            vec![InheritedChange {
                field: "workspace.dependencies.bar.version".to_string(),
            }]
        );
    }

    #[test]
    fn formatting_only_workspace_value_is_not_a_change() {
        let keys = InheritedKeys {
            package: vec!["edition".to_string()],
            dependencies: vec![],
            ..InheritedKeys::default()
        };
        let old = doc("[workspace.package]\nedition = \"2024\"\n");
        let new = doc("[workspace.package]\n  edition = \"2024\"\n");
        assert!(inherited_changes(&keys, &old, &new).is_empty());
    }

    /// Every toml shape canonicalizes distinctly.
    ///
    /// Every TOML shape an inherited value can take must round-trip into a distinct canonical form,
    /// because comparison of those forms is the only thing that decides whether an inherited value
    /// changed. The sample payloads are arbitrary representatives: only the TOML type of each entry
    /// selects the canonical form under test.
    #[test]
    fn every_toml_shape_canonicalizes_distinctly() {
        let document = doc(concat!(
            "[workspace.package]\n",
            "text = \"a\"\n",
            "integer = 1\n",
            "float = 1.5\n",
            "boolean = true\n",
            "datetime = 1979-05-27T07:32:00Z\n",
            "array = [1, 2]\n",
            "inline = { a = 1 }\n",
        ));

        let value = |key: &str| workspace_package_value(&document, key).unwrap();

        assert_eq!(value("text"), CanonicalValue::Text("a".to_string()));
        assert_eq!(value("integer"), CanonicalValue::Integer(1));
        assert_eq!(value("float"), CanonicalValue::Float(1.5_f64.to_bits()));
        assert_eq!(value("boolean"), CanonicalValue::Boolean(true));
        assert_eq!(
            value("datetime"),
            CanonicalValue::Datetime("1979-05-27T07:32:00Z".to_string())
        );
        assert_eq!(
            value("array"),
            CanonicalValue::Array(vec![CanonicalValue::Integer(1), CanonicalValue::Integer(2)])
        );
        assert_eq!(
            value("inline"),
            CanonicalValue::Table(BTreeMap::from([(
                "a".to_string(),
                CanonicalValue::Integer(1)
            )]))
        );
        assert_eq!(workspace_package_value(&document, "absent"), None);
    }

    #[test]
    fn a_missing_workspace_package_table_yields_no_value() {
        assert_eq!(workspace_package_value(&doc(""), "edition"), None);
        assert_eq!(
            workspace_package_value(&doc("[workspace]\nmembers = []\n"), "edition"),
            None
        );
    }

    /// Workspace dependencies canonicalize in every declaration form.
    ///
    /// Cargo accepts a workspace dependency as a bare version string, an inline table, or a full
    /// table, and each form carries the values a member inherits.
    #[test]
    fn workspace_dependencies_canonicalize_in_every_declaration_form() {
        let bare =
            workspace_dependency_fields(&doc("[workspace.dependencies]\nbar = \"1.0\"\n"), "bar");
        assert_eq!(
            bare.get("version"),
            Some(&CanonicalValue::Text("1.0".to_string()))
        );

        let inline = workspace_dependency_fields(
            &doc("[workspace.dependencies]\nbar = { version = \"1.0\", optional = true }\n"),
            "bar",
        );
        assert_eq!(
            inline.get("version"),
            Some(&CanonicalValue::Text("1.0".to_string()))
        );
        assert_eq!(inline.get("optional"), Some(&CanonicalValue::Boolean(true)));

        let table = workspace_dependency_fields(
            &doc("[workspace.dependencies.bar]\nversion = \"1.0\"\ndefault-features = false\n"),
            "bar",
        );
        assert_eq!(
            table.get("version"),
            Some(&CanonicalValue::Text("1.0".to_string()))
        );
        assert_eq!(
            table.get("default-features"),
            Some(&CanonicalValue::Boolean(false))
        );
    }

    /// Moving a workspace dependency path is not a change.
    ///
    /// Moving a workspace dependency's `path` does not alter what an inheriting package publishes.
    /// Ref:
    /// docs/design.md, "Inherited values".
    #[test]
    fn moving_an_inline_workspace_dependency_path_is_not_a_change() {
        let keys = InheritedKeys {
            package: vec![],
            dependencies: vec!["bar".to_string()],
            ..InheritedKeys::default()
        };
        let inline_old = doc(
            "[workspace.dependencies]\nbar = { version = \"1.0.0\", path = \"packages/bar\" }\n",
        );
        let inline_new =
            doc("[workspace.dependencies]\nbar = { version = \"1.0.0\", path = \"crates/bar\" }\n");
        assert!(inherited_changes(&keys, &inline_old, &inline_new).is_empty());
    }

    #[test]
    fn moving_a_table_workspace_dependency_path_is_not_a_change() {
        let keys = InheritedKeys {
            dependencies: vec!["bar".to_string()],
            ..InheritedKeys::default()
        };
        let table_old =
            doc("[workspace.dependencies.bar]\nversion = \"1.0.0\"\npath = \"packages/bar\"\n");
        let table_new =
            doc("[workspace.dependencies.bar]\nversion = \"1.0.0\"\npath = \"crates/bar\"\n");
        assert!(inherited_changes(&keys, &table_old, &table_new).is_empty());
    }

    #[test]
    fn a_manifest_without_inheritable_tables_yields_no_keys() {
        assert!(collect_inherited_keys(&doc("")).package.is_empty());

        let odd_target = doc("[target]\nnot-a-spec = 1\n");

        assert!(collect_inherited_keys(&odd_target).dependencies.is_empty());
    }

    /// Target gated tables contribute inherited dependencies.
    ///
    /// Cargo lets a package inherit a workspace dependency from a target-gated table, and those
    /// keys must be watched the same as unconditional ones.
    #[test]
    fn target_gated_tables_contribute_inherited_dependencies() {
        let package = doc(r#"
[package]
name = "foo"

[target.'cfg(unix)'.dependencies]
bar.workspace = true

[target.'cfg(windows)'.dev-dependencies]
baz.workspace = true
"#);
        let keys = collect_inherited_keys(&package);
        assert_eq!(keys.dependencies, vec!["bar", "baz"]);
        assert_eq!(keys.dev_only_dependencies, vec!["baz"]);
    }

    #[test]
    fn dependency_used_normally_and_for_development_is_not_dev_only() {
        let package = doc("[dependencies]\nbar.workspace = true\n\
             [dev-dependencies]\nbar.workspace = true\n");

        let keys = collect_inherited_keys(&package);

        assert_eq!(keys.dependencies, vec!["bar"]);
        assert!(keys.dev_only_dependencies.is_empty());
    }

    #[test]
    fn a_versionless_dev_dependency_change_is_not_published() {
        let keys = collect_inherited_keys(&doc("[dev-dependencies]\nbar.workspace = true\n"));
        let old = doc("[workspace.dependencies]\nbar = { path = \"old\", features = [\"a\"] }\n");
        let new = doc("[workspace.dependencies]\nbar = { path = \"new\", features = [\"b\"] }\n");

        assert!(inherited_changes(&keys, &old, &new).is_empty());
    }

    #[test]
    fn adding_or_removing_a_dev_dependency_version_is_published() {
        let keys = collect_inherited_keys(&doc("[dev-dependencies]\nbar.workspace = true\n"));
        let versionless = doc("[workspace.dependencies]\nbar = { path = \"packages/bar\" }\n");
        let versioned = doc(
            "[workspace.dependencies]\nbar = { path = \"packages/bar\", version = \"1.0.0\" }\n",
        );

        for (old, new) in [(&versionless, &versioned), (&versioned, &versionless)] {
            assert_eq!(
                inherited_changes(&keys, old, new),
                vec![InheritedChange {
                    field: "workspace.dependencies.bar.version".to_string(),
                }]
            );
        }
    }

    #[test]
    fn an_unchanged_workspace_dependency_is_not_a_change() {
        let keys = InheritedKeys {
            package: vec![],
            dependencies: vec!["bar".to_string()],
            ..InheritedKeys::default()
        };
        let old = doc("[workspace.dependencies]\nbar = { version = \"1.0.0\" }\n");
        let new = doc("# a comment\n[workspace.dependencies]\nbar = { version = \"1.0.0\" }\n");
        assert!(inherited_changes(&keys, &old, &new).is_empty());
    }

    /// An unusual workspace dependency shape still canonicalizes.
    ///
    /// A workspace dependency written in an unexpected shape still carries a value a member
    /// inherits, so it must canonicalize rather than vanish.
    #[test]
    fn an_unusual_workspace_dependency_shape_still_canonicalizes() {
        let fields = workspace_dependency_fields(
            &doc("[[workspace.dependencies.bar]]\nversion = \"1.0\"\n"),
            "bar",
        );

        assert_eq!(
            fields.get("value"),
            Some(&CanonicalValue::ArrayOfTables(vec![CanonicalValue::Table(
                BTreeMap::from([(
                    "version".to_string(),
                    CanonicalValue::Text("1.0".to_string())
                )])
            )]))
        );
    }

    #[test]
    fn a_nested_table_inside_a_workspace_dependency_canonicalizes() {
        let fields = workspace_dependency_fields(
            &doc(
                "[workspace.dependencies.bar]\nversion = \"1.0\"\n\n[workspace.dependencies.bar.nested]\nkey = \"value\"\n",
            ),
            "bar",
        );

        assert_eq!(
            fields.get("nested"),
            Some(&CanonicalValue::Table(BTreeMap::from([(
                "key".to_string(),
                CanonicalValue::Text("value".to_string())
            )])))
        );
    }

    #[test]
    fn an_empty_item_canonicalizes_as_absent() {
        assert_eq!(canonical_item(&Item::None), CanonicalValue::Absent);
    }

    #[test]
    fn an_absent_workspace_dependency_has_no_fields() {
        assert!(workspace_dependency_fields(&doc(""), "bar").is_empty());
        assert!(workspace_dependency_fields(&doc("[workspace]\nmembers = []\n"), "bar").is_empty());
        assert!(
            workspace_dependency_fields(&doc("[workspace.dependencies]\nother = \"1\"\n"), "bar")
                .is_empty()
        );
    }

    /// An array of tables keeps its table structure.
    ///
    /// An array of tables is the one shape whose members are themselves tables, so it needs its own
    /// arm to stay distinguishable from a plain array.
    #[test]
    fn an_array_of_tables_keeps_its_table_structure() {
        let document =
            doc("[[workspace.package.entry]]\na = 1\n\n[[workspace.package.entry]]\na = 2\n");

        let value = workspace_package_value(&document, "entry").unwrap();

        assert_eq!(
            value,
            CanonicalValue::ArrayOfTables(vec![
                CanonicalValue::Table(BTreeMap::from([(
                    "a".to_string(),
                    CanonicalValue::Integer(1)
                )])),
                CanonicalValue::Table(BTreeMap::from([(
                    "a".to_string(),
                    CanonicalValue::Integer(2)
                )])),
            ])
        );
    }
}
