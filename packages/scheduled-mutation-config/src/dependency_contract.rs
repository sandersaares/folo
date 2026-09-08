use std::collections::{BTreeMap, BTreeSet};

use ohno::AppError;
use serde::{Deserialize, Serialize};

/// The reviewed dependency identity needed to compare decoder implementations.
///
/// Only the helper's effective requirements and reachable dependency graph are retained. Workspace
/// package versions and checkout paths cannot invalidate an otherwise identical checker.
/// Ref: .github/workflows/implementation.md, "Scheduled controller ownership".
#[derive(Debug, Serialize)]
struct Contract<'a> {
    requirements: &'a BTreeSet<Requirement>,
    packages: BTreeMap<String, ResolvedPackage<'a>>,
}

/// A dependency's selected code and features, independent of Cargo's path-bearing package IDs.
#[derive(Debug, Serialize)]
struct ResolvedPackage<'a> {
    features: &'a BTreeSet<String>,
    dependencies: BTreeSet<String>,
}

/// The Cargo metadata fields needed to derive the controller helper's dependency identity.
#[derive(Debug, Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    workspace_members: BTreeSet<String>,
    resolve: Resolution,
}

/// Connects a Cargo package ID to its source identity and effective manifest requirements.
#[derive(Debug, Deserialize)]
struct Package {
    id: String,
    name: String,
    version: String,
    source: Option<String>,
    dependencies: BTreeSet<Requirement>,
}

impl Package {
    fn identity(&self) -> Result<String, AppError> {
        let source = self
            .source
            .as_ref()
            .filter(|source| source.starts_with("registry+"))
            .ok_or_else(DependencyContractError::new)?;
        // Registry coordinates identify immutable crate content; Cargo verifies its checksum
        // against the trusted lockfile while building. Local or git dependencies need a separate
        // reviewed content-identity policy rather than silently omitting their source bytes.
        Ok(format!("{} {} ({source})", self.name, self.version))
    }
}

/// Captures inherited requirements and features that may not appear in the helper's manifest.
#[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct Requirement {
    name: String,
    source: Option<String>,
    req: String,
    kind: Option<String>,
    rename: Option<String>,
    optional: bool,
    uses_default_features: bool,
    features: BTreeSet<String>,
    target: Option<String>,
}

/// Cargo's resolved dependency edges and feature selections.
#[derive(Debug, Deserialize)]
struct Resolution {
    nodes: Vec<Node>,
}

/// The reachable edges of a single package in Cargo's resolved graph.
#[derive(Debug, Deserialize)]
struct Node {
    id: String,
    dependencies: BTreeSet<String>,
    features: BTreeSet<String>,
}

pub(crate) fn dependency_contract(text: &str) -> Result<String, AppError> {
    let metadata: Metadata =
        serde_json::from_str(text).map_err(DependencyContractError::caused_by)?;
    let packages: BTreeMap<_, _> = metadata
        .packages
        .iter()
        .map(|package| (package.id.as_str(), package))
        .collect();
    let nodes: BTreeMap<_, _> = metadata
        .resolve
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();
    let helpers: Vec<_> = metadata
        .packages
        .iter()
        .filter(|package| {
            package.name == env!("CARGO_PKG_NAME")
                && metadata.workspace_members.contains(&package.id)
        })
        .collect();
    let [helper] = helpers.as_slice() else {
        return Err(DependencyContractError::new().into());
    };
    let helper_node = nodes
        .get(helper.id.as_str())
        .ok_or_else(DependencyContractError::new)?;
    let mut contract = Contract {
        requirements: &helper.dependencies,
        packages: BTreeMap::new(),
    };
    let mut pending: Vec<_> = helper_node
        .dependencies
        .iter()
        .map(String::as_str)
        .collect();
    let mut visited = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        let package = packages.get(id).ok_or_else(DependencyContractError::new)?;
        let node = nodes.get(id).ok_or_else(DependencyContractError::new)?;
        let mut dependencies = BTreeSet::new();
        for dependency in &node.dependencies {
            let dependency_package = packages
                .get(dependency.as_str())
                .ok_or_else(DependencyContractError::new)?;
            dependencies.insert(dependency_package.identity()?);
            pending.push(dependency.as_str());
        }
        contract.packages.insert(
            package.identity()?,
            ResolvedPackage {
                features: &node.features,
                dependencies,
            },
        );
    }
    serde_json::to_string_pretty(&contract)
        .map_err(DependencyContractError::caused_by)
        .map_err(Into::into)
}

/// Rejects metadata that cannot establish the reviewed decoder dependency identity.
#[ohno::error]
#[display("cannot identify the controller mutation decoder dependencies")]
struct DependencyContractError;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn metadata() -> Value {
        json!({
            "packages": [
                {"id": "helper", "name": env!("CARGO_PKG_NAME"), "version": "0.0.0",
                 "source": null, "dependencies": []},
                {"id": "parser", "name": "parser", "version": "1.0.0",
                 "source": "registry+trusted", "dependencies": []},
                {"id": "lexer", "name": "lexer", "version": "1.0.0",
                 "source": "registry+trusted", "dependencies": []},
                {"id": "unrelated", "name": "unrelated", "version": "1.0.0",
                 "source": null, "dependencies": []}
            ],
            "workspace_members": ["helper", "unrelated"],
            "resolve": {"nodes": [
                {"id": "helper", "dependencies": ["parser"], "features": []},
                {"id": "parser", "dependencies": ["lexer"], "features": ["parse"]},
                {"id": "lexer", "dependencies": [], "features": []},
                {"id": "unrelated", "dependencies": [], "features": []}
            ]}
        })
    }

    fn contract(metadata: &Value) -> Value {
        serde_json::from_str(&dependency_contract(&metadata.to_string()).unwrap()).unwrap()
    }

    #[test]
    fn records_the_reachable_registry_graph() {
        assert_eq!(
            contract(&metadata()),
            json!({
                "requirements": [],
                "packages": {
                    "parser 1.0.0 (registry+trusted)": {
                        "features": ["parse"],
                        "dependencies": ["lexer 1.0.0 (registry+trusted)"]
                    },
                    "lexer 1.0.0 (registry+trusted)": {"features": [], "dependencies": []}
                }
            })
        );
    }

    #[test]
    fn ignores_unrelated_versions_and_cargo_package_paths() {
        let mut changed = metadata();
        *changed.pointer_mut("/packages/3/version").unwrap() = json!("9.0.0");
        *changed.pointer_mut("/packages/0/id").unwrap() = json!("different-checkout");
        *changed.pointer_mut("/workspace_members/0").unwrap() = json!("different-checkout");
        *changed.pointer_mut("/resolve/nodes/0/id").unwrap() = json!("different-checkout");
        assert_eq!(contract(&changed), contract(&metadata()));
    }

    #[test]
    fn detects_transitive_source_version_and_feature_changes() {
        for (path, replacement) in [
            ("/packages/2/version", json!("2.0.0")),
            ("/packages/2/source", json!("registry+different")),
            ("/resolve/nodes/2/features", json!(["changed"])),
            ("/resolve/nodes/1/dependencies", json!([])),
        ] {
            let mut changed = metadata();
            *changed.pointer_mut(path).unwrap() = replacement;
            assert_ne!(contract(&changed), contract(&metadata()));
        }
    }

    #[test]
    fn retains_inherited_requirements_and_feature_flags() {
        let mut changed = metadata();
        let requirements = json!([{
            "name": "parser", "source": "registry+trusted", "req": "^1.0.0",
            "kind": null, "rename": null, "optional": false, "uses_default_features": false,
            "features": ["parse"], "target": null
        }]);
        *changed.pointer_mut("/packages/0/dependencies").unwrap() = requirements.clone();
        assert_eq!(
            contract(&changed).get("requirements").unwrap(),
            &requirements
        );
    }

    #[test]
    fn rejects_missing_graph_edges_and_unbound_sources() {
        for (path, replacement) in [
            ("/resolve/nodes/1/dependencies", json!(["missing"])),
            ("/packages/2/source", Value::Null),
            ("/packages/2/source", json!("git+unreviewed")),
            ("/workspace_members", json!([])),
        ] {
            let mut changed = metadata();
            *changed.pointer_mut(path).unwrap() = replacement;
            let error = dependency_contract(&changed.to_string()).unwrap_err();
            _ = error.find_source::<DependencyContractError>().unwrap();
        }
    }
}
