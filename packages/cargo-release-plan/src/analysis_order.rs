// Dependency-first batches keep semantic judgement outside mechanical plan resolution.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use ohno::AppError;
use serde::Serialize;

use crate::report::{ReportFile, read_report};
use crate::verbose::Verbose;

/// One strongly connected component, ready after all its external dependencies.
#[derive(Debug, Eq, PartialEq, Serialize)]
struct AnalysisBatch {
    order: usize,
    packages: Vec<String>,
    cyclic: bool,
}

pub(crate) fn run_analysis_order(path: &Path, verbose: Verbose) -> Result<String, AppError> {
    let report = read_report(path)?;
    Ok(serde_json::to_string(&analysis_order(&report, verbose))
        .expect("analysis batches contain only JSON-compatible data"))
}

fn analysis_order(report: &ReportFile, verbose: Verbose) -> Vec<AnalysisBatch> {
    let names: BTreeSet<_> = report
        .packages
        .iter()
        .map(|package| package.name.clone())
        .collect();
    let edges: BTreeMap<String, BTreeSet<String>> = report
        .packages
        .iter()
        .map(|package| {
            (
                package.name.clone(),
                package
                    .dependencies
                    .iter()
                    .filter(|dependency| names.contains(&dependency.name))
                    .map(|dependency| dependency.name.clone())
                    .collect(),
            )
        })
        .collect();

    // Reachability partitions the graph by mutual dependence. Version-group edges are not
    // dependency edges: grouping an implementation with its shell must not create a cycle.
    let reachable: BTreeMap<_, _> = names
        .iter()
        .map(|name| (name.clone(), reach(name, &edges)))
        .collect();
    let mut remaining = names;
    let mut components = Vec::new();
    while let Some(first) = remaining.pop_first() {
        let mut members = vec![first.clone()];
        for name in reachable
            .get(&first)
            .expect("every report package has a reachability set")
        {
            if remaining.contains(name)
                && reachable
                    .get(name)
                    .expect("reachability contains only report package names")
                    .contains(&first)
            {
                _ = remaining.remove(name);
                members.push(name.clone());
            }
        }
        members.sort();
        components.push(members);
    }

    let mut emitted = BTreeSet::new();
    let mut batches = Vec::new();
    while !components.is_empty() {
        let (ready, blocked): (Vec<_>, Vec<_>) = components.into_iter().partition(|members| {
            members.iter().all(|name| {
                edges
                    .get(name)
                    .expect("component members are report package names")
                    .iter()
                    .all(|dependency| emitted.contains(dependency) || members.contains(dependency))
            })
        });
        assert!(
            !ready.is_empty(),
            "a nonempty condensation graph has a dependency-ready component"
        );
        components = blocked;
        for packages in ready {
            let cyclic = packages.len() > 1;
            verbose.note(|| format!(
                "analysis batch {packages:?} is ready because every dependency outside it has an \
                 earlier batch; cyclic={cyclic} reflects dependency edges, not version groups"
            ));
            emitted.extend(packages.iter().cloned());
            batches.push(AnalysisBatch {
                order: batches
                    .len()
                    .checked_add(1)
                    .expect("batch count fits within addressable memory"),
                packages,
                cyclic,
            });
        }
    }
    batches
}

fn reach(start: &str, edges: &BTreeMap<String, BTreeSet<String>>) -> BTreeSet<String> {
    let mut visited = BTreeSet::new();
    let mut pending = vec![start.to_owned()];
    while let Some(name) = pending.pop() {
        if visited.insert(name.clone()) {
            pending.extend(
                edges
                    .get(&name)
                    .expect("pending edges refer only to report package names")
                    .iter()
                    .filter(|dependency| !visited.contains(*dependency))
                    .cloned(),
            );
        }
    }
    visited
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::report::fixture::{package, report};

    fn graph(edges: &[(&str, &[&str])]) -> ReportFile {
        report(
            edges
                .iter()
                .map(|(name, dependencies)| {
                    let mut package = package(name, "unchanged", true);
                    *package.get_mut("dependencies").unwrap() =
                        json!(dependencies.iter().map(|name| json!({
                "name": name, "req": "1.0.0", "exact_pin": false, "public": false
            })).collect::<Vec<_>>());
                    package
                })
                .collect(),
        )
    }

    fn batches(report: &ReportFile) -> Vec<(Vec<String>, bool)> {
        analysis_order(report, Verbose::new(false))
            .into_iter()
            .enumerate()
            .map(|(index, batch)| {
                assert_eq!(batch.order, index.checked_add(1).unwrap());
                (batch.packages, batch.cyclic)
            })
            .collect()
    }

    #[test]
    fn orders_dependencies_before_dependents_including_unchanged_packages() {
        let report = graph(&[
            ("top", &["middle", "leaf"]),
            ("middle", &["leaf"]),
            ("leaf", &[]),
        ]);
        assert_eq!(
            batches(&report),
            vec![
                (vec!["leaf".to_owned()], false),
                (vec!["middle".to_owned()], false),
                (vec!["top".to_owned()], false),
            ]
        );
    }

    #[test]
    fn cycles_are_components_not_every_node_blocked_by_them() {
        let report = graph(&[
            ("dependent", &["left"]),
            ("left", &["right", "root"]),
            ("right", &["left"]),
            ("root", &[]),
            ("self", &["self"]),
        ]);
        assert_eq!(
            batches(&report),
            vec![
                (vec!["root".to_owned()], false),
                (vec!["self".to_owned()], false),
                (vec!["left".to_owned(), "right".to_owned()], true),
                (vec!["dependent".to_owned()], false),
            ]
        );
    }

    #[test]
    fn exact_groups_do_not_turn_one_way_dependencies_into_cycles() {
        let mut report = graph(&[
            ("api", &["implementation"]),
            ("implementation", &["helper"]),
            ("helper", &[]),
        ]);
        report.packages.retain(|package| package.name != "helper");
        for package in &mut report.packages {
            package.group = Some("api".to_owned());
        }
        report.non_publishable_packages = serde_json::from_value(json!([
            {"name": "helper", "declared_version": "1.0.0", "group": "api"}
        ]))
        .unwrap();
        report.groups = serde_json::from_value(json!({
            "api": {"members": ["api", "helper", "implementation"], "consistent": true,
                "version": "1.0.0"}
        }))
        .unwrap();
        report.validate().unwrap();
        assert_eq!(
            batches(&report),
            vec![
                (vec!["implementation".to_owned()], false),
                (vec!["api".to_owned()], false),
            ]
        );
    }

    #[test]
    fn independent_names_are_ordinal_and_input_order_independent() {
        let mut report = graph(&[("z", &[]), ("a", &[]), ("A", &[])]);
        let expected = batches(&report);
        report.packages.reverse();
        assert_eq!(batches(&report), expected);
        assert_eq!(
            expected,
            vec![
                (vec!["A".to_owned()], false),
                (vec!["a".to_owned()], false),
                (vec!["z".to_owned()], false),
            ]
        );
        assert_eq!(
            serde_json::to_string(&analysis_order(&graph(&[]), Verbose::new(false))).unwrap(),
            "[]"
        );
    }

    #[test]
    fn completes_each_ready_wave_before_reconsidering_dependents() {
        let report = graph(&[("a", &["b"]), ("b", &[]), ("z", &[])]);
        assert_eq!(
            batches(&report),
            vec![
                (vec!["b".to_owned()], false),
                (vec!["z".to_owned()], false),
                (vec!["a".to_owned()], false),
            ]
        );
        let report = graph(&[("nm", &["nm_impl"]), ("nm_impl", &[])]);
        assert_eq!(
            batches(&report),
            vec![
                (vec!["nm_impl".to_owned()], false),
                (vec!["nm".to_owned()], false),
            ]
        );
    }
}
