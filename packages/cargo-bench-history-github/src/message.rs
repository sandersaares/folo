use crate::marker;
use crate::model::{CommitSha, Instance, IssueKind};

const REGRESSION_HEADING: &str = "# Benchmark history";
const PR_HEADING: &str = "## Benchmark history";
const WARNING_HEADING: &str = "> [!WARNING]";

/// Optional repository-specific prose around a standard report.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Envelope<'a> {
    pub(crate) intro: Option<&'a str>,
    pub(crate) docs_url: Option<&'a str>,
    pub(crate) artifact_url: Option<&'a str>,
}

pub(crate) fn regression_issue(
    instance: &Instance,
    analyzed_sha: &CommitSha,
    summary: &str,
    envelope: Envelope<'_>,
) -> String {
    let mut sections = vec![
        marker::issue(instance, IssueKind::Regression),
        marker::analyzed_sha(instance, analyzed_sha),
        REGRESSION_HEADING.to_owned(),
    ];
    push_optional(&mut sections, envelope.intro);
    sections.push(format!("Analyzed commit: {}", analyzed_sha.as_str()));
    sections.push(summary.trim().to_owned());
    push_links(&mut sections, envelope);
    join_sections(sections)
}

pub(crate) fn all_clear_issue(
    instance: &Instance,
    clean_sha: &CommitSha,
    envelope: Envelope<'_>,
) -> String {
    let mut sections = vec![
        marker::issue(instance, IssueKind::Regression),
        marker::analyzed_sha(instance, clean_sha),
        REGRESSION_HEADING.to_owned(),
        format!(
            "No notable benchmark changes were detected at {}.",
            clean_sha.as_str()
        ),
    ];
    push_optional(&mut sections, envelope.intro);
    push_links(&mut sections, envelope);
    join_sections(sections)
}

pub(crate) fn failure_issue(instance: &Instance, run_url: &str, envelope: Envelope<'_>) -> String {
    let mut sections = vec![
        marker::issue(instance, IssueKind::FailureAlert),
        "# Benchmark-history automation failed".to_owned(),
        "The benchmark history may be incomplete until a later run succeeds.".to_owned(),
        format!("Failed run: {run_url}"),
    ];
    push_optional(&mut sections, envelope.intro);
    push_links(&mut sections, envelope);
    join_sections(sections)
}

pub(crate) fn resolved_failure_issue(existing: &str, run_url: &str) -> String {
    format!("{existing}\n\nResolved by successful run: {run_url}")
}

pub(crate) fn pr_result(
    instance: &Instance,
    analyzed_sha: &CommitSha,
    packages: &str,
    summary: &str,
    envelope: Envelope<'_>,
) -> String {
    let mut sections = vec![
        marker::pr_comment(instance),
        marker::analyzed_sha(instance, analyzed_sha),
        PR_HEADING.to_owned(),
    ];
    push_optional(&mut sections, envelope.intro);
    sections.push(format_scope(packages));
    sections.push(format!("Analyzed commit: {}", analyzed_sha.as_str()));
    sections.push(summary.trim().to_owned());
    push_links(&mut sections, envelope);
    join_sections(sections)
}

pub(crate) fn pr_in_progress(instance: &Instance, packages: &str) -> String {
    join_sections(vec![
        marker::pr_comment(instance),
        marker::in_progress(instance),
        PR_HEADING.to_owned(),
        "Benchmarking is in progress.".to_owned(),
        format_scope(packages),
    ])
}

pub(crate) fn pr_nothing_in_scope(instance: &Instance) -> String {
    join_sections(vec![
        marker::pr_comment(instance),
        PR_HEADING.to_owned(),
        "No benchmarkable package is affected by this pull request.".to_owned(),
    ])
}

pub(crate) fn pr_failed(instance: &Instance, run_url: &str) -> String {
    join_sections(vec![
        marker::pr_comment(instance),
        PR_HEADING.to_owned(),
        "Benchmarking did not complete successfully.".to_owned(),
        format!("Failed run: {run_url}"),
    ])
}

pub(crate) fn stale_warning(distance: Option<u64>, subject: &str) -> String {
    match distance {
        Some(commits) => {
            let noun = if commits == 1 { "commit" } else { "commits" };
            format!("{subject} {commits} {noun} behind HEAD.")
        }
        None => format!("{subject} out of date; the commit distance is unavailable."),
    }
}

pub(crate) fn freshness_unverified(subject: &str) -> String {
    format!("{subject} freshness could not be verified.")
}

pub(crate) fn insert_stale_banner(body: &str, instance: &Instance, warning: &str) -> String {
    let start = marker::stale_start(instance);
    let end = marker::stale_end(instance);
    let mut without_old = Vec::new();
    let mut lines = body.lines();

    while let Some(line) = lines.next() {
        if line != start {
            without_old.push(line);
            continue;
        }

        let mut stale_block = vec![line];
        let mut complete = false;
        for stale_line in lines.by_ref() {
            stale_block.push(stale_line);
            if stale_line == end {
                complete = true;
                break;
            }
        }
        if !complete {
            without_old.extend(stale_block);
        }
    }

    let banner = [start.as_str(), WARNING_HEADING, warning, end.as_str()];
    let insertion = without_old
        .iter()
        .position(|line| line.starts_with("<!-- cargo-bench-history:"))
        .and_then(|position| position.checked_add(1))
        .unwrap_or_default();
    without_old.splice(insertion..insertion, banner);
    without_old.join("\n")
}

pub(crate) fn is_in_progress(body: &str, instance: &Instance) -> bool {
    body.lines()
        .any(|line| line == marker::in_progress(instance))
}

fn format_scope(packages: &str) -> String {
    let packages = packages
        .split(',')
        .map(str::trim)
        .filter(|one| !one.is_empty())
        .map(|one| format!("`{one}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("Packages benchmarked: {packages}")
}

fn push_optional(sections: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        sections.push(value.trim().to_owned());
    }
}

fn push_links(sections: &mut Vec<String>, envelope: Envelope<'_>) {
    if let Some(url) = envelope.artifact_url {
        sections.push(format!("[Download the full report bundle]({url})"));
    }
    if let Some(url) = envelope.docs_url {
        sections.push(format!("[How to read this report]({url})"));
    }
}

fn join_sections(sections: Vec<String>) -> String {
    sections
        .into_iter()
        .filter(|one| !one.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    fn instance() -> Instance {
        "default".parse().unwrap()
    }

    fn sha() -> CommitSha {
        "0123456789abcdef0123456789abcdef01234567".parse().unwrap()
    }

    #[test]
    fn regression_issue_embeds_domain_summary_without_interpreting_it() {
        let body = regression_issue(
            &instance(),
            &sha(),
            "DOMAIN SUMMARY",
            Envelope {
                intro: Some("Advisory."),
                docs_url: Some("https://example.test/docs"),
                artifact_url: Some("https://example.test/artifact"),
            },
        );

        assert!(body.starts_with("<!-- cargo-bench-history:default:issue:regression -->"));
        assert!(body.contains("DOMAIN SUMMARY"));
        assert!(body.contains("Advisory."));
        assert!(body.contains("[Download the full report bundle]"));
        assert!(body.contains("[How to read this report]"));
    }

    #[test]
    fn stale_banner_replaces_a_previous_banner() {
        let instance = instance();
        let body = pr_result(
            &instance,
            &sha(),
            "foo, bar",
            "summary",
            Envelope::default(),
        );
        let stale = insert_stale_banner(&body, &instance, "old warning");
        let refreshed = insert_stale_banner(&stale, &instance, "new warning");

        assert!(!refreshed.contains("old warning"));
        assert!(refreshed.starts_with(&marker::pr_comment(&instance)));
        assert_eq!(refreshed.matches("new warning").count(), 1);
        assert_eq!(refreshed.matches(WARNING_HEADING).count(), 1);
    }

    #[test]
    fn stale_banner_is_inserted_after_the_identity_marker() {
        let instance = instance();
        let body = format!("{}\nbody", marker::pr_comment(&instance));
        let refreshed = insert_stale_banner(&body, &instance, "warning");
        let expected = format!(
            "{}\n{}\n{}\nwarning\n{}\nbody",
            marker::pr_comment(&instance),
            marker::stale_start(&instance),
            WARNING_HEADING,
            marker::stale_end(&instance)
        );
        assert_eq!(refreshed, expected);
    }

    #[test]
    fn unterminated_banner_is_preserved_before_a_new_banner() {
        let instance = instance();
        let body = format!(
            "{}\n{}\nold",
            marker::pr_comment(&instance),
            marker::stale_start(&instance)
        );
        let refreshed = insert_stale_banner(&body, &instance, "new warning");

        assert!(refreshed.contains("old"));
        assert!(refreshed.contains("new warning"));
    }

    #[test]
    fn scope_is_trimmed_and_rendered_consistently() {
        let body = pr_in_progress(&instance(), "foo, bar");
        assert!(body.contains("Packages benchmarked: `foo`, `bar`"));
        assert!(is_in_progress(&body, &instance()));
        assert!(!is_in_progress(
            &pr_nothing_in_scope(&instance()),
            &instance()
        ));
    }

    #[test]
    fn failure_resolution_preserves_the_existing_report_and_names_the_run() {
        let body = resolved_failure_issue("failure body", "https://example.test/run");
        assert_eq!(
            body,
            "failure body\n\nResolved by successful run: https://example.test/run"
        );
    }
}
