use crate::model::{CommitSha, Instance, IssueKind};

pub(crate) fn issue(instance: &Instance, kind: IssueKind) -> String {
    format!(
        "<!-- cargo-bench-history:{}:issue:{} -->",
        instance.as_str(),
        kind.as_str()
    )
}

pub(crate) fn pr_comment(instance: &Instance) -> String {
    format!(
        "<!-- cargo-bench-history:{}:pr-comment -->",
        instance.as_str()
    )
}

pub(crate) fn analyzed_sha(instance: &Instance, sha: &CommitSha) -> String {
    format!(
        "<!-- cargo-bench-history:{}:analyzed-sha:{} -->",
        instance.as_str(),
        sha.as_str()
    )
}

pub(crate) fn in_progress(instance: &Instance) -> String {
    format!(
        "<!-- cargo-bench-history:{}:in-progress -->",
        instance.as_str()
    )
}

pub(crate) fn stale_start(instance: &Instance) -> String {
    format!(
        "<!-- cargo-bench-history:{}:stale:start -->",
        instance.as_str()
    )
}

pub(crate) fn stale_end(instance: &Instance) -> String {
    format!(
        "<!-- cargo-bench-history:{}:stale:end -->",
        instance.as_str()
    )
}

pub(crate) fn find_analyzed_sha(body: &str, instance: &Instance) -> Option<CommitSha> {
    let prefix = format!(
        "<!-- cargo-bench-history:{}:analyzed-sha:",
        instance.as_str()
    );
    body.lines().find_map(|line| {
        let value = line.strip_prefix(&prefix)?.strip_suffix(" -->")?;
        value.parse().ok()
    })
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn issue_kind_is_part_of_identity() {
        let instance: Instance = "default".parse().unwrap();
        assert_ne!(
            issue(&instance, IssueKind::Regression),
            issue(&instance, IssueKind::FailureAlert)
        );
    }

    #[test]
    fn analyzed_sha_round_trips_through_body() {
        let instance: Instance = "default".parse().unwrap();
        let sha: CommitSha = "0123456789abcdef0123456789abcdef01234567".parse().unwrap();
        let body = format!("{}\nbody", analyzed_sha(&instance, &sha));
        assert_eq!(find_analyzed_sha(&body, &instance), Some(sha));
    }

    #[test]
    fn every_pr_marker_is_namespaced_by_instance() {
        let instance: Instance = "nightly".parse().unwrap();
        assert_eq!(
            pr_comment(&instance),
            "<!-- cargo-bench-history:nightly:pr-comment -->"
        );
        assert_eq!(
            in_progress(&instance),
            "<!-- cargo-bench-history:nightly:in-progress -->"
        );
        assert_eq!(
            stale_start(&instance),
            "<!-- cargo-bench-history:nightly:stale:start -->"
        );
        assert_eq!(
            stale_end(&instance),
            "<!-- cargo-bench-history:nightly:stale:end -->"
        );
    }
}
