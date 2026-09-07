use std::{env, fs};

use ohno::{AppError, EnrichableExt as _};

use crate::cli::{Cli, Command};
use crate::errors::{MissingRepositoryError, read_body_error};
use crate::github::{Comment, Comparison, GitHub, Issue, RestGitHub};
use crate::marker;
use crate::message::{self, Envelope};
use crate::model::{CommitSha, Instance, IssueKind, Repository};

/// Inputs shared by every lifecycle operation.
#[derive(Clone, Debug)]
pub(crate) struct Context {
    pub(crate) repository: Repository,
    pub(crate) instance: Instance,
    pub(crate) verbose: bool,
}

/// Executes one GitHub lifecycle command.
///
/// # Errors
///
/// Returns an error when inputs cannot be loaded or a required GitHub operation
/// does not complete successfully.
// Process wiring constructs the live adapter and reads process environment/filesystem.
// The generic lifecycle functions it dispatches to carry the behavioral tests.
#[cfg_attr(test, mutants::skip)]
pub async fn run(cli: Cli) -> Result<(), AppError> {
    let repository = match cli.repository() {
        Some(repository) => repository,
        None => env::var("GITHUB_REPOSITORY")
            .map_err(MissingRepositoryError::caused_by)?
            .parse()?,
    };
    let context = Context {
        repository,
        instance: cli.instance(),
        verbose: cli.verbose(),
    };
    let command = cli.into_command();
    let github = RestGitHub::from_env()?;

    match command {
        Command::IssuePreflight { head } => issue_preflight(&github, &context, &head).await,
        Command::PublishIssue {
            title,
            body_file,
            analyzed_sha,
            artifact_url,
            intro,
            docs_url,
        } => {
            let body = fs::read_to_string(&body_file)
                .map_err(|error| read_body_error(body_file, error))?;
            publish_issue(
                &github,
                &context,
                &title,
                &body,
                &analyzed_sha,
                Envelope {
                    intro: intro.as_deref(),
                    docs_url: docs_url.as_deref(),
                    artifact_url: artifact_url.as_deref(),
                },
            )
            .await
        }
        Command::IssueCleanup {
            clean_commit,
            auto_close,
            intro,
            docs_url,
        } => {
            issue_cleanup(
                &github,
                &context,
                &clean_commit,
                auto_close,
                Envelope {
                    intro: intro.as_deref(),
                    docs_url: docs_url.as_deref(),
                    artifact_url: None,
                },
            )
            .await
        }
        Command::Alert {
            title,
            run_url,
            intro,
            docs_url,
        } => {
            alert(
                &github,
                &context,
                &title,
                &run_url,
                Envelope {
                    intro: intro.as_deref(),
                    docs_url: docs_url.as_deref(),
                    artifact_url: None,
                },
            )
            .await
        }
        Command::ResolveAlert { run_url } => resolve_alert(&github, &context, &run_url).await,
        Command::PrCommentPreflight {
            pull_request,
            packages,
        } => pr_comment_preflight(&github, &context, pull_request.get(), &packages).await,
        Command::PublishPrComment {
            pull_request,
            analyzed_sha,
            body_file,
            packages,
            artifact_url,
            intro,
            docs_url,
        } => {
            let body = fs::read_to_string(&body_file)
                .map_err(|error| read_body_error(body_file, error))?;
            publish_pr_comment(
                &github,
                &context,
                pull_request.get(),
                &analyzed_sha,
                &packages,
                &body,
                Envelope {
                    intro: intro.as_deref(),
                    docs_url: docs_url.as_deref(),
                    artifact_url: artifact_url.as_deref(),
                },
            )
            .await
        }
        Command::PrCommentCleanup {
            pull_request,
            delete,
        } => pr_comment_cleanup(&github, &context, pull_request.get(), delete).await,
        Command::PrCommentFinalize {
            pull_request,
            run_url,
        } => pr_comment_finalize(&github, &context, pull_request.get(), &run_url).await,
    }
}

pub(crate) async fn issue_preflight(
    github: &impl GitHub,
    context: &Context,
    head: &CommitSha,
) -> Result<(), AppError> {
    let identity = marker::issue(&context.instance, IssueKind::Regression);
    let Some(issue) = find_issue(github, &context.repository, &identity).await? else {
        note(
            context,
            "no rolling regression issue exists, so preflight is a no-op",
        );
        return Ok(());
    };
    let Some(analyzed) = marker::find_analyzed_sha(&issue.body, &context.instance) else {
        let body = message::insert_stale_banner(
            &issue.body,
            &context.instance,
            &message::stale_warning(None, "Findings are"),
        );
        return github
            .update_issue(&context.repository, issue.number, None, &body)
            .await;
    };
    if analyzed == *head {
        note(
            context,
            "the rolling issue already describes the current commit",
        );
        return Ok(());
    }
    let comparison = github
        .compare(&context.repository, &analyzed, head)
        .await
        .unwrap_or(Comparison { ahead_by: None });
    let warning = message::stale_warning(comparison.ahead_by, "Findings are");
    let body = message::insert_stale_banner(&issue.body, &context.instance, &warning);
    github
        .update_issue(&context.repository, issue.number, None, &body)
        .await
}

pub(crate) async fn publish_issue(
    github: &impl GitHub,
    context: &Context,
    title: &str,
    summary: &str,
    analyzed_sha: &CommitSha,
    envelope: Envelope<'_>,
) -> Result<(), AppError> {
    let identity = marker::issue(&context.instance, IssueKind::Regression);
    let body = message::regression_issue(&context.instance, analyzed_sha, summary, envelope);
    upsert_issue(github, context, title, &identity, &body).await
}

pub(crate) async fn issue_cleanup(
    github: &impl GitHub,
    context: &Context,
    clean_commit: &CommitSha,
    auto_close: bool,
    envelope: Envelope<'_>,
) -> Result<(), AppError> {
    let identity = marker::issue(&context.instance, IssueKind::Regression);
    let Some(issue) = find_issue(github, &context.repository, &identity).await? else {
        note(
            context,
            "no rolling regression issue exists, so cleanup is a no-op",
        );
        return Ok(());
    };
    let body = message::all_clear_issue(&context.instance, clean_commit, envelope);
    github
        .update_issue(&context.repository, issue.number, None, &body)
        .await?;
    if auto_close {
        github
            .close_issue(&context.repository, issue.number)
            .await?;
    }
    Ok(())
}

pub(crate) async fn alert(
    github: &impl GitHub,
    context: &Context,
    title: &str,
    run_url: &str,
    envelope: Envelope<'_>,
) -> Result<(), AppError> {
    let identity = marker::issue(&context.instance, IssueKind::FailureAlert);
    let body = message::failure_issue(&context.instance, run_url, envelope);
    upsert_issue(github, context, title, &identity, &body).await
}

pub(crate) async fn resolve_alert(
    github: &impl GitHub,
    context: &Context,
    run_url: &str,
) -> Result<(), AppError> {
    let identity = marker::issue(&context.instance, IssueKind::FailureAlert);
    let Some(issue) = find_issue(github, &context.repository, &identity).await? else {
        note(context, "no failure alert exists, so resolution is a no-op");
        return Ok(());
    };
    let body = message::resolved_failure_issue(&issue.body, run_url);
    github
        .update_issue(&context.repository, issue.number, None, &body)
        .await?;
    github.close_issue(&context.repository, issue.number).await
}

pub(crate) async fn pr_comment_preflight(
    github: &impl GitHub,
    context: &Context,
    pull_request: u64,
    packages: &str,
) -> Result<(), AppError> {
    let identity = marker::pr_comment(&context.instance);
    let existing = find_comment(github, &context.repository, pull_request, &identity).await?;
    match existing {
        None => {
            let body = message::pr_in_progress(&context.instance, packages);
            create_comment_reconciled(github, context, pull_request, &identity, &body).await
        }
        Some(comment) if message::is_in_progress(&comment.body, &context.instance) => {
            let body = message::pr_in_progress(&context.instance, packages);
            github
                .update_comment(&context.repository, comment.id, &body)
                .await
        }
        Some(comment) => {
            let Ok(live) = github
                .pull_request_head(&context.repository, pull_request)
                .await
            else {
                let warning = message::freshness_unverified("Benchmark result");
                let body = message::insert_stale_banner(&comment.body, &context.instance, &warning);
                return github
                    .update_comment(&context.repository, comment.id, &body)
                    .await;
            };
            let comparison = match marker::find_analyzed_sha(&comment.body, &context.instance) {
                Some(analyzed) if analyzed != live => github
                    .compare(&context.repository, &analyzed, &live)
                    .await
                    .unwrap_or(Comparison { ahead_by: None }),
                Some(_) => return Ok(()),
                None => Comparison { ahead_by: None },
            };
            let warning = message::stale_warning(comparison.ahead_by, "Benchmark results are");
            let body = message::insert_stale_banner(&comment.body, &context.instance, &warning);
            github
                .update_comment(&context.repository, comment.id, &body)
                .await
        }
    }
}

pub(crate) async fn publish_pr_comment(
    github: &impl GitHub,
    context: &Context,
    pull_request: u64,
    analyzed_sha: &CommitSha,
    packages: &str,
    summary: &str,
    envelope: Envelope<'_>,
) -> Result<(), AppError> {
    let identity = marker::pr_comment(&context.instance);
    let mut body = message::pr_result(&context.instance, analyzed_sha, packages, summary, envelope);
    match github
        .pull_request_head(&context.repository, pull_request)
        .await
    {
        Ok(live) if live != *analyzed_sha => {
            let comparison = github
                .compare(&context.repository, analyzed_sha, &live)
                .await
                .unwrap_or(Comparison { ahead_by: None });
            let warning = message::stale_warning(comparison.ahead_by, "Benchmark results are");
            body = message::insert_stale_banner(&body, &context.instance, &warning);
        }
        Ok(_) => {}
        Err(_) => {
            let warning = message::freshness_unverified("Benchmark result");
            body = message::insert_stale_banner(&body, &context.instance, &warning);
        }
    }

    match find_comment(github, &context.repository, pull_request, &identity).await? {
        Some(comment) => {
            github
                .update_comment(&context.repository, comment.id, &body)
                .await
        }
        None => create_comment_reconciled(github, context, pull_request, &identity, &body).await,
    }
}

pub(crate) async fn pr_comment_cleanup(
    github: &impl GitHub,
    context: &Context,
    pull_request: u64,
    delete: bool,
) -> Result<(), AppError> {
    let identity = marker::pr_comment(&context.instance);
    let Some(comment) = find_comment(github, &context.repository, pull_request, &identity).await?
    else {
        note(
            context,
            "no rolling pull-request comment exists, so cleanup is a no-op",
        );
        return Ok(());
    };
    if delete {
        github.delete_comment(&context.repository, comment.id).await
    } else {
        let body = message::pr_nothing_in_scope(&context.instance);
        github
            .update_comment(&context.repository, comment.id, &body)
            .await
    }
}

pub(crate) async fn pr_comment_finalize(
    github: &impl GitHub,
    context: &Context,
    pull_request: u64,
    run_url: &str,
) -> Result<(), AppError> {
    let identity = marker::pr_comment(&context.instance);
    let Some(comment) = find_comment(github, &context.repository, pull_request, &identity).await?
    else {
        return Ok(());
    };
    if !message::is_in_progress(&comment.body, &context.instance) {
        note(
            context,
            "the rolling comment already carries results, so finalize is a no-op",
        );
        return Ok(());
    }
    let body = message::pr_failed(&context.instance, run_url);
    github
        .update_comment(&context.repository, comment.id, &body)
        .await
}

async fn upsert_issue(
    github: &impl GitHub,
    context: &Context,
    title: &str,
    marker: &str,
    body: &str,
) -> Result<(), AppError> {
    if let Some(issue) = find_issue(github, &context.repository, marker).await? {
        return github
            .update_issue(&context.repository, issue.number, Some(title), body)
            .await;
    }
    match github.create_issue(&context.repository, title, body).await {
        Ok(_) => Ok(()),
        Err(error) => match find_issue(github, &context.repository, marker).await {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(error),
            Err(_lookup_error) => {
                Err(error.enrich("marker reconciliation also failed after the create error"))
            }
        },
    }
}

async fn create_comment_reconciled(
    github: &impl GitHub,
    context: &Context,
    pull_request: u64,
    marker: &str,
    body: &str,
) -> Result<(), AppError> {
    match github
        .create_comment(&context.repository, pull_request, body)
        .await
    {
        Ok(_) => Ok(()),
        Err(error) => match find_comment(github, &context.repository, pull_request, marker).await {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(error),
            Err(_lookup_error) => {
                Err(error.enrich("marker reconciliation also failed after the create error"))
            }
        },
    }
}

async fn find_issue(
    github: &impl GitHub,
    repository: &Repository,
    marker: &str,
) -> Result<Option<Issue>, AppError> {
    Ok(github
        .open_issues(repository)
        .await?
        .into_iter()
        .find(|issue| issue.body.lines().any(|line| line == marker)))
}

async fn find_comment(
    github: &impl GitHub,
    repository: &Repository,
    pull_request: u64,
    marker: &str,
) -> Result<Option<Comment>, AppError> {
    Ok(github
        .comments(repository, pull_request)
        .await?
        .into_iter()
        .find(|comment| comment.body.lines().any(|line| line == marker)))
}

// Verbose diagnostics have no behavioral effect, and capturing process stderr would
// add global-state coupling to otherwise hermetic unit tests.
#[cfg_attr(test, mutants::skip)]
fn note(context: &Context, message: &str) {
    if context.verbose {
        eprintln!(
            "[cargo-bench-history-github] {}: {message}",
            context.repository
        );
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use futures::executor::block_on;

    use super::*;
    use crate::errors::AmbiguousCreateError;
    use crate::github::fake::FakeGitHub;

    fn context() -> Context {
        Context {
            repository: "folo-rs/folo".parse().unwrap(),
            instance: "default".parse().unwrap(),
            verbose: false,
        }
    }

    fn sha(value: char) -> CommitSha {
        value.to_string().repeat(40).parse().unwrap()
    }

    fn only_issue(github: &FakeGitHub) -> Issue {
        let issues = github.issues();
        assert_eq!(issues.len(), 1);
        issues.into_iter().next().unwrap()
    }

    fn only_comment(github: &FakeGitHub, pull_request: u64) -> Comment {
        let comments = github.comments_for(pull_request);
        assert_eq!(comments.len(), 1);
        comments.into_iter().next().unwrap()
    }

    #[test]
    fn ambiguous_issue_create_is_reconciled_by_marker() {
        let github = FakeGitHub::new();
        github.fail_next_issue_create_after_commit();
        block_on(publish_issue(
            &github,
            &context(),
            "Regressions",
            "summary",
            &sha('a'),
            Envelope::default(),
        ))
        .unwrap();
        assert_eq!(github.issues().len(), 1);
    }

    #[test]
    fn create_error_survives_a_failed_issue_reconciliation() {
        let github = FakeGitHub::new();
        github.fail_next_issue_create_after_commit();
        github.fail_issue_list();
        let error = block_on(publish_issue(
            &github,
            &context(),
            "Regressions",
            "summary",
            &sha('a'),
            Envelope::default(),
        ))
        .unwrap_err();
        assert!(error.find_source::<AmbiguousCreateError>().is_some());
    }

    #[test]
    fn publishing_again_updates_the_displayed_issue_title() {
        let github = FakeGitHub::new();
        let context = context();
        block_on(publish_issue(
            &github,
            &context,
            "Old title",
            "summary",
            &sha('a'),
            Envelope::default(),
        ))
        .unwrap();
        block_on(publish_issue(
            &github,
            &context,
            "New title",
            "summary",
            &sha('b'),
            Envelope::default(),
        ))
        .unwrap();

        assert_eq!(only_issue(&github).title, "New title");
    }

    #[test]
    fn issue_preflight_replaces_staleness_banner() {
        let github = FakeGitHub::new();
        let context = context();
        block_on(publish_issue(
            &github,
            &context,
            "Regressions",
            "summary",
            &sha('a'),
            Envelope::default(),
        ))
        .unwrap();
        github.set_comparison(&sha('a'), &sha('b'), Comparison { ahead_by: Some(2) });
        block_on(issue_preflight(&github, &context, &sha('b'))).unwrap();
        block_on(issue_preflight(&github, &context, &sha('b'))).unwrap();

        let body = only_issue(&github).body;
        assert_eq!(body.matches("2 commits behind HEAD").count(), 1);
    }

    #[test]
    fn issue_cleanup_updates_before_optional_close() {
        let github = FakeGitHub::new();
        let context = context();
        block_on(publish_issue(
            &github,
            &context,
            "Regressions",
            "summary",
            &sha('a'),
            Envelope::default(),
        ))
        .unwrap();
        block_on(issue_cleanup(
            &github,
            &context,
            &sha('b'),
            false,
            Envelope::default(),
        ))
        .unwrap();
        assert!(
            only_issue(&github)
                .body
                .contains("No notable benchmark changes")
        );

        block_on(issue_cleanup(
            &github,
            &context,
            &sha('c'),
            true,
            Envelope::default(),
        ))
        .unwrap();
        assert!(github.issues().is_empty());
    }

    #[test]
    fn failure_alert_is_independent_of_regression_issue() {
        let github = FakeGitHub::new();
        let context = context();
        block_on(publish_issue(
            &github,
            &context,
            "Regressions",
            "summary",
            &sha('a'),
            Envelope::default(),
        ))
        .unwrap();
        block_on(alert(
            &github,
            &context,
            "Failure",
            "https://example.test/run",
            Envelope::default(),
        ))
        .unwrap();
        assert_eq!(github.issues().len(), 2);
        block_on(resolve_alert(
            &github,
            &context,
            "https://example.test/success",
        ))
        .unwrap();
        assert_eq!(github.issues().len(), 1);
    }

    #[test]
    fn pull_request_lifecycle_updates_one_comment() {
        let github = FakeGitHub::new();
        let context = context();
        let pull_request = 7;
        github.set_pull_head(pull_request, sha('a'));

        block_on(pr_comment_preflight(
            &github,
            &context,
            pull_request,
            "foo,bar",
        ))
        .unwrap();
        assert!(message::is_in_progress(
            &only_comment(&github, pull_request).body,
            &context.instance
        ));

        block_on(publish_pr_comment(
            &github,
            &context,
            pull_request,
            &sha('a'),
            "foo,bar",
            "summary",
            Envelope::default(),
        ))
        .unwrap();
        assert!(only_comment(&github, pull_request).body.contains("summary"));

        block_on(pr_comment_cleanup(&github, &context, pull_request, false)).unwrap();
        assert!(
            only_comment(&github, pull_request)
                .body
                .contains("No benchmarkable package")
        );
    }

    #[test]
    fn preflight_marks_existing_results_stale_but_not_current_results() {
        let github = FakeGitHub::new();
        let context = context();
        let pull_request = 13;
        github.set_pull_head(pull_request, sha('a'));
        block_on(publish_pr_comment(
            &github,
            &context,
            pull_request,
            &sha('a'),
            "foo",
            "summary",
            Envelope::default(),
        ))
        .unwrap();
        block_on(pr_comment_preflight(&github, &context, pull_request, "foo")).unwrap();
        assert!(
            !only_comment(&github, pull_request)
                .body
                .contains("[!WARNING]")
        );

        github.set_pull_head(pull_request, sha('b'));
        github.set_comparison(&sha('a'), &sha('b'), Comparison { ahead_by: Some(1) });
        block_on(pr_comment_preflight(&github, &context, pull_request, "foo")).unwrap();
        assert!(
            only_comment(&github, pull_request)
                .body
                .contains("1 commit behind HEAD")
        );
    }

    #[test]
    fn preflight_refreshes_an_existing_placeholder_scope() {
        let github = FakeGitHub::new();
        let context = context();
        let pull_request = 15;
        block_on(pr_comment_preflight(
            &github,
            &context,
            pull_request,
            "old-package",
        ))
        .unwrap();
        block_on(pr_comment_preflight(
            &github,
            &context,
            pull_request,
            "new-package",
        ))
        .unwrap();

        let body = only_comment(&github, pull_request).body;
        assert!(body.contains("`new-package`"));
        assert!(!body.contains("`old-package`"));
        assert!(!body.contains("[!WARNING]"));
    }

    #[test]
    fn marker_lookup_ignores_an_unrelated_comment() {
        let github = FakeGitHub::new();
        let context = context();
        let pull_request = 14;
        block_on(github.create_comment(&context.repository, pull_request, "unrelated")).unwrap();
        block_on(pr_comment_preflight(&github, &context, pull_request, "foo")).unwrap();

        let comments = github.comments_for(pull_request);
        assert_eq!(comments.len(), 2);
        assert!(comments.iter().any(|one| one.body == "unrelated"));
        assert!(
            comments
                .iter()
                .any(|one| message::is_in_progress(&one.body, &context.instance))
        );
    }

    #[test]
    fn publish_marks_results_stale_when_head_advanced() {
        let github = FakeGitHub::new();
        let context = context();
        let pull_request = 8;
        github.set_pull_head(pull_request, sha('b'));
        github.set_comparison(&sha('a'), &sha('b'), Comparison { ahead_by: Some(1) });

        block_on(publish_pr_comment(
            &github,
            &context,
            pull_request,
            &sha('a'),
            "foo",
            "summary",
            Envelope::default(),
        ))
        .unwrap();

        let body = only_comment(&github, pull_request).body;
        assert!(body.contains("1 commit behind HEAD"));
    }

    #[test]
    fn publish_warns_when_freshness_cannot_be_verified() {
        let github = FakeGitHub::new();
        let context = context();
        let pull_request = 11;
        github.fail_pull_head();

        block_on(publish_pr_comment(
            &github,
            &context,
            pull_request,
            &sha('a'),
            "foo",
            "summary",
            Envelope::default(),
        ))
        .unwrap();

        let body = only_comment(&github, pull_request).body;
        assert!(body.contains("freshness could not be verified"));
    }

    #[test]
    fn ambiguous_comment_create_is_reconciled_by_marker() {
        let github = FakeGitHub::new();
        let context = context();
        let pull_request = 9;
        github.set_pull_head(pull_request, sha('a'));
        github.fail_next_comment_create_after_commit();

        block_on(publish_pr_comment(
            &github,
            &context,
            pull_request,
            &sha('a'),
            "foo",
            "summary",
            Envelope::default(),
        ))
        .unwrap();
        assert_eq!(github.comments_for(pull_request).len(), 1);
    }

    #[test]
    fn create_error_survives_a_failed_comment_reconciliation() {
        let github = FakeGitHub::new();
        let context = context();
        let pull_request = 16;
        github.set_pull_head(pull_request, sha('a'));
        github.fail_next_comment_create_after_commit();
        github.fail_comment_list();
        let error = block_on(publish_pr_comment(
            &github,
            &context,
            pull_request,
            &sha('a'),
            "foo",
            "summary",
            Envelope::default(),
        ))
        .unwrap_err();
        assert!(error.find_source::<AmbiguousCreateError>().is_some());
    }

    #[test]
    fn finalize_only_replaces_an_in_progress_comment() {
        let github = FakeGitHub::new();
        let context = context();
        let pull_request = 10;
        github.set_pull_head(pull_request, sha('a'));
        block_on(pr_comment_preflight(&github, &context, pull_request, "foo")).unwrap();
        block_on(pr_comment_finalize(
            &github,
            &context,
            pull_request,
            "https://example.test/run",
        ))
        .unwrap();
        assert!(
            only_comment(&github, pull_request)
                .body
                .contains("did not complete successfully")
        );
    }

    #[test]
    fn cleanup_can_delete_instead_of_leaving_a_note() {
        let github = FakeGitHub::new();
        let context = context();
        let pull_request = 12;
        block_on(pr_comment_preflight(&github, &context, pull_request, "foo")).unwrap();
        block_on(pr_comment_cleanup(&github, &context, pull_request, true)).unwrap();
        assert!(github.comments_for(pull_request).is_empty());
    }
}
