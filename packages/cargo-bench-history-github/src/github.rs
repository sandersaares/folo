use std::future::Future;
use std::time::Duration;
use std::{env, fmt};

use ohno::AppError;
use reqwest::{Method, RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::errors::{
    InvalidResponseError, MissingCreatedArtifactError, MissingTokenError, RequestFailedError,
    UnexpectedStatusError,
};
use crate::model::{CommitSha, Repository};

/// A rolling GitHub issue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Issue {
    pub(crate) number: u64,
    pub(crate) title: String,
    pub(crate) body: String,
}

/// A GitHub pull-request comment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Comment {
    pub(crate) id: u64,
    pub(crate) body: String,
}

/// How far the compared head is ahead of the analyzed commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Comparison {
    pub(crate) ahead_by: Option<u64>,
}

/// Semantic GitHub operations used by the report lifecycles.
pub(crate) trait GitHub {
    fn open_issues(
        &self,
        repository: &Repository,
    ) -> impl Future<Output = Result<Vec<Issue>, AppError>>;

    fn create_issue(
        &self,
        repository: &Repository,
        title: &str,
        body: &str,
    ) -> impl Future<Output = Result<Issue, AppError>>;

    fn update_issue(
        &self,
        repository: &Repository,
        number: u64,
        title: Option<&str>,
        body: &str,
    ) -> impl Future<Output = Result<(), AppError>>;

    fn close_issue(
        &self,
        repository: &Repository,
        number: u64,
    ) -> impl Future<Output = Result<(), AppError>>;

    fn comments(
        &self,
        repository: &Repository,
        pull_request: u64,
    ) -> impl Future<Output = Result<Vec<Comment>, AppError>>;

    fn create_comment(
        &self,
        repository: &Repository,
        pull_request: u64,
        body: &str,
    ) -> impl Future<Output = Result<Comment, AppError>>;

    fn update_comment(
        &self,
        repository: &Repository,
        id: u64,
        body: &str,
    ) -> impl Future<Output = Result<(), AppError>>;

    fn delete_comment(
        &self,
        repository: &Repository,
        id: u64,
    ) -> impl Future<Output = Result<(), AppError>>;

    fn pull_request_head(
        &self,
        repository: &Repository,
        pull_request: u64,
    ) -> impl Future<Output = Result<CommitSha, AppError>>;

    fn compare(
        &self,
        repository: &Repository,
        base: &CommitSha,
        head: &CommitSha,
    ) -> impl Future<Output = Result<Comparison, AppError>>;
}

/// The production GitHub REST adapter.
pub(crate) struct RestGitHub {
    client: reqwest::Client,
    token: SecretToken,
}

impl fmt::Debug for RestGitHub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestGitHub")
            .field("client", &self.client)
            .field("token", &self.token)
            .finish()
    }
}

// The concrete HTTP adapter requires live responses to exercise meaningfully. Its pure
// classifiers and redaction behavior are covered below; lifecycle behavior is covered
// through the fake.
#[cfg_attr(test, mutants::skip)]
impl RestGitHub {
    pub(crate) fn from_env() -> Result<Self, AppError> {
        let token = env::var("GITHUB_TOKEN")
            .or_else(|_| env::var("GH_TOKEN"))
            .map_err(MissingTokenError::caused_by)?;
        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent("cargo-bench-history-github")
                // A stuck API call must fail the workflow rather than occupying a
                // runner forever. GitHub normally responds in seconds; this budget
                // leaves ample room for a degraded service before retry takes over.
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|error| {
                    RequestFailedError::caused_by("building the HTTP client", error)
                })?,
            token: SecretToken(token),
        })
    }

    fn request(&self, method: Method, repository: &Repository, path: &str) -> RequestBuilder {
        let url = format!(
            "https://api.github.com/repos/{}/{}/{}",
            repository.owner(),
            repository.name(),
            path
        );
        self.client
            .request(method, url)
            .bearer_auth(self.token.expose())
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
    }

    async fn send_json<T, F>(&self, operation: &str, build: F) -> Result<T, AppError>
    where
        T: DeserializeOwned,
        F: Fn() -> RequestBuilder,
    {
        let response = self.send(operation, build).await?;
        response
            .json()
            .await
            .map_err(|error| InvalidResponseError::caused_by(operation, error).into())
    }

    async fn send_empty<F>(&self, operation: &str, build: F) -> Result<(), AppError>
    where
        F: Fn() -> RequestBuilder,
    {
        _ = self.send(operation, build).await?;
        Ok(())
    }

    async fn send_delete<F>(&self, operation: &str, build: F) -> Result<(), AppError>
    where
        F: Fn() -> RequestBuilder,
    {
        match self.send(operation, build).await {
            Ok(_) => Ok(()),
            Err(error)
                if error
                    .find_source::<UnexpectedStatusError>()
                    .is_some_and(|status| status.status() == StatusCode::NOT_FOUND.as_u16()) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn send<F>(&self, operation: &str, build: F) -> Result<Response, AppError>
    where
        F: Fn() -> RequestBuilder,
    {
        // Short delays absorb the common API blips without making a deterministic
        // failure costly. The create path deliberately does not use this helper.
        const RETRY_DELAYS: [Duration; 2] =
            [Duration::from_millis(200), Duration::from_millis(800)];

        for delay in RETRY_DELAYS {
            match build().send().await {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) if is_transient_status(response.status()) => {
                    tokio::time::sleep(delay).await;
                }
                Ok(response) => return Err(unexpected_status(operation, response).await),
                Err(error) if is_transient_error(&error) => {
                    tokio::time::sleep(delay).await;
                }
                Err(error) => {
                    return Err(RequestFailedError::caused_by(operation, error).into());
                }
            }
        }

        match build().send().await {
            Ok(response) if response.status().is_success() => Ok(response),
            Ok(response) => Err(unexpected_status(operation, response).await),
            Err(error) => Err(RequestFailedError::caused_by(operation, error).into()),
        }
    }

    async fn send_allow_not_found<F>(
        &self,
        operation: &str,
        build: F,
    ) -> Result<Option<Response>, AppError>
    where
        F: Fn() -> RequestBuilder,
    {
        // Match the ordinary request budget; only the terminal 404 interpretation
        // differs.
        const RETRY_DELAYS: [Duration; 2] =
            [Duration::from_millis(200), Duration::from_millis(800)];

        for delay in RETRY_DELAYS {
            match build().send().await {
                Ok(response) if response.status().is_success() => return Ok(Some(response)),
                Ok(response) if response.status() == StatusCode::NOT_FOUND => return Ok(None),
                Ok(response) if is_transient_status(response.status()) => {
                    tokio::time::sleep(delay).await;
                }
                Ok(response) => return Err(unexpected_status(operation, response).await),
                Err(error) if is_transient_error(&error) => {
                    tokio::time::sleep(delay).await;
                }
                Err(error) => {
                    return Err(RequestFailedError::caused_by(operation, error).into());
                }
            }
        }

        match build().send().await {
            Ok(response) if response.status().is_success() => Ok(Some(response)),
            Ok(response) if response.status() == StatusCode::NOT_FOUND => Ok(None),
            Ok(response) => Err(unexpected_status(operation, response).await),
            Err(error) => Err(RequestFailedError::caused_by(operation, error).into()),
        }
    }

    async fn send_create<T, F>(&self, operation: &str, build: F) -> Result<T, AppError>
    where
        T: DeserializeOwned,
        F: FnOnce() -> RequestBuilder,
    {
        let response = build()
            .send()
            .await
            .map_err(|error| RequestFailedError::caused_by(operation, error))?;
        if !response.status().is_success() {
            return Err(unexpected_status(operation, response).await);
        }
        response
            .json()
            .await
            .map_err(|error| InvalidResponseError::caused_by(operation, error).into())
    }
}

// Every branch below delegates to the live GitHub REST API. Pure retry classification
// and lifecycle decisions are tested separately; mutating these adapters would require
// network tests, which this package deliberately does not perform.
#[cfg_attr(test, mutants::skip)]
impl GitHub for RestGitHub {
    async fn open_issues(&self, repository: &Repository) -> Result<Vec<Issue>, AppError> {
        const PAGE_SIZE: usize = 100;

        let mut page = 1_u64;
        let mut issues = Vec::new();
        loop {
            let values: Vec<IssueResponse> = self
                .send_json("listing open issues", || {
                    self.request(
                        Method::GET,
                        repository,
                        &format!("issues?state=open&per_page={PAGE_SIZE}&page={page}"),
                    )
                })
                .await?;
            let last_page = values.len() < PAGE_SIZE;
            issues.extend(
                values
                    .into_iter()
                    .filter(|one| one.pull_request.is_none())
                    .map(|one| Issue {
                        number: one.number,
                        title: one.title,
                        body: one.body.unwrap_or_default(),
                    }),
            );
            if last_page {
                return Ok(issues);
            }
            page = page
                .checked_add(1)
                .expect("GitHub cannot return u64::MAX pages of issues");
        }
    }

    async fn create_issue(
        &self,
        repository: &Repository,
        title: &str,
        body: &str,
    ) -> Result<Issue, AppError> {
        let request = IssueWrite { title, body };
        let value: IssueResponse = self
            .send_create("creating an issue", || {
                self.request(Method::POST, repository, "issues")
                    .json(&request)
            })
            .await?;
        issue_from_created(value, "creating an issue")
    }

    async fn update_issue(
        &self,
        repository: &Repository,
        number: u64,
        title: Option<&str>,
        body: &str,
    ) -> Result<(), AppError> {
        let request = IssueUpdate { title, body };
        self.send_empty("updating an issue", || {
            self.request(Method::PATCH, repository, &format!("issues/{number}"))
                .json(&request)
        })
        .await
    }

    async fn close_issue(&self, repository: &Repository, number: u64) -> Result<(), AppError> {
        let close = IssueStateWrite { state: "closed" };
        self.send_empty("closing an issue", || {
            self.request(Method::PATCH, repository, &format!("issues/{number}"))
                .json(&close)
        })
        .await
    }

    async fn comments(
        &self,
        repository: &Repository,
        pull_request: u64,
    ) -> Result<Vec<Comment>, AppError> {
        const PAGE_SIZE: usize = 100;

        let mut page = 1_u64;
        let mut comments = Vec::new();
        loop {
            let values: Vec<CommentResponse> = self
                .send_json("listing pull-request comments", || {
                    self.request(
                        Method::GET,
                        repository,
                        &format!("issues/{pull_request}/comments?per_page={PAGE_SIZE}&page={page}"),
                    )
                })
                .await?;
            let last_page = values.len() < PAGE_SIZE;
            comments.extend(values.into_iter().map(|one| Comment {
                id: one.id,
                body: one.body.unwrap_or_default(),
            }));
            if last_page {
                return Ok(comments);
            }
            page = page
                .checked_add(1)
                .expect("GitHub cannot return u64::MAX pages of comments");
        }
    }

    async fn create_comment(
        &self,
        repository: &Repository,
        pull_request: u64,
        body: &str,
    ) -> Result<Comment, AppError> {
        let request = BodyWrite { body };
        let value: CommentResponse = self
            .send_create("creating a pull-request comment", || {
                self.request(
                    Method::POST,
                    repository,
                    &format!("issues/{pull_request}/comments"),
                )
                .json(&request)
            })
            .await?;
        Ok(Comment {
            id: value.id,
            body: value.body.unwrap_or_default(),
        })
    }

    async fn update_comment(
        &self,
        repository: &Repository,
        id: u64,
        body: &str,
    ) -> Result<(), AppError> {
        let request = BodyWrite { body };
        self.send_empty("updating a pull-request comment", || {
            self.request(Method::PATCH, repository, &format!("issues/comments/{id}"))
                .json(&request)
        })
        .await
    }

    async fn delete_comment(&self, repository: &Repository, id: u64) -> Result<(), AppError> {
        self.send_delete("deleting a pull-request comment", || {
            self.request(Method::DELETE, repository, &format!("issues/comments/{id}"))
        })
        .await
    }

    async fn pull_request_head(
        &self,
        repository: &Repository,
        pull_request: u64,
    ) -> Result<CommitSha, AppError> {
        let value: PullResponse = self
            .send_json("reading the pull-request head", || {
                self.request(Method::GET, repository, &format!("pulls/{pull_request}"))
            })
            .await?;
        value.head.sha.parse()
    }

    async fn compare(
        &self,
        repository: &Repository,
        base: &CommitSha,
        head: &CommitSha,
    ) -> Result<Comparison, AppError> {
        let operation = "comparing commits";
        let path = format!("compare/{}...{}", base.as_str(), head.as_str());
        let Some(response) = self
            .send_allow_not_found(operation, || self.request(Method::GET, repository, &path))
            .await?
        else {
            return Ok(Comparison { ahead_by: None });
        };
        let value: CompareResponse = response
            .json()
            .await
            .map_err(|error| InvalidResponseError::caused_by(operation, error))?;
        Ok(Comparison {
            ahead_by: Some(value.ahead_by),
        })
    }
}

#[derive(Deserialize)]
struct IssueResponse {
    number: u64,
    title: String,
    body: Option<String>,
    #[serde(default)]
    pull_request: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct CommentResponse {
    id: u64,
    body: Option<String>,
}

#[derive(Deserialize)]
struct PullResponse {
    head: PullHead,
}

#[derive(Deserialize)]
struct PullHead {
    sha: String,
}

#[derive(Deserialize)]
struct CompareResponse {
    ahead_by: u64,
}

#[derive(Serialize)]
struct IssueWrite<'a> {
    title: &'a str,
    body: &'a str,
}

#[derive(Serialize)]
struct BodyWrite<'a> {
    body: &'a str,
}

#[derive(Serialize)]
struct IssueUpdate<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<&'a str>,
    body: &'a str,
}

#[derive(Serialize)]
struct IssueStateWrite<'a> {
    state: &'a str,
}

struct SecretToken(String);

impl SecretToken {
    fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

fn issue_from_created(value: IssueResponse, operation: &str) -> Result<Issue, AppError> {
    if value.number == 0 {
        return Err(MissingCreatedArtifactError::new(operation).into());
    }
    Ok(Issue {
        number: value.number,
        title: value.title,
        body: value.body.unwrap_or_default(),
    })
}

fn is_transient_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

// This is a direct projection of reqwest's private transport classification; constructing
// each error kind without real IO is not supported by reqwest.
#[cfg_attr(test, mutants::skip)]
fn is_transient_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request()
}

async fn unexpected_status(operation: &str, response: Response) -> AppError {
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .unwrap_or_else(|_| "<response body unavailable>".to_owned());
    UnexpectedStatusError::new(operation, status, body).into()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
pub(crate) mod fake {
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeMap;

    use super::*;
    use crate::errors::{AmbiguousCreateError, RequestFailedError};

    /// An in-memory GitHub port for orchestration tests.
    #[derive(Debug, Default)]
    pub(crate) struct FakeGitHub {
        issues: RefCell<BTreeMap<u64, Issue>>,
        comments: RefCell<BTreeMap<u64, (u64, Comment)>>,
        pull_heads: RefCell<BTreeMap<u64, CommitSha>>,
        comparisons: RefCell<BTreeMap<(String, String), Comparison>>,
        next_id: Cell<u64>,
        fail_issue_create_after_commit: Cell<bool>,
        fail_comment_create_after_commit: Cell<bool>,
        fail_issue_list: Cell<bool>,
        fail_comment_list: Cell<bool>,
        issue_list_calls: Cell<usize>,
        comment_list_calls: Cell<usize>,
        fail_pull_head: Cell<bool>,
    }

    impl FakeGitHub {
        pub(crate) fn new() -> Self {
            Self {
                next_id: Cell::new(1),
                ..Self::default()
            }
        }

        pub(crate) fn issues(&self) -> Vec<Issue> {
            self.issues.borrow().values().cloned().collect()
        }

        pub(crate) fn comments_for(&self, pull_request: u64) -> Vec<Comment> {
            self.comments
                .borrow()
                .values()
                .filter(|(pr, _)| *pr == pull_request)
                .map(|(_, comment)| comment.clone())
                .collect()
        }

        pub(crate) fn set_pull_head(&self, pull_request: u64, sha: CommitSha) {
            self.pull_heads.borrow_mut().insert(pull_request, sha);
        }

        pub(crate) fn set_comparison(
            &self,
            base: &CommitSha,
            head: &CommitSha,
            comparison: Comparison,
        ) {
            self.comparisons.borrow_mut().insert(
                (base.as_str().to_owned(), head.as_str().to_owned()),
                comparison,
            );
        }

        pub(crate) fn fail_next_issue_create_after_commit(&self) {
            self.fail_issue_create_after_commit.set(true);
        }

        pub(crate) fn fail_next_comment_create_after_commit(&self) {
            self.fail_comment_create_after_commit.set(true);
        }

        pub(crate) fn fail_issue_list(&self) {
            self.fail_issue_list.set(true);
        }

        pub(crate) fn fail_comment_list(&self) {
            self.fail_comment_list.set(true);
        }

        pub(crate) fn fail_pull_head(&self) {
            self.fail_pull_head.set(true);
        }

        fn next_id(&self) -> u64 {
            let id = self.next_id.get();
            self.next_id.set(
                id.checked_add(1)
                    .expect("the in-memory fake cannot create u64::MAX artifacts"),
            );
            id
        }
    }

    impl GitHub for FakeGitHub {
        async fn open_issues(&self, _repository: &Repository) -> Result<Vec<Issue>, AppError> {
            let calls = self.issue_list_calls.get();
            self.issue_list_calls.set(
                calls
                    .checked_add(1)
                    .expect("the fake cannot perform usize::MAX issue-list calls"),
            );
            if self.fail_issue_list.get() && calls != 0 {
                return Err(RequestFailedError::new("listing fake issues").into());
            }
            Ok(self.issues())
        }

        async fn create_issue(
            &self,
            _repository: &Repository,
            _title: &str,
            body: &str,
        ) -> Result<Issue, AppError> {
            let issue = Issue {
                number: self.next_id(),
                title: _title.to_owned(),
                body: body.to_owned(),
            };
            self.issues.borrow_mut().insert(issue.number, issue.clone());
            if self.fail_issue_create_after_commit.replace(false) {
                return Err(AmbiguousCreateError::new().into());
            }
            Ok(issue)
        }

        async fn update_issue(
            &self,
            _repository: &Repository,
            number: u64,
            title: Option<&str>,
            body: &str,
        ) -> Result<(), AppError> {
            if let Some(issue) = self.issues.borrow_mut().get_mut(&number) {
                if let Some(title) = title {
                    issue.title = title.to_owned();
                }
                issue.body = body.to_owned();
            }
            Ok(())
        }

        async fn close_issue(&self, _repository: &Repository, number: u64) -> Result<(), AppError> {
            self.issues.borrow_mut().remove(&number);
            Ok(())
        }

        async fn comments(
            &self,
            _repository: &Repository,
            pull_request: u64,
        ) -> Result<Vec<Comment>, AppError> {
            let calls = self.comment_list_calls.get();
            self.comment_list_calls.set(
                calls
                    .checked_add(1)
                    .expect("the fake cannot perform usize::MAX comment-list calls"),
            );
            if self.fail_comment_list.get() && calls != 0 {
                return Err(RequestFailedError::new("listing fake comments").into());
            }
            Ok(self.comments_for(pull_request))
        }

        async fn create_comment(
            &self,
            _repository: &Repository,
            pull_request: u64,
            body: &str,
        ) -> Result<Comment, AppError> {
            let comment = Comment {
                id: self.next_id(),
                body: body.to_owned(),
            };
            self.comments
                .borrow_mut()
                .insert(comment.id, (pull_request, comment.clone()));
            if self.fail_comment_create_after_commit.replace(false) {
                return Err(AmbiguousCreateError::new().into());
            }
            Ok(comment)
        }

        async fn update_comment(
            &self,
            _repository: &Repository,
            id: u64,
            body: &str,
        ) -> Result<(), AppError> {
            if let Some((_, comment)) = self.comments.borrow_mut().get_mut(&id) {
                comment.body = body.to_owned();
            }
            Ok(())
        }

        async fn delete_comment(&self, _repository: &Repository, id: u64) -> Result<(), AppError> {
            self.comments.borrow_mut().remove(&id);
            Ok(())
        }

        async fn pull_request_head(
            &self,
            _repository: &Repository,
            pull_request: u64,
        ) -> Result<CommitSha, AppError> {
            if self.fail_pull_head.get() {
                return Err(RequestFailedError::new("reading the fake pull-request head").into());
            }
            Ok(self
                .pull_heads
                .borrow()
                .get(&pull_request)
                .cloned()
                .expect("the test must seed the pull-request head"))
        }

        async fn compare(
            &self,
            _repository: &Repository,
            base: &CommitSha,
            head: &CommitSha,
        ) -> Result<Comparison, AppError> {
            Ok(self
                .comparisons
                .borrow()
                .get(&(base.as_str().to_owned(), head.as_str().to_owned()))
                .copied()
                .unwrap_or(Comparison { ahead_by: None }))
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn token_debug_is_redacted() {
        let token = SecretToken("secret".to_owned());
        assert_eq!(format!("{token:?}"), "[REDACTED]");
        assert_eq!(token.expose(), "secret");
    }

    #[test]
    fn retry_status_classifier_is_narrow() {
        assert!(is_transient_status(StatusCode::REQUEST_TIMEOUT));
        assert!(is_transient_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_transient_status(StatusCode::BAD_GATEWAY));
        assert!(!is_transient_status(StatusCode::UNAUTHORIZED));
        assert!(!is_transient_status(StatusCode::NOT_FOUND));
    }

    #[test]
    fn created_issue_requires_a_nonzero_number() {
        let missing = IssueResponse {
            number: 0,
            title: "title".to_owned(),
            body: Some("body".to_owned()),
            pull_request: None,
        };
        issue_from_created(missing, "creating").unwrap_err();

        let created = IssueResponse {
            number: 7,
            title: "title".to_owned(),
            body: Some("body".to_owned()),
            pull_request: None,
        };
        assert_eq!(issue_from_created(created, "creating").unwrap().number, 7);
    }
}
