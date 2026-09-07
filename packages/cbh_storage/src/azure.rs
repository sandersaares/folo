//! [`AzureBlobStorage`]: a [`Storage`] backed by an Azure Blob Storage container.
//!
//! Object keys map directly to blob names, so the key model is identical to
//! [`LocalStorage`](super::local::LocalStorage): write-once objects and
//! list-by-prefix, with no special-casing by callers. Hierarchical keys become
//! real `/`-separated blob names (the blob URL is built segment by segment so the
//! separators are never percent-encoded), which makes prefix listing line up with
//! the partition layout.
//!
//! Authentication is Microsoft Entra ID (OAuth), resolved once at construction:
//! a token credential is attached to every request and the endpoint is always
//! HTTPS. See [`entra_credential`] for how the credential itself is discovered
//! (ambient developer/CLI session, or self-minted GitHub OIDC in CI).

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::{env, fmt};

use azure_core::Uuid;
use azure_core::credentials::{AccessToken, TokenCredential, TokenRequestOptions};
use azure_core::error::ErrorKind;
use azure_core::http::{
    ClientOptions, HttpClient, HttpClientOptions, RequestContent, StatusCode, Transport, Url,
    new_http_client,
};
use azure_core::time::{Duration, OffsetDateTime};
use azure_identity::DeveloperToolsCredential;
use azure_storage_blob::models::{
    BlobClientGetPropertiesResultHeaders as _, BlobClientUploadOptions,
    BlobContainerClientListBlobsOptions, StorageErrorCode,
};
use azure_storage_blob::{
    BlobClient, BlobClientOptions, BlobContainerClient, BlobContainerClientOptions,
};
use cbh_diag::{Reporter, ReporterExt};
use futures::TryStreamExt as _;
use jiff::Timestamp;

use crate::{
    AzureBlobOperationError, DecompressAzureBlobError, ObjectAlreadyExistsError,
    ObjectNotFoundError, PendingInvalidation, Storage, StorageConfigurationError, StorageError,
    cache_epoch_key, github_oidc, validate_key,
};

/// The HTTP content coding declared on every uploaded blob. The storage layer
/// always stores gzip, so this header is unconditionally truthful and lets a
/// non-SDK reader inflate the blob with standard tooling (the backend still
/// inflates on [`get`](AzureBlobStorage::get) itself rather than relying on the
/// service to decode).
const GZIP_CONTENT_ENCODING: &str = "gzip";

/// The blob metadata key that associates a committed conditional create with the
/// request that wrote it.
///
/// The SDK can retry a request after Azure committed it but the response was lost.
/// Persisting the request identity atomically with the body lets the resulting
/// collision be distinguished from an object written by another request.
const CONDITIONAL_CREATE_ID_METADATA_KEY: &str = "cbh_create_id";

/// A [`Storage`] that persists objects as blobs in an Azure Blob container.
#[derive(Clone)]
pub struct AzureBlobStorage {
    /// The container endpoint URL. It carries no secret: authentication is a
    /// per-request Entra token, not a signed query.
    container_endpoint: Url,
    /// The Entra ID token credential attached to every request.
    credential: Arc<dyn TokenCredential>,
    /// One pooled HTTP client shared by every per-object blob and container
    /// client (injected via the transport seam in [`shared_client_options`]).
    /// All operations then reuse a single `reqwest` connection pool, so the
    /// backend keeps TCP+TLS connections alive across objects instead of paying
    /// a fresh handshake per object (and exhausting ephemeral ports at high
    /// fetch concurrency). Built once in [`from_config`].
    ///
    /// [`shared_client_options`]: AzureBlobStorage::shared_client_options
    /// [`from_config`]: AzureBlobStorage::from_config
    http_client: Arc<dyn HttpClient>,
    /// One-shot flag, shared across clones, that records whether this backend has
    /// removed or overwritten an existing object since the last flush. A read
    /// command's [`flush_pending_invalidation`] consults it to decide whether to
    /// bump this project's cache-invalidation marker. Held behind an [`Arc`] so a
    /// clone made deeper in the stack still arms the same flag.
    ///
    /// [`flush_pending_invalidation`]: AzureBlobStorage::flush_pending_invalidation
    invalidation: Arc<PendingInvalidation>,
}

impl fmt::Debug for AzureBlobStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The container endpoint carries no secret (Entra supplies a per-request
        // token), so it is shown in full. `http_client` is an opaque shared
        // transport with no meaningful debug representation; omit it explicitly.
        f.debug_struct("AzureBlobStorage")
            .field("endpoint", &self.container_endpoint.as_str())
            .finish_non_exhaustive()
    }
}

impl AzureBlobStorage {
    /// Builds an Azure backend from its configured parameters, authenticating
    /// with Microsoft Entra ID.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if the endpoint is not a valid base URL or is not
    /// HTTPS (Entra ID authentication requires TLS).
    pub(crate) fn from_config(
        account: &str,
        container: &str,
        endpoint: Option<String>,
    ) -> Result<Self, StorageError> {
        let container_endpoint = container_endpoint_url(account, container, endpoint)?;

        // Build one HTTP client up front and share it across every per-object blob
        // and container client (via the transport seam in `shared_client_options`),
        // so all operations reuse a single connection pool and keep TCP+TLS
        // connections alive instead of paying a fresh handshake per object.
        // `automatic_decompression` is left off to match the SDK's own per-client
        // default: the storage layer stores gzip and inflates it itself in `get`, so
        // the transport must hand back the raw compressed bytes (turning
        // auto-decompression on would double-inflate). The Entra OIDC credential
        // reuses the same client for its token `GET`; that endpoint is never gzipped
        // (the request advertises no `Accept-Encoding`), so decompression staying off
        // is correct there too.
        let http_client = new_http_client(Some(HttpClientOptions {
            automatic_decompression: false,
        }));

        let credential = entra_credential(&http_client)?;
        // Wrap the credential so a burst of concurrent reads shares one token
        // instead of each driving its own token acquisition (see `CachingCredential`).
        let credential: Arc<dyn TokenCredential> = Arc::new(CachingCredential::new(credential));

        Ok(Self {
            container_endpoint,
            credential,
            http_client,
            invalidation: Arc::new(PendingInvalidation::default()),
        })
    }

    /// Assembles a backend from already-built parts, bypassing credential and
    /// transport discovery.
    ///
    /// This is the injection seam the Azurite tests use (directly, and through the
    /// [`azure_backend_from_parts`](crate::azure_backend_from_parts) command seam):
    /// they supply a fake token credential (accepted by Azurite's `--oauth basic`
    /// structural check) and an HTTP client that trusts the emulator's self-signed
    /// certificate, neither of which [`from_config`](Self::from_config) would
    /// produce.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if the endpoint is not a valid base URL or is not
    /// HTTPS (Entra ID authentication requires TLS, even through this abstraction).
    pub(crate) fn from_parts(
        account: &str,
        container: &str,
        endpoint: Option<String>,
        credential: Arc<dyn TokenCredential>,
        http_client: Arc<dyn HttpClient>,
    ) -> Result<Self, StorageError> {
        let container_endpoint = container_endpoint_url(account, container, endpoint)?;
        Ok(Self {
            container_endpoint,
            credential,
            http_client,
            invalidation: Arc::new(PendingInvalidation::default()),
        })
    }

    /// The client options every per-object client is built with: a transport
    /// backed by the one shared, pooled [`http_client`](Self::http_client), so
    /// all operations reuse a single `reqwest` connection pool (keep-alive)
    /// rather than each opening its own and handshaking afresh.
    // Mutation-skipped: injecting the shared transport is a connection-reuse
    // performance optimization. A client built with default options round-trips
    // identically against Azure (only its pooling differs), so no behavioral test
    // can distinguish carrying the shared transport from not, and a mutant that
    // drops it makes the SDK build a default client that hangs the round-trip.
    #[cfg_attr(test, mutants::skip)]
    fn shared_client_options(&self) -> ClientOptions {
        ClientOptions {
            transport: Some(Transport::new(Arc::clone(&self.http_client))),
            ..Default::default()
        }
    }

    /// Builds a client for the blob named `key`, constructing the URL one path
    /// segment at a time so `/` separators in the key stay literal.
    // Mutation-skipped: the surviving mutant only drops the shared-transport
    // client options, an unobservable connection-reuse optimization (see
    // `shared_client_options`). The URL plumbing is exercised by the Azurite
    // round-trip and prefix-escape tests, which mutation testing cannot run.
    #[cfg_attr(test, mutants::skip)]
    fn blob_client(&self, key: &str) -> Result<BlobClient, StorageError> {
        let mut url = self.container_endpoint.clone();
        url.path_segments_mut()
            .map_err(|()| config_error("Azure endpoint cannot be a base URL"))?
            .extend(key.split('/'));
        let options = BlobClientOptions {
            client_options: self.shared_client_options(),
            ..Default::default()
        };
        BlobClient::new(url, Some(Arc::clone(&self.credential)), Some(options)).map_err(|error| {
            azure_io(
                format!("could not initialize Azure blob client for {key:?}"),
                error,
            )
        })
    }

    /// Builds a client for the configured container.
    // Mutation-skipped: the surviving mutant only drops the shared-transport
    // client options, an unobservable connection-reuse optimization (see
    // `shared_client_options`). The container round-trip is covered by the
    // Azurite tests, which mutation testing cannot run.
    #[cfg_attr(test, mutants::skip)]
    fn container_client(&self) -> Result<BlobContainerClient, StorageError> {
        let options = BlobContainerClientOptions {
            client_options: self.shared_client_options(),
            ..Default::default()
        };
        BlobContainerClient::new(
            self.container_endpoint.clone(),
            Some(Arc::clone(&self.credential)),
            Some(options),
        )
        .map_err(|error| azure_io("could not initialize Azure blob container client", error))
    }

    /// Creates the container, treating an already-existing container as success.
    #[cfg_attr(test, mutants::skip)] // Delegates to the Azure SDK; verified by the Azurite round-trip tests, which mutation testing cannot run.
    async fn ensure_container(&self) -> Result<(), StorageError> {
        let client = self.container_client()?;
        match client.create(None).await {
            Ok(_) => Ok(()),
            // Only this exact service code proves that the intended container already exists.
            // Treating every conflict as success would hide lease and service-condition failures
            // that must remain operation errors with their Azure SDK sources intact.
            Err(error)
                if has_storage_error_code(&error, &StorageErrorCode::ContainerAlreadyExists) =>
            {
                Ok(())
            }
            Err(error) => Err(azure_io("could not create Azure blob container", error)),
        }
    }

    /// Uploads `bytes` at `client`, creating the container and retrying once if it
    /// does not exist yet. `mode` selects write-once, replacing, or cache-marker
    /// semantics (see [`upload`]).
    #[cfg_attr(test, mutants::skip)] // Delegates to the Azure SDK; verified by the Azurite round-trip tests, which mutation testing cannot run.
    async fn upload_with_retry(
        &self,
        client: &BlobClient,
        bytes: &[u8],
        key: &str,
        mode: UploadMode,
    ) -> Result<(), StorageError> {
        let error = match upload(client, bytes, &mode).await {
            Ok(()) => return Ok(()),
            Err(error) if matches!(classify(&error), Fault::ContainerMissing) => {
                // The container does not exist yet; create it and retry once.
                self.ensure_container().await?;
                match upload(client, bytes, &mode).await {
                    Ok(()) => return Ok(()),
                    Err(error) => error,
                }
            }
            Err(error) => error,
        };

        resolve_upload_error(client, error, key, &mode).await
    }

    /// Writes a fresh cache-invalidation marker for `project` if this backend has
    /// removed or overwritten an object since the last flush, coalescing every
    /// mutation in the command into a single marker write.
    ///
    /// The marker carries an opaque epoch token (the flush instant rendered
    /// RFC 3339); only whether the token *differs* matters to a reader, so reading
    /// the wall clock here — rather than threading a clock down — is harmless (the
    /// worst case under skew is one extra cache wipe). The write goes through the
    /// internal upload path, so writing the marker does not itself re-arm the flag.
    #[cfg_attr(test, mutants::skip)] // Delegates to the Azure SDK; verified by the Azurite round-trip tests, which mutation testing cannot run.
    pub(crate) async fn flush_pending_invalidation(
        &self,
        project: &str,
        reporter: &dyn Reporter,
    ) -> Result<(), StorageError> {
        if !self.invalidation.take() {
            return Ok(());
        }
        let key = cache_epoch_key(project);
        let token = Timestamp::now().to_string();
        reporter.note_with(|| {
            format!(
                "cache: a delete or overwrite reached the cloud this command, so caches keyed on \
                 older data are now stale; bumping the invalidation marker {key} to epoch {token}"
            )
        });
        let client = self.blob_client(&key)?;
        let compressed = cbh_codec::compress(token.as_bytes());
        self.upload_with_retry(&client, &compressed, &key, UploadMode::InvalidationMarker)
            .await
    }
}

impl Storage for AzureBlobStorage {
    #[cfg_attr(test, mutants::skip)] // Delegates to the Azure SDK; verified by the Azurite round-trip tests, which mutation testing cannot run.
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
        validate_key(key)?;
        let client = self.blob_client(key)?;
        let compressed = cbh_codec::compress(bytes);
        self.upload_with_retry(&client, &compressed, key, UploadMode::conditional_create())
            .await
    }

    #[cfg_attr(test, mutants::skip)] // Delegates to the Azure SDK; the create-vs-replace arming is verified by the Azurite round-trip tests, which mutation testing cannot run.
    async fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
        validate_key(key)?;
        let client = self.blob_client(key)?;
        let compressed = cbh_codec::compress(bytes);
        // Probe with a write-once upload first. Creating a brand-new key leaves
        // per-key immutability intact and no cache mirrors it yet, so it must not
        // invalidate anything (a read-through mirror discovers new keys via its
        // always-fresh listing). Only a genuine replace of an existing object can
        // leave a stale cached copy, so only that path arms the flag a later flush
        // consults to bump this project's cache-invalidation marker.
        match self
            .upload_with_retry(&client, &compressed, key, UploadMode::conditional_create())
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if error.already_existing_key().is_some() => {
                self.upload_with_retry(&client, &compressed, key, UploadMode::Overwrite)
                    .await?;
                self.invalidation.arm();
                Ok(())
            }
            Err(other) => Err(other),
        }
    }

    #[cfg_attr(test, mutants::skip)] // Delegates to the Azure SDK; verified by the Azurite round-trip tests, which mutation testing cannot run.
    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        validate_key(key)?;
        let client = self.blob_client(key)?;
        match client.download(None).await {
            Ok(response) => {
                let bytes = response.body.collect().await.map_err(|error| {
                    azure_io(format!("could not read Azure blob {key:?}"), error)
                })?;
                cbh_codec::decompress(&bytes).map_err(|error| {
                    DecompressAzureBlobError::caused_by(key.to_owned(), error).into()
                })
            }
            Err(error) if matches!(classify(&error), Fault::NotFound | Fault::ContainerMissing) => {
                Err(ObjectNotFoundError::caused_by(key.to_owned(), error).into())
            }
            Err(error) => Err(azure_io(
                format!("could not download Azure blob {key:?}"),
                error,
            )),
        }
    }

    #[cfg_attr(test, mutants::skip)] // Delegates to the Azure SDK; verified by the Azurite round-trip tests, which mutation testing cannot run.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let client = self.container_client()?;
        let mut pager = client
            .list_blobs(Some(BlobContainerClientListBlobsOptions {
                prefix: Some(prefix.to_owned()),
                ..Default::default()
            }))
            .map_err(|error| {
                azure_io(
                    format!("could not list Azure blobs with prefix {prefix:?}"),
                    error,
                )
            })?;

        let mut keys = Vec::new();
        loop {
            match pager.try_next().await {
                Ok(Some(item)) => {
                    if let Some(name) = item.name {
                        keys.push(name);
                    }
                }
                Ok(None) => break,
                // A missing container holds no objects, mirroring a missing local
                // storage root listing as empty rather than erroring.
                Err(error) if matches!(classify(&error), Fault::ContainerMissing) => {
                    return Ok(Vec::new());
                }
                Err(error) => {
                    return Err(azure_io(
                        format!("could not list Azure blobs with prefix {prefix:?}"),
                        error,
                    ));
                }
            }
        }
        keys.sort();
        Ok(keys)
    }

    #[cfg_attr(test, mutants::skip)] // Delegates to the Azure SDK; verified by the Azurite round-trip tests, which mutation testing cannot run.
    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        validate_key(key)?;
        let client = self.blob_client(key)?;
        match client.delete(None).await {
            Ok(_) => {
                // Removing an object breaks per-key immutability, so any local
                // cache mirroring it is now stale: arm the flag a later flush
                // consults to bump this project's cache-invalidation marker.
                self.invalidation.arm();
                Ok(())
            }
            Err(error) if matches!(classify(&error), Fault::NotFound | Fault::ContainerMissing) => {
                Err(ObjectNotFoundError::caused_by(key.to_owned(), error).into())
            }
            Err(error) => Err(azure_io(
                format!("could not delete Azure blob {key:?}"),
                error,
            )),
        }
    }
}

/// Resolves the Entra ID token credential used against the HTTPS Blob endpoint.
///
/// In a GitHub Actions job configured for Azure federation it self-mints fresh OIDC
/// assertions (see [`github_oidc`]), which keeps a long collection run authenticated
/// past the first hourly access-token refresh — a single `azure/login` session
/// cannot, because the assertion it caches expires within minutes. Everywhere else
/// (local development, and the `test-azure` CI job that signs in with `azure/login`)
/// it falls back to [`DeveloperToolsCredential`], which discovers the existing
/// Azure CLI session.
///
/// This thin wrapper only supplies the live process environment to the testable
/// [`entra_credential_from`] seam; it is coverage- and mutation-excluded because its
/// sole behavior — reading ambient OS environment variables — cannot be driven from a
/// unit test without mutating the global process environment (which this crate's tests
/// avoid). The `test-azure-gh` job exercises it end to end in a real federated job.
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
fn entra_credential(
    http_client: &Arc<dyn HttpClient>,
) -> Result<Arc<dyn TokenCredential>, StorageError> {
    entra_credential_from(|key| env::var(key).ok(), http_client)
}

/// Resolves the Entra credential from an arbitrary environment getter, so both the
/// self-minting and fallback branches are unit-testable without mutating the global
/// process environment. With all GitHub OIDC federation variables present it returns
/// the self-minting assertion credential; otherwise it falls back to the developer
/// credential.
fn entra_credential_from(
    get: impl Fn(&str) -> Option<String>,
    http_client: &Arc<dyn HttpClient>,
) -> Result<Arc<dyn TokenCredential>, StorageError> {
    if let Some(result) = github_oidc::credential_from(get, http_client) {
        return result;
    }
    developer_tools_credential()
}

/// Discovers the ambient Azure CLI / developer credential.
///
/// Coverage-excluded because both of its outcomes depend on the host: it succeeds only
/// where a developer or CI Azure session exists and reports an error only where the
/// developer-tooling discovery itself fails, neither of which a unit test can stage. It
/// is isolated to a single call so the surrounding [`entra_credential_from`] resolution
/// logic stays testable.
#[cfg_attr(coverage_nightly, coverage(off))]
fn developer_tools_credential() -> Result<Arc<dyn TokenCredential>, StorageError> {
    let credential: Arc<dyn TokenCredential> =
        DeveloperToolsCredential::new(None).map_err(|error| {
            StorageConfigurationError::caused_by(
                "could not initialize Entra ID credential".to_owned(),
                error,
            )
        })?;
    Ok(credential)
}

/// How long before a cached token's stated expiry it is treated as stale and
/// re-acquired, so a token is never handed out only to expire while the request
/// that just read it is still in flight.
const TOKEN_REFRESH_MARGIN: Duration = Duration::minutes(5);

/// A [`TokenCredential`] decorator that serializes token acquisition and caches
/// each scope's most recent token, so a burst of concurrent reads shares one
/// token instead of each driving a separate acquisition of the wrapped
/// credential.
///
/// The Entra credential ([`DeveloperToolsCredential`]) shells out to the Azure
/// CLI on every uncached `get_token`. Without this decorator, `analyze`'s
/// concurrent object loads fan out into that many simultaneous
/// `az account get-access-token` invocations; on Windows they collide on the
/// exclusive MSAL token-cache lockfile and the whole read fails with a permission
/// error. Holding the cache lock across the inner acquisition means only one
/// acquisition is ever in flight, so later callers reuse the freshly cached token
/// rather than racing the CLI.
struct CachingCredential {
    /// The wrapped credential that performs the real token acquisition.
    inner: Arc<dyn TokenCredential>,
    /// The most recent token per requested scope set. The lock is deliberately
    /// held across the inner acquisition, so concurrent callers serialize on it
    /// and share a single fetch rather than stampeding the wrapped credential.
    cache: futures::lock::Mutex<HashMap<Vec<String>, AccessToken>>,
    /// The wall-clock source used to decide whether a cached token is still
    /// fresh. Injected so the freshness logic is deterministic under test.
    now: fn() -> OffsetDateTime,
}

impl fmt::Debug for CachingCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachingCredential")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl CachingCredential {
    /// Wraps `inner`, reading the real wall clock for token-freshness decisions.
    fn new(inner: Arc<dyn TokenCredential>) -> Self {
        Self::with_clock(inner, OffsetDateTime::now_utc)
    }

    /// Wraps `inner` with an explicit clock, so tests pin token freshness
    /// deterministically instead of reading the wall clock.
    fn with_clock(inner: Arc<dyn TokenCredential>, now: fn() -> OffsetDateTime) -> Self {
        Self {
            inner,
            cache: futures::lock::Mutex::new(HashMap::new()),
            now,
        }
    }
}

#[async_trait::async_trait]
impl TokenCredential for CachingCredential {
    async fn get_token(
        &self,
        scopes: &[&str],
        options: Option<TokenRequestOptions<'_>>,
    ) -> azure_core::Result<AccessToken> {
        let cache_key: Vec<String> = scopes.iter().map(|scope| (*scope).to_owned()).collect();

        // Holding the lock across the inner acquisition is the whole point: it
        // collapses a concurrent burst into one acquisition whose result the
        // other callers then read from the cache.
        let mut cache = self.cache.lock().await;
        if let Some(token) = cache.get(&cache_key)
            && token_is_fresh(token, (self.now)())
        {
            return Ok(token.clone());
        }

        let token = self.inner.get_token(scopes, options).await?;
        cache.insert(cache_key, token.clone());
        Ok(token)
    }
}

/// Whether `token` is still far enough from its expiry to reuse at `now`,
/// keeping a [`TOKEN_REFRESH_MARGIN`] safety margin so it cannot expire while a
/// request that just read it is still in flight.
///
/// A `now` so close to the maximum representable date that adding the margin
/// overflows is treated as not fresh (re-acquire), which is the safe default.
fn token_is_fresh(token: &AccessToken, now: OffsetDateTime) -> bool {
    now.checked_add(TOKEN_REFRESH_MARGIN)
        .is_some_and(|deadline| token.expires_on > deadline)
}

/// How an upload participates in the storage protocol.
#[derive(Debug)]
enum UploadMode {
    /// Creates a blob only when its key is unoccupied, carrying a unique identity
    /// that remains stable across every retry of this logical upload.
    ConditionalCreate { create_id: String },
    /// Replaces the object stored at a key.
    Overwrite,
    /// Replaces a project's cache-invalidation marker.
    InvalidationMarker,
}

impl UploadMode {
    /// Starts one logical conditional-create upload.
    fn conditional_create() -> Self {
        Self::ConditionalCreate {
            create_id: Uuid::new_v4().to_string(),
        }
    }

    /// Whether the upload must carry the conditional-create request option.
    fn is_conditional_create(&self) -> bool {
        matches!(self, Self::ConditionalCreate { .. })
    }

    /// Returns the request identity persisted by a conditional create.
    fn conditional_create_id(&self) -> Option<&str> {
        match self {
            Self::ConditionalCreate { create_id } => Some(create_id),
            Self::Overwrite | Self::InvalidationMarker => None,
        }
    }

    /// Returns the metadata that must be persisted with this upload.
    fn metadata(&self) -> Option<HashMap<String, String>> {
        self.conditional_create_id().map(|create_id| {
            HashMap::from([(
                CONDITIONAL_CREATE_ID_METADATA_KEY.to_owned(),
                create_id.to_owned(),
            )])
        })
    }

    /// Describes the attempted upload while retaining its mode and key.
    fn operation(&self, key: &str) -> String {
        match self {
            Self::ConditionalCreate { .. } => {
                format!("could not conditionally create Azure blob {key:?}")
            }
            Self::Overwrite => format!("could not overwrite Azure blob {key:?}"),
            Self::InvalidationMarker => {
                format!("could not write Azure cache-invalidation marker {key:?}")
            }
        }
    }
}

/// Uploads `bytes` to `client` using `mode`'s request precondition.
#[cfg_attr(test, mutants::skip)] // Delegates to the Azure SDK; verified by the Azurite round-trip tests, which mutation testing cannot run.
async fn upload(client: &BlobClient, bytes: &[u8], mode: &UploadMode) -> azure_core::Result<()> {
    let mut options = BlobClientUploadOptions {
        // The body is always gzip, so declare it: a non-SDK reader can then
        // inflate the blob with standard tooling.
        blob_content_encoding: Some(GZIP_CONTENT_ENCODING.to_owned()),
        metadata: mode.metadata(),
        ..Default::default()
    };
    if mode.is_conditional_create() {
        options = options.if_not_exists();
    }
    client
        .upload(RequestContent::from(bytes.to_vec()), Some(options))
        .await?;
    Ok(())
}

/// Builds the container endpoint URL from the configured account, container, and
/// optional endpoint override, defaulting to the public blob endpoint for
/// `account`.
///
/// # Errors
///
/// Returns a [`StorageError`] if the endpoint is not a valid base URL or is not
/// HTTPS (Entra ID authentication requires TLS).
fn container_endpoint_url(
    account: &str,
    container: &str,
    endpoint: Option<String>,
) -> Result<Url, StorageError> {
    let endpoint = endpoint.unwrap_or_else(|| format!("https://{account}.blob.core.windows.net"));
    let mut container_endpoint = Url::parse(&endpoint).map_err(|error| {
        StorageConfigurationError::caused_by(format!("invalid Azure endpoint {endpoint:?}"), error)
    })?;
    container_endpoint
        .path_segments_mut()
        .map_err(|()| config_error(format!("Azure endpoint {endpoint:?} cannot be a base URL")))?
        .pop_if_empty()
        .push(container);

    // Entra ID authentication requires TLS, so reject any non-HTTPS endpoint here
    // rather than after building the transport. Centralizing the check means both
    // `from_config` and the `from_parts` injection seam enforce it, and it never
    // depends on first constructing the HTTP client — which would pull in `reqwest`
    // and make the check un-runnable under Miri.
    if container_endpoint.scheme() != "https" {
        return Err(config_error(
            "Azure Entra ID authentication requires an https endpoint",
        ));
    }

    Ok(container_endpoint)
}

/// The kind of fault an Azure error represents, in terms the storage model cares
/// about.
#[derive(Clone, Copy)]
enum Fault {
    /// The requested blob does not exist.
    NotFound,
    /// The container does not exist.
    ContainerMissing,
    /// Any other failure.
    Other,
}

/// Renders the Azure SDK's complete diagnostic while retaining its typed source.
#[derive(Debug)]
struct AzureSdkDiagnosticError(azure_core::Error);

impl fmt::Display for AzureSdkDiagnosticError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The SDK retry layer can replace the outer Display message with an opaque retry
        // message that hides the underlying credential, transport, or service failure. Debug
        // deliberately retains the complete diagnostic, while Error::source below keeps the
        // typed source chain traversable. Revisit this substitution if the SDK makes its outer
        // Display complete or stops providing the full failure through Debug.
        fmt::Debug::fmt(&self.0, f)
    }
}

impl Error for AzureSdkDiagnosticError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

/// Returns whether `error` carries the exact Azure Storage error `code`.
fn has_storage_error_code(error: &azure_core::Error, code: &StorageErrorCode) -> bool {
    matches!(
        error.kind(),
        ErrorKind::HttpResponse {
            error_code: Some(error_code),
            ..
        } if error_code == code.as_ref()
    )
}

/// Classifies an Azure error by HTTP status and storage error code.
fn classify(error: &azure_core::Error) -> Fault {
    if has_storage_error_code(error, &StorageErrorCode::ContainerNotFound) {
        return Fault::ContainerMissing;
    }
    match error.http_status() {
        Some(StatusCode::NotFound) => Fault::NotFound,
        _ => Fault::Other,
    }
}

/// Resolves an upload failure using the request mode that produced it.
async fn resolve_upload_error(
    client: &BlobClient,
    error: azure_core::Error,
    key: &str,
    mode: &UploadMode,
) -> Result<(), StorageError> {
    let Some(create_id) = mode.conditional_create_id() else {
        return Err(map_upload_error(error, key, mode));
    };
    if !has_storage_error_code(&error, &StorageErrorCode::BlobAlreadyExists) {
        return Err(map_upload_error(error, key, mode));
    }

    // A transport retry can receive BlobAlreadyExists after its own first attempt
    // committed. Only an identity persisted by this logical upload proves that the
    // collision is the successful result of that attempt. A different or absent
    // identity remains an ordinary write-once collision.
    // If a concurrent mutation removes the blob before this probe, the resulting
    // operation error preserves the uncertainty instead of guessing success.
    let properties = client
        .get_properties(None)
        .await
        .map_err(|verification_error| collision_verification_error(key, verification_error))?;
    let metadata = properties
        .metadata()
        .map_err(|verification_error| collision_verification_error(key, verification_error))?;
    if metadata
        .get(CONDITIONAL_CREATE_ID_METADATA_KEY)
        .is_some_and(|stored_id| stored_id == create_id)
    {
        return Ok(());
    }

    Err(ObjectAlreadyExistsError::caused_by(key.to_owned(), error).into())
}

/// Maps a definitive upload failure using the request mode that produced it.
fn map_upload_error(error: azure_core::Error, key: &str, mode: &UploadMode) -> StorageError {
    azure_io(mode.operation(key), error)
}

/// Adds operation context to an error encountered while verifying a collision.
fn collision_verification_error(key: &str, error: azure_core::Error) -> StorageError {
    azure_io(
        format!(
            "could not verify whether conditional creation of Azure blob {key:?} had already \
             succeeded"
        ),
        error,
    )
}

/// Adds operation context to an Azure error.
fn azure_io(operation: impl Into<String>, error: azure_core::Error) -> StorageError {
    let error =
        AzureBlobOperationError::caused_by(operation.into(), AzureSdkDiagnosticError(error));
    error.into()
}

/// Builds a storage configuration error.
fn config_error(message: impl Into<String>) -> StorageError {
    StorageConfigurationError::new(message.into()).into()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::io;

    use azure_core::http::Method;
    use azure_core::http::headers::Headers;
    use futures::executor::block_on;
    use ohno::ErrorExt as _;

    use super::*;
    use crate::InvalidStorageKeyError;

    /// Builds an Azure HTTP-response error with the given status and storage
    /// error code, mirroring what the SDK surfaces for a failed request.
    fn http_error(status: StatusCode, error_code: Option<&str>) -> azure_core::Error {
        ErrorKind::HttpResponse {
            status,
            error_code: error_code.map(str::to_owned),
            raw_response: None,
        }
        .into_error()
    }

    #[test]
    fn classify_maps_container_not_found_code_to_container_missing() {
        let error = http_error(
            StatusCode::NotFound,
            Some(StorageErrorCode::ContainerNotFound.as_ref()),
        );
        assert!(matches!(classify(&error), Fault::ContainerMissing));
    }

    #[test]
    fn classify_maps_not_found_status_without_container_code_to_not_found() {
        let error = http_error(StatusCode::NotFound, None);
        assert!(matches!(classify(&error), Fault::NotFound));
    }

    #[test]
    fn classify_maps_conflict_status_to_other() {
        let error = http_error(StatusCode::Conflict, None);
        assert!(matches!(classify(&error), Fault::Other));
    }

    #[test]
    fn classify_maps_precondition_failed_status_to_other() {
        let error = http_error(StatusCode::PreconditionFailed, None);
        assert!(matches!(classify(&error), Fault::Other));
    }

    #[test]
    fn classify_maps_other_status_to_other() {
        let error = http_error(StatusCode::InternalServerError, None);
        assert!(matches!(classify(&error), Fault::Other));
    }

    #[test]
    fn classify_maps_non_http_error_to_other() {
        let error = ErrorKind::Io.into_error();
        assert!(matches!(classify(&error), Fault::Other));
    }

    #[test]
    fn storage_error_code_requires_an_exact_code() {
        let container_exists = http_error(
            StatusCode::Conflict,
            Some(StorageErrorCode::ContainerAlreadyExists.as_ref()),
        );
        assert!(has_storage_error_code(
            &container_exists,
            &StorageErrorCode::ContainerAlreadyExists
        ));
        assert!(!has_storage_error_code(
            &container_exists,
            &StorageErrorCode::BlobAlreadyExists
        ));
        let code_missing = http_error(StatusCode::Conflict, None);
        assert!(!has_storage_error_code(
            &code_missing,
            &StorageErrorCode::ContainerAlreadyExists
        ));
    }

    fn assert_azure_upload_operation(error: &StorageError, key: &str) {
        assert!(!error.is_not_found());
        assert_eq!(error.already_existing_key(), None);
        let operation = error.find_source::<AzureBlobOperationError>().unwrap();
        assert!(operation.operation.contains(key));
        assert!(error.find_source::<AzureSdkDiagnosticError>().is_some());
        assert!(error.find_source::<azure_core::Error>().is_some());
    }

    #[test]
    fn other_upload_conflicts_remain_operation_errors() {
        for (mode, status, code) in [
            (
                UploadMode::conditional_create(),
                StatusCode::Conflict,
                StorageErrorCode::LeaseAlreadyPresent,
            ),
            (
                UploadMode::Overwrite,
                StatusCode::Conflict,
                StorageErrorCode::BlobAlreadyExists,
            ),
            (
                UploadMode::InvalidationMarker,
                StatusCode::PreconditionFailed,
                StorageErrorCode::LeaseIdMissing,
            ),
        ] {
            let error = http_error(status, Some(code.as_ref()));
            let error = map_upload_error(error, "object", &mode);

            assert_azure_upload_operation(&error, "object");
        }
    }

    #[test]
    fn upload_not_found_responses_remain_operation_errors() {
        for code in [
            StorageErrorCode::BlobNotFound,
            StorageErrorCode::ContainerNotFound,
        ] {
            let error = http_error(StatusCode::NotFound, Some(code.as_ref()));
            let mode = UploadMode::conditional_create();
            let error = map_upload_error(error, "object", &mode);

            assert_azure_upload_operation(&error, "object");
        }
    }

    #[test]
    fn only_conditional_create_carries_request_identity_metadata() {
        let conditional = UploadMode::conditional_create();
        assert!(conditional.is_conditional_create());
        let create_id = conditional.conditional_create_id().unwrap();
        let metadata = conditional.metadata().unwrap();
        assert_eq!(
            metadata
                .get(CONDITIONAL_CREATE_ID_METADATA_KEY)
                .map(String::as_str),
            Some(create_id)
        );
        assert!(!UploadMode::Overwrite.is_conditional_create());
        assert!(!UploadMode::InvalidationMarker.is_conditional_create());
        assert!(UploadMode::Overwrite.metadata().is_none());
        assert!(UploadMode::InvalidationMarker.metadata().is_none());
    }

    #[test]
    fn azure_io_retains_the_operation_and_nested_sdk_causes() {
        let sdk_error = azure_core::Error::with_error(
            ErrorKind::Other,
            io::Error::other("credential canary"),
            "retry policy stopped",
        );
        let error = azure_io("could not upload Azure blob", sdk_error);

        assert!(error.find_source::<AzureBlobOperationError>().is_some());
        let diagnostic = error.find_source::<AzureSdkDiagnosticError>().unwrap();
        assert!(!diagnostic.to_string().is_empty());
        assert!(error.find_source::<azure_core::Error>().is_some());
        assert!(error.find_source::<io::Error>().is_some());
    }

    #[test]
    fn decompression_error_converts_to_storage_error() {
        let error: StorageError =
            DecompressAzureBlobError::caused_by("object".to_owned(), io::Error::other("bad gzip"))
                .into();

        assert!(error.find_source::<DecompressAzureBlobError>().is_some());
        assert!(error.find_source::<io::Error>().is_some());
    }

    /// A far-future Unix second, well beyond any token refresh margin, for fake
    /// credentials in the pure `from_parts` tests (which never actually acquire a
    /// token, but must construct one that would look fresh).
    const FAR_FUTURE_UNIX: i64 = 4_102_444_800; // 2100-01-01T00:00:00Z

    /// A fake token credential for the `from_parts` tests. It either remains
    /// unused for pre-request checks or supplies a fresh token to the fake HTTP
    /// transport.
    fn fake_credential() -> Arc<dyn TokenCredential> {
        Arc::new(CountingCredential::new(FAR_FUTURE_UNIX, false))
    }

    /// An HTTP client that must never be driven: the `from_parts` unit tests
    /// exercise URL assembly and key validation, both of which complete before any
    /// request reaches the transport.
    #[derive(Debug)]
    struct UnusedHttpClient;

    #[async_trait::async_trait]
    impl HttpClient for UnusedHttpClient {
        async fn execute_request(
            &self,
            _request: &azure_core::http::Request,
        ) -> azure_core::Result<azure_core::http::AsyncRawResponse> {
            panic!("the stub HTTP client must not be used")
        }
    }

    /// An HTTP client that answers every request with `403 Forbidden`, standing
    /// in for a non-conflict, non-retryable Azure failure (for example a rejected
    /// credential). The status is outside the SDK's retry set, so the request is
    /// issued exactly once and the test incurs no backoff delay.
    #[derive(Debug)]
    struct ForbiddenHttpClient;

    #[async_trait::async_trait]
    impl HttpClient for ForbiddenHttpClient {
        async fn execute_request(
            &self,
            _request: &azure_core::http::Request,
        ) -> azure_core::Result<azure_core::http::AsyncRawResponse> {
            Ok(azure_core::http::AsyncRawResponse::from_bytes(
                StatusCode::Forbidden,
                Headers::new(),
                azure_core::Bytes::new(),
            ))
        }
    }

    /// An HTTP client that answers every request with `404 Not Found`, standing
    /// in for a blob (or container) that does not exist. The status is outside
    /// the SDK's retry set, so the request is issued exactly once and the test
    /// incurs no backoff delay.
    #[derive(Debug)]
    struct NotFoundHttpClient;

    #[async_trait::async_trait]
    impl HttpClient for NotFoundHttpClient {
        async fn execute_request(
            &self,
            _request: &azure_core::http::Request,
        ) -> azure_core::Result<azure_core::http::AsyncRawResponse> {
            Ok(azure_core::http::AsyncRawResponse::from_bytes(
                StatusCode::NotFound,
                Headers::new(),
                azure_core::Bytes::new(),
            ))
        }
    }

    /// The result exposed by a simulated conditional-create ownership probe.
    #[derive(Clone, Copy, Debug)]
    enum CollisionProbe {
        /// The object was committed by the request whose response reports a collision.
        ThisUpload,
        /// The object was committed by another request.
        OtherUpload,
        /// The object predates request identities or was written by another tool.
        NoIdentity,
        /// Inspecting the colliding object fails.
        Failure,
    }

    /// An HTTP client that models the final response of a retried conditional
    /// create, followed by the ownership probe.
    ///
    /// The upload always returns `BlobAlreadyExists`. A subsequent properties
    /// request exposes the configured identity outcome or a probe failure, allowing
    /// the real SDK pipeline to exercise every interpretation without a transport
    /// retry delay.
    #[derive(Debug)]
    struct ConditionalCreateCollisionHttpClient {
        probe: CollisionProbe,
        create_id: futures::lock::Mutex<Option<String>>,
    }

    impl ConditionalCreateCollisionHttpClient {
        fn new(probe: CollisionProbe) -> Self {
            Self {
                probe,
                create_id: futures::lock::Mutex::new(None),
            }
        }
    }

    #[async_trait::async_trait]
    impl HttpClient for ConditionalCreateCollisionHttpClient {
        async fn execute_request(
            &self,
            request: &azure_core::http::Request,
        ) -> azure_core::Result<azure_core::http::AsyncRawResponse> {
            let metadata_header = format!("x-ms-meta-{CONDITIONAL_CREATE_ID_METADATA_KEY}");
            match request.method() {
                Method::Put => {
                    let create_id = request
                        .headers()
                        .iter()
                        .find(|(name, _)| {
                            name.as_str().eq_ignore_ascii_case(metadata_header.as_str())
                        })
                        .map(|(_, value)| value.as_str().to_owned());
                    *self.create_id.lock().await = create_id;

                    let mut headers = Headers::new();
                    headers.insert(
                        "x-ms-error-code",
                        StorageErrorCode::BlobAlreadyExists.as_ref(),
                    );
                    Ok(azure_core::http::AsyncRawResponse::from_bytes(
                        StatusCode::Conflict,
                        headers,
                        azure_core::Bytes::new(),
                    ))
                }
                Method::Head => {
                    let create_id = match self.probe {
                        CollisionProbe::ThisUpload => {
                            let Some(create_id) = self.create_id.lock().await.clone() else {
                                return Err(azure_core::Error::with_message(
                                    ErrorKind::Other,
                                    "the conditional create request carried no request identity",
                                ));
                            };
                            Some(create_id)
                        }
                        CollisionProbe::OtherUpload => Some("other-upload".to_owned()),
                        CollisionProbe::NoIdentity => None,
                        CollisionProbe::Failure => {
                            return Ok(azure_core::http::AsyncRawResponse::from_bytes(
                                StatusCode::NotFound,
                                Headers::new(),
                                azure_core::Bytes::new(),
                            ));
                        }
                    };
                    let mut headers = Headers::new();
                    if let Some(create_id) = create_id {
                        headers.insert(metadata_header, create_id);
                    }
                    Ok(azure_core::http::AsyncRawResponse::from_bytes(
                        StatusCode::Ok,
                        headers,
                        azure_core::Bytes::new(),
                    ))
                }
                method => panic!("unexpected HTTP method {method:?}"),
            }
        }
    }

    /// Builds an Entra backend from fake parts (fake credential, unused transport)
    /// for the pure unit tests that only inspect URL assembly and key validation.
    fn storage_from_fake_parts(
        account: &str,
        container: &str,
        endpoint: Option<String>,
    ) -> AzureBlobStorage {
        AzureBlobStorage::from_parts(
            account,
            container,
            endpoint,
            fake_credential(),
            Arc::new(UnusedHttpClient),
        )
        .unwrap()
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "builds a real HTTP client (reqwest) that Miri cannot run"
    )]
    fn from_config_defaults_to_the_public_https_endpoint() {
        let storage = AzureBlobStorage::from_config("prod", "history", None).unwrap();

        // Entra carries no query; the default endpoint targets the public blob host.
        assert_eq!(storage.container_endpoint.query(), None);
        assert_eq!(
            storage.container_endpoint.as_str(),
            "https://prod.blob.core.windows.net/history"
        );
    }

    #[test]
    fn blob_client_keeps_slash_separators_literal() {
        let storage = storage_from_fake_parts(
            "devstoreaccount1",
            "bench-history",
            Some("https://127.0.0.1:10000/devstoreaccount1".to_owned()),
        );

        let client = storage
            .blob_client("v1/proj/objects/callgrind/run.json")
            .unwrap();
        assert_eq!(
            client.url().path(),
            "/devstoreaccount1/bench-history/v1/proj/objects/callgrind/run.json"
        );
        // Entra carries no query, so the blob URL stays clean.
        assert_eq!(client.url().query(), None);
    }

    #[test]
    fn entra_over_http_is_a_config_error() {
        let error = AzureBlobStorage::from_config(
            "prod",
            "history",
            Some("http://insecure.example/account".to_owned()),
        )
        .unwrap_err();
        let leaf = error.find_source::<StorageConfigurationError>().unwrap();
        assert!(leaf.source().is_none());
    }

    #[test]
    fn from_parts_rejects_a_non_https_endpoint() {
        // The injection seam enforces the same TLS invariant as `from_config`, so the
        // public `azure_backend_from_parts` seam can never build an insecure backend.
        let error = AzureBlobStorage::from_parts(
            "devstoreaccount1",
            "history",
            Some("http://insecure.example/account".to_owned()),
            fake_credential(),
            Arc::new(UnusedHttpClient),
        )
        .unwrap_err();
        let leaf = error.find_source::<StorageConfigurationError>().unwrap();
        assert!(leaf.source().is_none());
    }

    #[test]
    fn azure_backend_from_parts_wraps_a_valid_endpoint_as_an_azure_override() {
        // The command-level seam wraps a ready backend for injection via
        // `Overrides::storage_override`; a valid HTTPS endpoint yields an Azure-backed
        // override without any network access.
        let injected = crate::azure_backend_from_parts(
            "devstoreaccount1",
            "history",
            Some("https://127.0.0.1:10000/devstoreaccount1".to_owned()),
            fake_credential(),
            Arc::new(UnusedHttpClient),
        )
        .unwrap();
        assert!(matches!(injected.0, crate::StorageFacade::Azure(_)));
    }

    #[test]
    fn invalid_endpoint_is_a_config_error() {
        let error = AzureBlobStorage::from_config("prod", "history", Some("not a url".to_owned()))
            .unwrap_err();
        assert!(error.find_source::<StorageConfigurationError>().is_some());
        let parse_error = Url::parse("not a url").unwrap_err();
        assert_source_type(&error, &parse_error);
    }

    fn assert_source_type<T: Error + 'static>(error: &StorageError, _: &T) {
        assert!(error.find_source::<T>().is_some());
    }

    #[test]
    fn container_endpoint_url_trailing_slash_does_not_double_up_segments() {
        let url = container_endpoint_url(
            "devstoreaccount1",
            "bench-history",
            Some("https://127.0.0.1:10000/devstoreaccount1/".to_owned()),
        )
        .unwrap();
        assert_eq!(url.path(), "/devstoreaccount1/bench-history");
    }

    #[test]
    fn put_and_get_reject_keys_that_escape_the_prefix() {
        // The Azure IO methods delegate to the SDK (and are mutation-skipped), but
        // each first runs the pure `validate_key` guard before any network call.
        // `block_on` drives that guard to completion without an emulator or a Tokio
        // runtime: the future resolves to the rejection before it awaits anything.
        let storage = storage_from_fake_parts(
            "prod",
            "history",
            Some("https://prod.blob.core.windows.net".to_owned()),
        );

        let put = block_on(storage.put("../bad", b"x")).unwrap_err();
        assert!(put.find_source::<InvalidStorageKeyError>().is_some());
        let get = block_on(storage.get("../bad")).unwrap_err();
        assert!(get.find_source::<InvalidStorageKeyError>().is_some());
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "drives the Azure SDK request pipeline, which Miri cannot run"
    )]
    async fn conditional_create_accepts_a_collision_from_its_own_committed_upload() {
        let storage = AzureBlobStorage::from_parts(
            "acct",
            "history",
            None,
            fake_credential(),
            Arc::new(ConditionalCreateCollisionHttpClient::new(
                CollisionProbe::ThisUpload,
            )),
        )
        .unwrap();

        storage.put("v1/proj/object.json", b"body").await.unwrap();
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "drives the Azure SDK request pipeline, which Miri cannot run"
    )]
    async fn conditional_create_rejects_collisions_not_owned_by_this_upload() {
        let key = "v1/proj/object.json";
        for probe in [CollisionProbe::OtherUpload, CollisionProbe::NoIdentity] {
            let storage = AzureBlobStorage::from_parts(
                "acct",
                "history",
                None,
                fake_credential(),
                Arc::new(ConditionalCreateCollisionHttpClient::new(probe)),
            )
            .unwrap();

            let error = storage.put(key, b"body").await.unwrap_err();

            assert_eq!(error.already_existing_key(), Some(key));
            let leaf = error.find_source::<ObjectAlreadyExistsError>().unwrap();
            assert_eq!(leaf.key, key);
            let source = error.find_source::<azure_core::Error>().unwrap();
            assert_eq!(source.http_status(), Some(StatusCode::Conflict));
        }
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "drives the Azure SDK request pipeline, which Miri cannot run"
    )]
    async fn conditional_create_probe_failure_remains_an_operation_error() {
        let key = "v1/proj/object.json";
        let storage = AzureBlobStorage::from_parts(
            "acct",
            "history",
            None,
            fake_credential(),
            Arc::new(ConditionalCreateCollisionHttpClient::new(
                CollisionProbe::Failure,
            )),
        )
        .unwrap();

        let error = storage.put(key, b"body").await.unwrap_err();

        assert_azure_upload_operation(&error, key);
        assert!(error.find_source::<ObjectAlreadyExistsError>().is_none());
        let operation = error.find_source::<AzureBlobOperationError>().unwrap();
        assert!(operation.operation.contains("verify"));
        let source = error.find_source::<azure_core::Error>().unwrap();
        assert_eq!(source.http_status(), Some(StatusCode::NotFound));
    }

    // =======================================================================
    // Caching-credential tests.
    //
    // These cover the token caching/serialization decorator with a fake inner
    // credential, so they exercise the dedup, freshness, and per-scope behaviour
    // without a real Entra credential or any wall-clock read (the clock is
    // injected). They stay Miri-safe: no IO, no real time.
    //
    // `AtomicU64` and `Ordering` are imported by the Azurite section below (one
    // `tests` module, so module-scoped). `Future` is in the 2024 prelude.
    // =======================================================================

    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures::future::join_all;

    /// The Unix-second anchor shared by [`fixed_now`] and [`at_offset`].
    const BASE_UNIX: i64 = 1_000_000_000;

    /// A fixed "now" used by the caching-credential tests so token freshness is
    /// deterministic and Miri-safe (no wall-clock read).
    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(BASE_UNIX).unwrap()
    }

    /// Builds an [`OffsetDateTime`] `offset` seconds from [`fixed_now`].
    fn at_offset(offset: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(BASE_UNIX.saturating_add(offset)).unwrap()
    }

    /// Yields control back to the executor exactly once, so a concurrently-polled
    /// burst can interleave inside the fake credential's acquisition (and thus be
    /// observed as concurrent if it were ever allowed to run unserialized).
    struct YieldOnce(bool);

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    /// A fake [`TokenCredential`] that records how many times — and how
    /// concurrently — it is asked to acquire a token, optionally yielding once
    /// mid-acquisition so a burst can be observed as concurrent if it is not
    /// serialized by the decorator.
    #[derive(Debug)]
    struct CountingCredential {
        calls: AtomicU64,
        in_flight: AtomicU64,
        max_in_flight: AtomicU64,
        yield_during_acquire: bool,
        expires_at_unix: i64,
    }

    impl CountingCredential {
        fn new(expires_at_unix: i64, yield_during_acquire: bool) -> Self {
            Self {
                calls: AtomicU64::new(0),
                in_flight: AtomicU64::new(0),
                max_in_flight: AtomicU64::new(0),
                yield_during_acquire,
                expires_at_unix,
            }
        }

        fn calls(&self) -> u64 {
            self.calls.load(Ordering::Relaxed)
        }

        fn max_in_flight(&self) -> u64 {
            self.max_in_flight.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl TokenCredential for CountingCredential {
        async fn get_token(
            &self,
            _scopes: &[&str],
            _options: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let now = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
            self.max_in_flight.fetch_max(now, Ordering::Relaxed);
            if self.yield_during_acquire {
                YieldOnce(false).await;
            }
            self.in_flight.fetch_sub(1, Ordering::Relaxed);
            Ok(AccessToken::new(
                "fake-token",
                OffsetDateTime::from_unix_timestamp(self.expires_at_unix).unwrap(),
            ))
        }
    }

    #[test]
    fn token_is_fresh_accepts_a_token_beyond_the_refresh_margin() {
        let margin = TOKEN_REFRESH_MARGIN.whole_seconds();
        let token = AccessToken::new("t", at_offset(margin + 1));
        assert!(token_is_fresh(&token, fixed_now()));
    }

    #[test]
    fn token_is_fresh_rejects_a_token_within_the_refresh_margin() {
        let margin = TOKEN_REFRESH_MARGIN.whole_seconds();
        let token = AccessToken::new("t", at_offset(margin - 1));
        assert!(!token_is_fresh(&token, fixed_now()));
    }

    #[test]
    fn token_is_fresh_rejects_a_token_exactly_at_the_refresh_margin() {
        let margin = TOKEN_REFRESH_MARGIN.whole_seconds();
        let token = AccessToken::new("t", at_offset(margin));
        assert!(!token_is_fresh(&token, fixed_now()));
    }

    #[test]
    fn token_is_fresh_rejects_an_expired_token() {
        let token = AccessToken::new("t", at_offset(-1));
        assert!(!token_is_fresh(&token, fixed_now()));
    }

    #[test]
    fn counting_credential_observes_concurrency_when_unserialized() {
        // Establishes the discriminating power of the burst test: hit the fake
        // directly (no decorator) and the yield lets the burst overlap, so the
        // observed max concurrency exceeds one.
        let inner = Arc::new(CountingCredential::new(
            at_offset(3600).unix_timestamp(),
            true,
        ));
        let scopes = ["scope"];
        let calls = std::iter::repeat_with(|| inner.get_token(&scopes, None)).take(8);
        for result in block_on(join_all(calls)) {
            result.unwrap();
        }
        assert_eq!(inner.calls(), 8);
        assert!(inner.max_in_flight() > 1, "{}", inner.max_in_flight());
    }

    #[test]
    fn caching_credential_collapses_a_concurrent_burst_into_one_acquisition() {
        let inner = Arc::new(CountingCredential::new(
            at_offset(3600).unix_timestamp(),
            true,
        ));
        let cred =
            CachingCredential::with_clock(Arc::<CountingCredential>::clone(&inner), fixed_now);
        let scopes = ["scope"];
        let calls = std::iter::repeat_with(|| cred.get_token(&scopes, None)).take(8);
        for result in block_on(join_all(calls)) {
            result.unwrap();
        }
        // Serialized through the cache lock: a single acquisition, never overlapping.
        assert_eq!(inner.calls(), 1);
        assert_eq!(inner.max_in_flight(), 1);
    }

    #[test]
    fn caching_credential_reuses_a_fresh_token() {
        let inner = Arc::new(CountingCredential::new(
            at_offset(3600).unix_timestamp(),
            false,
        ));
        let cred =
            CachingCredential::with_clock(Arc::<CountingCredential>::clone(&inner), fixed_now);
        let scopes = ["scope"];
        block_on(cred.get_token(&scopes, None)).unwrap();
        block_on(cred.get_token(&scopes, None)).unwrap();
        assert_eq!(inner.calls(), 1);
    }

    #[test]
    fn caching_credential_reacquires_an_expired_token() {
        // The fake hands back an already-stale token, so every call must refresh.
        let inner = Arc::new(CountingCredential::new(
            at_offset(-10).unix_timestamp(),
            false,
        ));
        let cred =
            CachingCredential::with_clock(Arc::<CountingCredential>::clone(&inner), fixed_now);
        let scopes = ["scope"];
        block_on(cred.get_token(&scopes, None)).unwrap();
        block_on(cred.get_token(&scopes, None)).unwrap();
        assert_eq!(inner.calls(), 2);
    }

    #[test]
    fn caching_credential_caches_each_scope_independently() {
        let inner = Arc::new(CountingCredential::new(
            at_offset(3600).unix_timestamp(),
            false,
        ));
        let cred =
            CachingCredential::with_clock(Arc::<CountingCredential>::clone(&inner), fixed_now);
        block_on(cred.get_token(&["scope-a"], None)).unwrap();
        block_on(cred.get_token(&["scope-b"], None)).unwrap();
        // Each new scope acquires once; re-requesting a cached scope does not.
        assert_eq!(inner.calls(), 2);
        block_on(cred.get_token(&["scope-a"], None)).unwrap();
        assert_eq!(inner.calls(), 2);
    }

    #[test]
    fn caching_credential_debug_hides_the_cache_and_clock() {
        let inner: Arc<dyn TokenCredential> = Arc::new(CountingCredential::new(
            at_offset(3600).unix_timestamp(),
            false,
        ));
        let cred = CachingCredential::with_clock(inner, fixed_now);
        let rendered = format!("{cred:?}");
        assert!(rendered.contains("CachingCredential"), "{rendered}");
    }

    // =======================================================================
    // Network tests against a live Azurite emulator.
    //
    // These exercise the real put/get/list paths and the error classification
    // that pure tests cannot reach. They require an Azurite blob endpoint (the
    // CI `test-azurite` job provides one; see the package AGENTS.md for running
    // them locally) and so are network-bound: ignored under Miri and serialized.
    // Each test uses a freshly named container, so they never share state even
    // against a shared emulator.
    // =======================================================================

    use std::net::{TcpStream, ToSocketAddrs as _};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration as StdDuration;

    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use jiff::Timestamp;
    use serial_test::serial;

    /// The Azurite blob endpoint, overridable for a non-default emulator.
    ///
    /// The default is HTTPS: Entra authentication (the only supported mode)
    /// requires TLS, so the emulator runs behind a self-signed certificate that
    /// [`azurite_http_client`] is configured to trust.
    fn azurite_endpoint() -> String {
        env::var("AZURITE_BLOB_ENDPOINT")
            .unwrap_or_else(|_| "https://127.0.0.1:10000/devstoreaccount1".to_owned())
    }

    /// A fresh, valid container name (lowercase, 3-63 chars) unique to one test.
    fn unique_container() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = Timestamp::now().as_nanosecond();
        format!("bh-test-{nanos}-{n}")
    }

    /// Whether an Azurite blob endpoint is reachable via a short TCP connect.
    ///
    /// These tests are always compiled, but the runner usually has no emulator. A
    /// reachability probe lets the test self-skip there while still running for
    /// real wherever Azurite is provided.
    fn azurite_reachable() -> bool {
        let endpoint = azurite_endpoint();
        let Ok(url) = Url::parse(&endpoint) else {
            return false;
        };
        let host = url.host_str().unwrap_or("127.0.0.1").to_owned();
        let port = url.port().unwrap_or(10000);
        let Ok(addrs) = (host.as_str(), port).to_socket_addrs() else {
            return false;
        };
        addrs
            .into_iter()
            .any(|addr| TcpStream::connect_timeout(&addr, StdDuration::from_secs(2)).is_ok())
    }

    /// A fake Entra token credential whose JWT is accepted by Azurite's
    /// `--oauth basic` mode.
    ///
    /// That mode validates a token's structure and time claims (`iss` prefix,
    /// `aud`, `nbf`/`iat`/`exp`) but never verifies the signature, so a locally
    /// crafted token stands in for a real Entra token. Real signature validation
    /// stays covered by the `test-azure` / `test-azure-gh` jobs against a real
    /// Entra-only account.
    #[derive(Debug)]
    struct FakeEntraCredential;

    #[async_trait::async_trait]
    impl TokenCredential for FakeEntraCredential {
        async fn get_token(
            &self,
            _scopes: &[&str],
            _options: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            let now = OffsetDateTime::now_utc();
            let expires = now
                .checked_add(Duration::hours(1))
                .expect("one hour past the current time is representable");
            Ok(AccessToken::new(fake_entra_jwt(now, expires), expires))
        }
    }

    /// Crafts an unsigned JWT that satisfies Azurite's `--oauth basic` structural
    /// checks: a valid `sts.windows.net` issuer, the storage audience, and
    /// `iat`/`nbf` in the past with `exp` in the future.
    fn fake_entra_jwt(now: OffsetDateTime, expires: OffsetDateTime) -> String {
        let encode = |bytes: &[u8]| URL_SAFE_NO_PAD.encode(bytes);
        let header = encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        // Backdate `iat`/`nbf` slightly so minor clock skew never rejects the token.
        let issued = now
            .checked_sub(Duration::seconds(60))
            .expect("60 seconds before the current time is representable")
            .unix_timestamp();
        let expiry = expires.unix_timestamp();
        let payload = encode(
            format!(
                concat!(
                    "{{\"iss\":\"https://sts.windows.net/",
                    "00000000-0000-0000-0000-000000000000/\",",
                    "\"aud\":\"https://storage.azure.com\",",
                    "\"iat\":{issued},\"nbf\":{issued},\"exp\":{expiry}}}"
                ),
                issued = issued,
                expiry = expiry,
            )
            .as_bytes(),
        );
        // The signature is never checked; any base64url segment satisfies the shape.
        let signature = encode(b"signature");
        format!("{header}.{payload}.{signature}")
    }

    /// An HTTP client that trusts Azurite's self-signed certificate.
    ///
    /// The production transport (`new_http_client`) validates certificates against
    /// the platform trust store and so rejects the emulator's throwaway cert; this
    /// test client disables that check. Automatic gzip decompression is turned off
    /// to match the production client: the backend stores gzip and inflates it
    /// itself in `get`, so the transport must hand back the raw compressed bytes.
    fn azurite_http_client() -> Arc<dyn HttpClient> {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .no_gzip()
            .build()
            .expect("building the Azurite HTTP client");
        Arc::new(client)
    }

    /// An Entra backend for a fresh container, wired to Azurite via a fake token
    /// and a cert-trusting transport; or `None` to skip when no emulator is
    /// reachable.
    ///
    /// Setting `BENCH_HISTORY_REQUIRE_AZURITE` turns an unreachable emulator into
    /// a hard failure, so the dedicated CI job that provisions Azurite cannot
    /// silently degrade into skipping every network test.
    fn azurite_storage_or_skip() -> Option<AzureBlobStorage> {
        if !azurite_reachable() {
            assert!(
                env::var_os("BENCH_HISTORY_REQUIRE_AZURITE").is_none(),
                "BENCH_HISTORY_REQUIRE_AZURITE is set but no Azurite emulator is reachable at {}",
                azurite_endpoint()
            );
            eprintln!(
                "skipping Azurite network test: no emulator reachable at {}",
                azurite_endpoint()
            );
            return None;
        }
        let storage = AzureBlobStorage::from_parts(
            "devstoreaccount1",
            &unique_container(),
            Some(azurite_endpoint()),
            Arc::new(FakeEntraCredential),
            azurite_http_client(),
        )
        .unwrap();
        Some(storage)
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn put_creates_the_container_then_get_and_list_round_trip() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };

        // The very first put hits a container that does not exist yet, so this
        // also covers the create-on-demand retry path.
        storage.put("v1/proj/a/1.json", b"first").await.unwrap();
        storage.put("v1/proj/a/2.json", b"second").await.unwrap();
        storage.put("v1/proj/b/3.json", b"third").await.unwrap();

        assert_eq!(storage.get("v1/proj/a/1.json").await.unwrap(), b"first");

        let listed = storage.list("v1/proj/a/").await.unwrap();
        assert_eq!(
            listed,
            vec!["v1/proj/a/1.json".to_owned(), "v1/proj/a/2.json".to_owned()]
        );

        let all = storage.list("v1/proj/").await.unwrap();
        assert_eq!(all.len(), 3, "{all:?}");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn get_missing_blob_in_existing_container_reports_not_found() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };
        // Create the container (and an unrelated blob) so the missing-blob case
        // is a blob-level 404, not a missing container.
        storage.put("v1/present.json", b"x").await.unwrap();

        let error = storage.get("v1/absent.json").await.unwrap_err();
        assert!(error.is_not_found());
        assert!(error.find_source::<ObjectNotFoundError>().is_some());
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn get_in_missing_container_reports_not_found() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };
        // No put, so the container does not exist: a get must still resolve to a
        // plain not-found rather than an I/O error.
        let error = storage.get("v1/absent.json").await.unwrap_err();
        assert!(error.is_not_found());
        assert!(error.find_source::<ObjectNotFoundError>().is_some());
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn delete_removes_a_blob_and_leaves_siblings() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };
        storage.put("v1/proj/clean.json", b"c").await.unwrap();
        storage.put("v1/proj/dirty.json", b"d").await.unwrap();

        storage.delete("v1/proj/dirty.json").await.unwrap();

        // The sibling survives and the deleted blob reports not-found.
        assert_eq!(
            storage.list("v1/proj/").await.unwrap(),
            vec!["v1/proj/clean.json".to_owned()]
        );
        let error = storage.get("v1/proj/dirty.json").await.unwrap_err();
        assert!(error.is_not_found());
        assert!(error.find_source::<ObjectNotFoundError>().is_some());
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn delete_missing_blob_reports_not_found() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };
        // Create the container (and an unrelated blob) so the missing-blob case
        // is a blob-level 404, not a missing container.
        storage.put("v1/present.json", b"x").await.unwrap();

        let error = storage.delete("v1/absent.json").await.unwrap_err();
        assert!(error.is_not_found());
        assert!(error.find_source::<ObjectNotFoundError>().is_some());
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn delete_in_missing_container_reports_not_found() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };
        // No put, so the container does not exist: a delete must still resolve to
        // a plain not-found rather than an I/O error.
        let error = storage.delete("v1/absent.json").await.unwrap_err();
        assert!(error.is_not_found());
        assert!(error.find_source::<ObjectNotFoundError>().is_some());
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn put_is_write_once() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };
        storage.put("v1/once.json", b"original").await.unwrap();

        let error = storage
            .put("v1/once.json", b"replacement")
            .await
            .unwrap_err();
        assert_eq!(error.already_existing_key(), Some("v1/once.json"));
        assert!(error.find_source::<ObjectAlreadyExistsError>().is_some());
        // The original value is preserved.
        assert_eq!(storage.get("v1/once.json").await.unwrap(), b"original");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn put_overwrite_replaces_an_existing_blob() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };
        // The first write creates the container on demand; the overwrite replaces
        // the blob's contents in place rather than failing as `put` would.
        storage.put("v1/clobber.json", b"original").await.unwrap();
        storage
            .put_overwrite("v1/clobber.json", b"replacement")
            .await
            .unwrap();

        assert_eq!(
            storage.get("v1/clobber.json").await.unwrap(),
            b"replacement"
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn put_overwrite_creates_when_absent() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };
        // No prior blob and no container: the overwrite must still create both.
        storage
            .put_overwrite("v1/fresh.json", b"only")
            .await
            .unwrap();

        assert_eq!(storage.get("v1/fresh.json").await.unwrap(), b"only");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn put_overwrite_creating_a_new_key_does_not_arm_invalidation() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };
        // Adding a brand-new key leaves per-key immutability intact and no cache
        // mirrors it, so it must not arm the invalidation flag.
        storage
            .put_overwrite("v1/added.json", b"body")
            .await
            .unwrap();
        assert!(
            !storage.invalidation.take(),
            "creating a new key must not arm invalidation"
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn put_overwrite_replacing_an_existing_key_arms_invalidation() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };
        // A write-once `put` seeds the object without arming (the additive path).
        storage.put("v1/replaced.json", b"original").await.unwrap();
        assert!(
            !storage.invalidation.take(),
            "a write-once put must not arm invalidation"
        );
        // Overwriting the now-existing object breaks immutability, so any mirror of
        // it is stale: this path must arm the flag.
        storage
            .put_overwrite("v1/replaced.json", b"replacement")
            .await
            .unwrap();
        assert!(
            storage.invalidation.take(),
            "replacing an existing key must arm invalidation"
        );
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "drives the Azure SDK request pipeline, which Miri cannot run"
    )]
    async fn put_overwrite_forwards_a_non_conflict_upload_error() {
        // The write-once probe fails with a non-conflict, non-retryable status
        // (403 Forbidden here), which is neither `AlreadyExists` nor a missing
        // container: `put_overwrite` must forward it verbatim rather than fall
        // through to the replace path.
        let storage = AzureBlobStorage::from_parts(
            "acct",
            "history",
            None,
            fake_credential(),
            Arc::new(ForbiddenHttpClient),
        )
        .unwrap();

        let error = storage
            .put_overwrite("v1/proj/object.json", b"body")
            .await
            .expect_err("a forbidden upload must surface as an error");

        assert!(error.find_source::<AzureBlobOperationError>().is_some());
        assert!(error.find_source::<azure_core::Error>().is_some());
        assert!(
            !storage.invalidation.take(),
            "a failed write-once probe must not arm invalidation"
        );
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "drives the Azure SDK request pipeline, which Miri cannot run"
    )]
    async fn operations_forward_non_not_found_responses_as_io_errors() {
        let storage = AzureBlobStorage::from_parts(
            "acct",
            "history",
            None,
            fake_credential(),
            Arc::new(ForbiddenHttpClient),
        )
        .unwrap();

        let create = storage.ensure_container().await.unwrap_err();
        let get = storage.get("v1/proj/object.json").await.unwrap_err();
        let list = storage.list("v1/proj").await.unwrap_err();
        let delete = storage.delete("v1/proj/object.json").await.unwrap_err();

        for error in [create, get, list, delete] {
            assert!(error.find_source::<AzureBlobOperationError>().is_some());
            let source = error.find_source::<azure_core::Error>().unwrap();
            assert_eq!(source.http_status(), Some(StatusCode::Forbidden));
        }
        assert!(!storage.invalidation.take());
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "drives the Azure SDK request pipeline, which Miri cannot run"
    )]
    async fn delete_maps_a_not_found_response_to_not_found() {
        // A 404 on the delete request means the object is simply not there, which
        // the storage port reports as `NotFound` rather than as an I/O failure so
        // callers can treat it as an already-absent object.
        let storage = AzureBlobStorage::from_parts(
            "acct",
            "history",
            None,
            fake_credential(),
            Arc::new(NotFoundHttpClient),
        )
        .unwrap();

        let error = storage.delete("v1/proj/object.json").await.unwrap_err();

        assert!(error.is_not_found());
        assert!(error.find_source::<ObjectNotFoundError>().is_some());
        assert!(!storage.invalidation.take());
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn list_on_missing_container_is_empty() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };
        // A container that was never created lists as empty, mirroring a missing
        // local storage root.
        assert!(storage.list("v1/").await.unwrap().is_empty());
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    #[cfg_attr(
        mutants,
        ignore = "Azurite network test: self-skips without an emulator (as under mutation), and the IO it exercises is already mutants::skip"
    )]
    #[serial]
    async fn list_with_a_non_matching_prefix_is_empty() {
        let Some(storage) = azurite_storage_or_skip() else {
            return;
        };
        storage.put("v1/proj/a.json", b"x").await.unwrap();
        assert!(storage.list("v1/other/").await.unwrap().is_empty());
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "builds a real HTTP pipeline (reqwest) that Miri cannot run"
    )]
    fn entra_credential_uses_the_self_minting_oidc_path_when_all_vars_present() {
        let http_client = new_http_client(None);
        // With every GitHub OIDC federation variable present the self-minting branch
        // builds and returns the assertion credential without falling through to the
        // developer credential. Construction performs no network I/O; the tenant and
        // client IDs must be legal GUIDs so the assertion credential constructs. The
        // names are GitHub's and Azure's fixed federation-variable contract.
        let get = |key: &str| match key {
            "ACTIONS_ID_TOKEN_REQUEST_URL" => Some("https://example.test/token".to_owned()),
            "ACTIONS_ID_TOKEN_REQUEST_TOKEN" => Some("request-token".to_owned()),
            "AZURE_CLIENT_ID" => Some("11111111-1111-1111-1111-111111111111".to_owned()),
            "AZURE_TENANT_ID" => Some("22222222-2222-2222-2222-222222222222".to_owned()),
            _ => None,
        };
        entra_credential_from(get, &http_client).expect("self-minting OIDC credential builds");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "constructs the developer credential chain (reqwest) that Miri cannot run"
    )]
    fn entra_credential_falls_back_to_the_developer_credential_without_oidc_vars() {
        let http_client = new_http_client(None);
        // Absent the federation variables the self-minting branch is skipped and
        // resolution falls back to the ambient developer/CLI credential. Constructing
        // it assembles the discovery chain without authenticating, so the fallback
        // wiring is exercised whether or not an Azure session exists on the host.
        entra_credential_from(|_| None, &http_client)
            .expect("developer credential chain constructs");
    }
}
