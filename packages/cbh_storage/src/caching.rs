//! [`CachingStorage`]: a read-through cache decorator over a [`Storage`].
//!
//! The cloud history is immutable per key, so once an object has been fetched it
//! never needs fetching again — only *new* objects do. This decorator mirrors every
//! fetched object into a local backing store (a directory under `actions/cache` in
//! CI), so a read command (`analyze`/`list`/`prune`) downloads each object at most
//! once across all runs instead of re-downloading the entire history every time.
//!
//! It is generic over both the wrapped backend (`Inner`, the cloud
//! [`AzureBlobStorage`](super::azure::AzureBlobStorage) in production) and the
//! on-disk mirror (`Cache`, [`LocalStorage`](super::local::LocalStorage) in
//! production), so its logic is unit-tested with an in-memory fake for *both* —
//! reactor-free and Miri-safe — while production composes the two real backends.
//!
//! Correctness rests on two rules:
//!
//! * **Reads are read-through, writes and listings are pass-through.** `get`
//!   consults the mirror first and populates it on a miss; `list` always goes to
//!   the cloud, so an object stored after the mirror was populated is still
//!   discovered (the mirror can lag the cloud but never hides newer keys).
//! * **A cloud-side marker drives per-project invalidation.** The mirror is a
//!   faithful image of the cloud namespace under identical keys, so it may hold
//!   several projects' objects at once. Deletes and overwrites (rare, deliberate)
//!   bump the mutated project's marker
//!   ([`cache_epoch_key`](super::cache_epoch_key)); on the next
//!   [`synchronize`](CachingStorage::synchronize) a changed marker wipes only
//!   *that* project's objects subtree
//!   ([`project_objects_prefix`](super::project_objects_prefix)) and re-records the
//!   new epoch under the same marker key, so a stale object can never be served and
//!   other projects' cached objects are untouched.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cbh_diag::{Reporter, ReporterExt, count_noun};

use crate::{Storage, StorageError, cache_epoch_key, project_objects_prefix};

/// The epoch a project that has never recorded a mutation reports.
///
/// A missing cloud marker means "no delete or overwrite has ever happened", which
/// must compare *equal* across runs so an untouched project's cache is reused. A
/// fixed sentinel gives that stable identity (an absent marker is not an error).
const GENESIS_EPOCH: &[u8] = b"genesis";

/// A read-through cache over an inner [`Storage`], mirroring fetched objects into a
/// local backing store under their exact cloud keys.
///
/// A project's mirrored objects are invalidated when that project's cloud-side marker
/// changes. The mirror
/// faithfully images the cloud namespace, so it can hold several projects at once;
/// invalidation is scoped to one project's objects subtree at a time. See the
/// [module docs](self) for the invalidation contract.
///
/// `Clone` shares all interior state (the mirror via the backends' own `Arc`
/// fields, and the tally counters here), so the parallel-load fan-out — which
/// clones the storage into each spawned task — accumulates one mirror and one
/// hit/miss tally across every worker.
#[derive(Clone, Debug)]
pub struct CachingStorage<Inner, Cache> {
    /// The wrapped backend (the cloud history in production). Writes, listings, and
    /// cache misses go here; it also owns the cloud-side invalidation marker.
    inner: Inner,
    /// The local mirror that absorbs fetched objects so each is downloaded at most
    /// once across runs.
    cache: Cache,
    /// Objects served from the mirror this process, for the end-of-load tally.
    /// Shared across clones so parallel-load tasks aggregate into one count.
    hits: Arc<AtomicUsize>,
    /// Objects fetched from the cloud (and mirrored) this process, for the tally.
    /// Shared across clones so parallel-load tasks aggregate into one count.
    misses: Arc<AtomicUsize>,
}

impl<Inner, Cache> CachingStorage<Inner, Cache>
where
    Inner: Storage,
    Cache: Storage,
{
    /// Wraps `inner`, mirroring fetched objects into `cache`.
    pub(crate) fn new(inner: Inner, cache: Cache) -> Self {
        Self {
            inner,
            cache,
            hits: Arc::new(AtomicUsize::new(0)),
            misses: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The wrapped backend, so a caller holding the concrete decorator can reach
    /// the cloud backend directly (e.g. to flush its invalidation marker).
    pub(crate) fn inner(&self) -> &Inner {
        &self.inner
    }

    /// Reconciles the mirror with the cloud before a load: reads `project`'s
    /// invalidation marker, and if it differs from the epoch the mirror recorded
    /// under the same marker key (or the mirror has none), wipes this project's
    /// mirrored objects and records the freshly read epoch. A matching epoch leaves
    /// the mirror untouched.
    ///
    /// Per-project invalidation is intentionally coarse: deletes and overwrites are
    /// rare, so "throw this project's objects away and re-download" is simpler and
    /// obviously correct, and re-recording the epoch immediately bounds the wipe to
    /// once per process. Only this project's objects subtree is touched, so any
    /// other project the mirror also holds keeps its cached objects.
    ///
    /// # Errors
    ///
    /// Returns any [`StorageError`] from reading the marker, listing or deleting the
    /// mirrored objects, or recording the new epoch.
    pub(crate) async fn synchronize(
        &self,
        project: &str,
        reporter: &dyn Reporter,
    ) -> Result<(), StorageError> {
        let marker_key = cache_epoch_key(project);
        // A missing marker means no mutation was ever recorded: treat it as a fixed
        // genesis epoch so an untouched project reuses its mirror run after run.
        let cloud_epoch = match self.inner.get(&marker_key).await {
            Ok(bytes) => bytes,
            Err(error) if error.is_not_found() => GENESIS_EPOCH.to_vec(),
            Err(other) => return Err(other),
        };
        // The mirror records the epoch it last synced under the *same* key the cloud
        // holds it at, so freshness is a direct local-vs-cloud comparison of one
        // marker object rather than a separate bookkeeping slot.
        let recorded_epoch = match self.cache.get(&marker_key).await {
            Ok(bytes) => Some(bytes),
            Err(error) if error.is_not_found() => None,
            Err(other) => return Err(other),
        };

        let reuse = recorded_epoch.as_deref() == Some(cloud_epoch.as_slice());
        reporter.if_enabled(|notes| {
            notes.note(&format!(
                "cache: cloud invalidation marker {marker_key} reads epoch {}",
                String::from_utf8_lossy(&cloud_epoch)
            ));
            match &recorded_epoch {
                Some(recorded) => notes.note(&format!(
                    "cache: local mirror last synchronized at epoch {}",
                    String::from_utf8_lossy(recorded)
                )),
                None => notes
                    .note("cache: local mirror has no recorded epoch, so it is treated as cold"),
            }
            if reuse {
                notes.note(
                    "cache: epochs match, so the mirror is reused without re-downloading the \
                     history",
                );
            } else {
                notes.note(
                    "cache: epochs differ, so this project's mirrored objects are wiped and the \
                     cloud epoch re-recorded before reloading",
                );
            }
        });

        if reuse {
            return Ok(());
        }

        self.wipe_objects(project).await?;
        self.cache.put_overwrite(&marker_key, &cloud_epoch).await?;
        Ok(())
    }

    /// Notes the hit/miss tally accumulated by [`get`](Self::get) this process, so a
    /// slow load can be diagnosed as a cold or invalidated mirror (many misses)
    /// rather than a cloud problem. A no-op when the reporter is disabled.
    pub(crate) fn report_tally(&self, reporter: &dyn Reporter) {
        reporter.note_with(|| {
            let hits = self.hits.load(Ordering::Relaxed);
            let misses = self.misses.load(Ordering::Relaxed);
            format!(
                "cache: served {} from the local mirror and fetched {} from the cloud this load",
                count_noun(hits, "object"),
                count_noun(misses, "object"),
            )
        });
    }

    /// Deletes `project`'s mirrored **data** objects (everything under its
    /// [`project_objects_prefix`]). The project's marker is left in place and
    /// re-recorded by the caller; any other project's objects are outside this
    /// prefix and so are untouched.
    async fn wipe_objects(&self, project: &str) -> Result<(), StorageError> {
        let prefix = project_objects_prefix(project);
        for key in self.cache.list(&prefix).await? {
            self.cache.delete(&key).await?;
        }
        Ok(())
    }
}

impl<Inner, Cache> Storage for CachingStorage<Inner, Cache>
where
    Inner: Storage,
    Cache: Storage,
{
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
        // Writes target the authoritative backend; the mirror is populated lazily
        // by reads, so a freshly written object is fetched (and mirrored) on demand.
        self.inner.put(key, bytes).await
    }

    async fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
        self.inner.put_overwrite(key, bytes).await
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        match self.cache.get(key).await {
            Ok(bytes) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Ok(bytes)
            }
            Err(error) if error.is_not_found() => {
                let bytes = self.inner.get(key).await?;
                // Mirror the object so a later run (or a re-fetch this run) is served
                // locally. `put_overwrite` keeps the populate idempotent even if the
                // same key is fetched twice.
                self.cache.put_overwrite(key, &bytes).await?;
                self.misses.fetch_add(1, Ordering::Relaxed);
                Ok(bytes)
            }
            Err(other) => Err(other),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        // List the cloud, not the mirror: an object stored after the mirror was last
        // populated must still be discovered, so the mirror may lag the cloud but
        // never hides newer keys.
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.inner.delete(key).await
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::future::{Future, ready};

    use cbh_diag::RecordingReporter;
    use futures::executor::block_on;
    use ohno::ErrorExt as _;

    use super::*;
    use crate::{MemoryStorage, ObjectNotFoundError, TestStorageError};

    /// Builds a decorator over two in-memory fakes (cloud + mirror) so the
    /// read-through logic is exercised reactor-free and under Miri.
    fn caching() -> CachingStorage<MemoryStorage, MemoryStorage> {
        CachingStorage::new(MemoryStorage::new(), MemoryStorage::new())
    }

    #[test]
    fn get_populates_the_mirror_on_a_miss() {
        let storage = caching();
        block_on(
            storage
                .inner()
                .put("v1/p/objects/criterion/abc.json", b"body"),
        )
        .unwrap();

        // First read is a miss served from the cloud and mirrored.
        let bytes = block_on(storage.get("v1/p/objects/criterion/abc.json")).unwrap();
        assert_eq!(bytes, b"body");
        assert_eq!(
            storage.cache.keys(),
            vec!["v1/p/objects/criterion/abc.json".to_owned()],
            "the fetched object is mirrored"
        );
    }

    #[test]
    fn get_serves_a_hit_from_the_mirror_even_when_the_cloud_diverges() {
        let storage = caching();
        block_on(
            storage
                .inner()
                .put("v1/p/objects/criterion/abc.json", b"body"),
        )
        .unwrap();
        // Populate the mirror.
        block_on(storage.get("v1/p/objects/criterion/abc.json")).unwrap();

        // Overwrite the cloud copy out from under the mirror; a hit must still serve
        // the mirrored bytes (invalidation is the marker's job, not get's).
        block_on(
            storage
                .inner()
                .put_overwrite("v1/p/objects/criterion/abc.json", b"newer"),
        )
        .unwrap();
        let bytes = block_on(storage.get("v1/p/objects/criterion/abc.json")).unwrap();
        assert_eq!(bytes, b"body", "a cache hit serves the mirrored bytes");
    }

    #[test]
    fn list_reflects_objects_added_to_the_cloud_after_the_mirror_was_populated() {
        let storage = caching();
        block_on(storage.inner().put("v1/p/objects/criterion/a.json", b"a")).unwrap();
        block_on(storage.get("v1/p/objects/criterion/a.json")).unwrap();

        // A key added to the cloud after the mirror was populated must still be
        // listed, so the load discovers the run's new objects.
        block_on(storage.inner().put("v1/p/objects/criterion/b.json", b"b")).unwrap();
        let keys = block_on(storage.list("v1/p/objects/criterion/")).unwrap();
        assert_eq!(
            keys,
            vec![
                "v1/p/objects/criterion/a.json".to_owned(),
                "v1/p/objects/criterion/b.json".to_owned()
            ]
        );
    }

    #[test]
    fn synchronize_reuses_the_mirror_when_the_epoch_matches() {
        let storage = caching();
        let reporter = RecordingReporter::new();

        // Production order: synchronize records the genesis epoch (no cloud marker)
        // against an empty mirror *before* any object is fetched.
        block_on(storage.synchronize("p", &reporter)).unwrap();
        // The load then populates the mirror.
        block_on(storage.inner().put("v1/p/objects/criterion/a.json", b"a")).unwrap();
        block_on(storage.get("v1/p/objects/criterion/a.json")).unwrap();

        // A second synchronize against the unchanged (still absent) marker is a no-op
        // that leaves the mirrored object in place.
        block_on(storage.synchronize("p", &reporter)).unwrap();
        assert!(
            storage
                .cache
                .keys()
                .contains(&"v1/p/objects/criterion/a.json".to_owned()),
            "a matching epoch leaves the mirror intact"
        );
        assert!(reporter.contains("epochs match"));
    }

    #[test]
    fn synchronize_wipes_the_mirror_when_the_epoch_changes() {
        let storage = caching();
        let reporter = RecordingReporter::new();
        block_on(storage.inner().put("v1/p/objects/criterion/a.json", b"a")).unwrap();
        block_on(storage.get("v1/p/objects/criterion/a.json")).unwrap();
        // Record the genesis epoch.
        block_on(storage.synchronize("p", &reporter)).unwrap();

        // A cloud mutation bumps the marker; the next synchronize must wipe the
        // mirror and re-record the new epoch.
        block_on(
            storage
                .inner()
                .put_overwrite(&cache_epoch_key("p"), b"2024-01-01T00:00:00Z"),
        )
        .unwrap();
        block_on(storage.synchronize("p", &reporter)).unwrap();
        assert_eq!(
            storage.cache.keys(),
            vec![cache_epoch_key("p")],
            "the mirror is wiped down to the freshly recorded epoch"
        );
        assert!(reporter.contains("epochs differ"));
    }

    #[test]
    fn synchronize_treats_an_absent_marker_as_a_stable_genesis_epoch() {
        let storage = caching();
        let reporter = RecordingReporter::new();
        // No marker exists, so the recorded epoch must be the genesis sentinel.
        block_on(storage.synchronize("p", &reporter)).unwrap();
        let recorded = block_on(storage.cache.get(&cache_epoch_key("p"))).unwrap();
        assert_eq!(recorded, GENESIS_EPOCH);
    }

    #[test]
    fn report_tally_counts_hits_and_misses() {
        let storage = caching();
        let reporter = RecordingReporter::new();
        block_on(storage.inner().put("v1/p/objects/criterion/a.json", b"a")).unwrap();
        // One miss (cold) then one hit (mirrored).
        block_on(storage.get("v1/p/objects/criterion/a.json")).unwrap();
        block_on(storage.get("v1/p/objects/criterion/a.json")).unwrap();

        storage.report_tally(&reporter);
        assert!(
            reporter.contains("served 1 object from the local mirror and fetched 1 object"),
            "{:?}",
            reporter.notes()
        );
    }

    /// A backend whose `get` always fails with a non-`NotFound` I/O error, so the
    /// decorator's error-propagation arms — which must forward anything that is not
    /// a cache miss rather than swallow it — can be exercised without a real cloud.
    #[derive(Debug)]
    struct GetFailsStorage;

    impl Storage for GetFailsStorage {
        fn put(&self, _key: &str, _bytes: &[u8]) -> impl Future<Output = Result<(), StorageError>> {
            ready(Ok(()))
        }

        fn put_overwrite(
            &self,
            _key: &str,
            _bytes: &[u8],
        ) -> impl Future<Output = Result<(), StorageError>> + Send {
            ready(Ok(()))
        }

        fn get(&self, _key: &str) -> impl Future<Output = Result<Vec<u8>, StorageError>> + Send {
            ready(Err(TestStorageError::new().into()))
        }

        fn list(&self, _prefix: &str) -> impl Future<Output = Result<Vec<String>, StorageError>> {
            ready(Ok(Vec::new()))
        }

        fn delete(&self, _key: &str) -> impl Future<Output = Result<(), StorageError>> {
            ready(Ok(()))
        }
    }

    #[test]
    fn put_targets_the_cloud_and_not_the_mirror() {
        let storage = caching();
        block_on(storage.put("v1/p/objects/criterion/a.json", b"body")).unwrap();

        // The write lands in the authoritative backend...
        assert_eq!(
            block_on(storage.inner().get("v1/p/objects/criterion/a.json")).unwrap(),
            b"body"
        );
        // ...and the mirror stays empty until a later read populates it (writes are
        // pass-through, not write-through).
        assert!(
            storage.cache.keys().is_empty(),
            "{:?}",
            storage.cache.keys()
        );
    }

    #[test]
    fn put_overwrite_targets_the_cloud_and_not_the_mirror() {
        let storage = caching();
        block_on(storage.put_overwrite("v1/p/objects/criterion/a.json", b"body")).unwrap();

        assert_eq!(
            block_on(storage.inner().get("v1/p/objects/criterion/a.json")).unwrap(),
            b"body"
        );
        assert!(
            storage.cache.keys().is_empty(),
            "{:?}",
            storage.cache.keys()
        );
    }

    #[test]
    fn delete_removes_from_the_cloud() {
        let storage = caching();
        block_on(
            storage
                .inner()
                .put("v1/p/objects/criterion/a.json", b"body"),
        )
        .unwrap();

        block_on(storage.delete("v1/p/objects/criterion/a.json")).unwrap();
        let error = block_on(storage.inner().get("v1/p/objects/criterion/a.json")).unwrap_err();
        assert!(error.is_not_found());
        assert!(error.find_source::<ObjectNotFoundError>().is_some());
    }

    #[test]
    fn get_propagates_a_mirror_read_error() {
        // A mirror read failing with anything other than NotFound is a real fault,
        // not a miss, so get must surface it instead of falling through to the cloud.
        let storage = CachingStorage::new(MemoryStorage::new(), GetFailsStorage);
        let error = block_on(storage.get("v1/p/objects/criterion/a.json")).unwrap_err();
        assert!(error.find_source::<TestStorageError>().is_some());
    }

    #[test]
    fn synchronize_propagates_a_marker_read_error() {
        // The cloud marker read failing with anything other than NotFound must abort
        // synchronization rather than be mistaken for a genesis (absent) marker.
        let storage = CachingStorage::new(GetFailsStorage, MemoryStorage::new());
        let reporter = RecordingReporter::new();
        let error = block_on(storage.synchronize("p", &reporter)).unwrap_err();
        assert!(error.find_source::<TestStorageError>().is_some());
    }

    #[test]
    fn synchronize_propagates_a_recorded_epoch_read_error() {
        // The cloud marker is genesis (absent, so inner returns NotFound), but if
        // reading the mirror's own recorded epoch fails for a non-NotFound reason,
        // synchronize must surface it rather than silently treat the mirror as cold.
        let storage = CachingStorage::new(MemoryStorage::new(), GetFailsStorage);
        let reporter = RecordingReporter::new();
        let error = block_on(storage.synchronize("p", &reporter)).unwrap_err();
        assert!(error.find_source::<TestStorageError>().is_some());
    }
}
