// Copyright (c) Microsoft Corporation.

/// A handle that can be transformed into a storage object. Handles are multithreaded objects
/// that can be passed freely between threads and accessed from any thread but which gets unpacked
/// into a single-threaded storage object on the destination thread.
pub trait StorageHandle<S>: Clone + Send + Sync + 'static {
    fn into_storage(self) -> S;
}
