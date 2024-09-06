use std::{future::Future, pin::Pin};

// An async task whose return type has been erased. It will be polled but no result will be made
// available. This is the single-threaded variant.
pub(crate) type LocalErasedAsyncTask = Pin<Box<dyn Future<Output = ()> + 'static>>;

// An async task whose return type has been erased. It will be polled but no result will be made
// available. This is the thread-safe variant.
pub(crate) type RemoteErasedAsyncTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

// A synchronous task whose return type has been erased. It will be executed but no result will
// be made available.
pub(crate) type ErasedSyncTask = Box<dyn FnOnce() -> () + Send + 'static>;
