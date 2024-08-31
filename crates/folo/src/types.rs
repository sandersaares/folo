use std::{future::Future, pin::Pin};

// A task whose return type has been erased. It will be polled but no result will be made available.
// This is the single-threaded variant.
pub(crate) type LocalErasedTask = Pin<Box<dyn Future<Output = ()> + 'static>>;

// A task whose return type has been erased. It will be polled but no result will be made available.
// This is the thread-safe variant.
pub(crate) type RemoteErasedTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
