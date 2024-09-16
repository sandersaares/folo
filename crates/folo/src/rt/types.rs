// A synchronous task whose return type has been erased. It will be executed but no result will
// be made available.
pub(crate) type ErasedSyncTask = Box<dyn FnOnce() + Send + 'static>;
