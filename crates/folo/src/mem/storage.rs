// Copyright (c) Microsoft Corporation.

mod handle;
mod readwrite;
mod thread_local;
mod thread_local_inline;
mod with_data;

#[cfg(test)]
mod test_types;

// Flatten the hierarchy - we only use the submodules to keep code separate but do not want the hierarchy.
pub use handle::*;
pub use readwrite::*;
pub use thread_local::*;
pub use thread_local_inline::*;
pub use with_data::*;

/// Placeholder for the day when we actually support NUMA-local storage.
/// For now we pretend each thread is a separate NUMA node, as we need a fake model that still has
/// more than 1 instance (we could also use global storage but that is more explicitly singular).
pub type NumaLocalStorage<T> = ThreadLocalStorage<T>;

/// Placeholder for the day when we actually support NUMA-local storage.
/// For now we pretend each thread is a separate NUMA node, as we need a fake model that still has
/// more than 1 instance (we could also use global storage but that is more explicitly singular).
pub type NumaLocalStorageHandle<T> = ThreadLocalStorageHandle<T>;
