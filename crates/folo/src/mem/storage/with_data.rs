// Copyright (c) Microsoft Corporation.

/// Enables stored data to be accessed via reference within closures. This is an alternate
/// mechanism to `read()`/`write()` exposed by [`ReadStorage`][super::ReadStorage] and
/// [`WriteStorage`][super::WriteStorage]. Functionally equivalent, just a different syntax
/// for achieving the same thing.
pub trait WithData<T> {
    /// Provides read access to the data for the duration of a closure.
    ///
    /// # Examples
    ///
    /// ```
    /// # use folo::rt::storage::*;
    /// let my_box = ThreadLocalStorage::with_initial_value(9000);
    ///
    /// let squared = my_box.with(|val| val * val);
    /// assert_eq!(81_000_000, squared);
    /// ```
    fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R;

    /// Provides mutable access to the data for the duration of a closure.
    ///
    /// Does not require a mutable reference - interior mutability is used,
    /// with caller being responsible for ensuring no concurrent mutation occurs.
    ///
    /// # Examples
    ///
    /// ```
    /// # use folo::rt::storage::*;
    /// let my_box = ThreadLocalStorage::with_initial_value(9000);
    ///
    /// let squared = my_box.with_mut(|val| {
    ///     let result = *val * *val;
    ///     *val = 0;
    ///
    ///     result
    /// });
    /// assert_eq!(81_000_000, squared);
    ///
    /// let squared = my_box.with(|val| val * val);
    /// assert_eq!(0, squared);
    /// ```
    fn with_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R;
}
