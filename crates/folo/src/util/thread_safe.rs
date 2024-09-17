use std::ops::{Deref, DerefMut};

/// A promise that a T is thread-safe (`Send` and `Sync`), even if the type `T` is not originally
/// so. This may be because `T` is over-generalized and not all instances are thread-safe, or
/// because it is used in special circumstances where its typical thread-safety guarantees can be
/// strengthened by other factors.
#[derive(Debug)]
pub struct ThreadSafe<T> {
    inner: T,
}

unsafe impl<T> Send for ThreadSafe<T> {}
unsafe impl<T> Sync for ThreadSafe<T> {}

impl<T> ThreadSafe<T> {
    /// # Safety
    ///
    /// The caller must ensure that the inner value truly is thread-safe,
    /// both for sending and for referencing (`Send` and `Sync`).
    pub unsafe fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> Deref for ThreadSafe<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for ThreadSafe<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> Clone for ThreadSafe<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> PartialEq for ThreadSafe<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T> Eq for ThreadSafe<T> where T: Eq {}

impl<T> PartialOrd for ThreadSafe<T>
where
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

impl<T> Ord for ThreadSafe<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.inner.cmp(&other.inner)
    }
}
