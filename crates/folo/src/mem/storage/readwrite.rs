// Copyright (c) Microsoft Corporation.

use std::ops::{Deref, DerefMut};

/// Represents the capability to immutably access the contents of a storage object.
pub trait ReadStorage<'s, R, T>
where
    R: Deref<Target = T>,
{
    /// Accesses the storage object for reading. See the storage object documentation for
    /// information about concurrent access concerns - it differs from storage to storage.
    fn read(&'s self) -> R;
}

/// Represents the capability to mutably access the contents of a storage object.
pub trait WriteStorage<'s, W, T>
where
    W: DerefMut<Target = T>,
{
    /// Accesses the storage object for writing. See the storage object documentation for
    /// information about concurrent access concerns - it differs from storage to storage.
    ///
    /// Does not require a mutable reference - interior mutability is used. Caller is responsible
    /// for ensuring no concurrent writes or concurrent reads+writes occur (on pain of panic).
    fn write(&'s self) -> W;
}
