// Copyright (c) Microsoft Corporation.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use negative_impl::negative_impl;

use crate::linked::{Handle, Linked};

/// This is meant to be used via the [`linked::new!`][folo::linked::new] macro.
///
/// Creates a family of linked objects, the instances for which are created using a callback whose
/// captured state connects all members of the linked object family.
///
/// The instance factory must be thread-safe, which implies that all captured state in this factory
/// function must be `Send` + `Sync` + `'static`. The instances it returns do not need to be thread-
/// safe, however.
pub fn new<T>(instance_factory: impl Fn(Link<T>) -> T + Send + Sync + 'static) -> T {
    Link::new(Arc::new(instance_factory)).into_instance()
}

/// This is meant to be used via the `#[linked::object]` macro.
///
/// Clones a linked object. They require a specific pattern to clone, so this is an easy to
/// apply shorthand for that pattern, to avoid accidental mistakes.
pub fn clone<T>(value: &T) -> T
where
    T: Linked + From<Handle<T>>,
{
    value.handle().into()
}

pub(crate) type InstanceFactory<T> = Arc<dyn Fn(Link<T>) -> T + Send + Sync + 'static>;

/// An object that connects an instance to other instances in the same linked object family.
///
/// This type serves the linked object infrastructure and is not meant to be used by user code.
/// It is a private public type because it is used in macro-generated code.
pub struct Link<T> {
    pub(super) instance_factory: InstanceFactory<T>,
}

impl<T> Debug for Link<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Link")
            .field(
                "instance_factory",
                &format_args!(
                    "Arc<dyn Fn(Link<{t}>) -> {t}>",
                    t = std::any::type_name::<T>()
                ),
            )
            .finish()
    }
}

// A `Link` is a single-threaded object to avoid accidentally passing a linked object across
// threads. Instead, use `Handle` (from `Linked::handle()`) to send instances across threads.
#[negative_impl]
impl<T> !Send for Link<T> {}
#[negative_impl]
impl<T> !Sync for Link<T> {}

impl<T> Link<T> {
    pub(super) fn new(instance_factory: InstanceFactory<T>) -> Self {
        Self { instance_factory }
    }

    pub(super) fn into_instance(self) -> T {
        let instance_factory = Arc::clone(&self.instance_factory);
        (instance_factory)(self)
    }

    // This type deliberately does not implement `Clone` to discourage accidental implementation of
    // cloning of type `T` via `#[derive(Clone)]`. The expected pattern is to use `#[linked::object]`
    // which generates both a `Linked` implementation and a specialized `Clone` implementation.
    fn clone(&self) -> Self {
        Self {
            instance_factory: Arc::clone(&self.instance_factory),
        }
    }

    pub fn handle(&self) -> Handle<T> {
        Handle::new(self.clone())
    }
}
