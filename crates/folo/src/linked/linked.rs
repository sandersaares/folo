// Copyright (c) Microsoft Corporation.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use crate::linked::__private::{InstanceFactory, Link};

/// Operations supported on an instance of a [linked object][folo::linked].
///
/// The only supported way to implement this is via [`#[linked::object]`][folo::linked::object].
pub trait Linked: Sized + Clone {
    /// Gets a thread-safe handle that can be used to create linked instances on other threads.
    ///
    /// The returned handle can be converted into a new instance of a linked object via
    /// the `From<Handle<T>> for T` implementation.
    fn handle(&self) -> Handle<Self>;
}

/// A handle that can be transformed into an instance of a linked object from a specific family of
/// linked objects.
///
/// The handle can be cloned to allow multiple instances of the linked object to be created.
/// Alternatively, the instances of linked objects can themselves be cloned - both approaches
/// end up with the same outcome.
///
/// # Thread safety
///
/// The handle is thread-safe and may be used on any thread.
#[derive(Clone)]
pub struct Handle<T> {
    // For the handle, we extract the factory from the `Link` because the `Link` is not thread-safe.
    // In other words, a `Link` exists only in interactions with a specific instance of `T`.
    instance_factory: InstanceFactory<T>,
}

impl<T> Debug for Handle<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle")
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

impl<T> Handle<T> {
    pub(super) fn new(link: Link<T>) -> Self {
        Self {
            instance_factory: link.instance_factory,
        }
    }

    // Implementation of `From<Handle<T>> for T`, called from macro-generated code for a specific T.
    #[doc(hidden)]
    pub fn __private_into(self) -> T {
        Link::new(Arc::clone(&self.instance_factory)).into_instance()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::thread;

    use super::*;
    use crate::linked::{self, Linked};

    #[test]
    fn linked_objects() {
        #[linked::object]
        struct Thing {
            local_value: usize,
            global_value: Arc<Mutex<String>>,
        }

        impl Thing {
            pub fn new(local_value: usize, global_value: String) -> Self {
                let global_value = Arc::new(Mutex::new(global_value));

                linked::new!(Self {
                    local_value,
                    global_value: Arc::clone(&global_value),
                })
            }

            fn set_global_value(&self, value: &str) {
                let mut global_value = self.global_value.lock().unwrap();
                *global_value = value.to_string();
            }

            fn get_global_value(&self) -> String {
                let global_value = self.global_value.lock().unwrap();
                global_value.clone()
            }

            fn get_local_value(&self) -> usize {
                self.local_value
            }

            fn set_local_value(&mut self, value: usize) {
                self.local_value = value;
            }
        }

        let mut linked_object = Thing::new(42, "hello".to_string());

        assert_eq!(linked_object.get_local_value(), 42);
        assert_eq!(linked_object.get_global_value(), "hello");

        let clone = linked_object.clone();

        linked_object.set_global_value("world");
        linked_object.set_local_value(43);

        assert_eq!(linked_object.get_local_value(), 43);
        assert_eq!(linked_object.get_global_value(), "world");

        assert_eq!(clone.get_local_value(), 42);
        assert_eq!(clone.get_global_value(), "world");

        let handle = linked_object.handle();

        thread::spawn(move || {
            let mut linked_object: Thing = handle.into();

            assert_eq!(linked_object.get_local_value(), 42);
            assert_eq!(linked_object.get_global_value(), "world");

            linked_object.set_global_value("paradise");
            linked_object.set_local_value(45);
        })
        .join()
        .unwrap();

        assert_eq!(linked_object.get_local_value(), 43);
        assert_eq!(linked_object.get_global_value(), "paradise");

        assert_eq!(clone.get_local_value(), 42);
        assert_eq!(clone.get_global_value(), "paradise");
    }

    #[test]
    fn empty_struct() {
        #[linked::object]
        struct Empty {}

        impl Empty {
            pub fn new() -> Self {
                linked::new!(Self {})
            }
        }

        _ = Empty::new();
    }

    #[test]
    fn very_empty_struct() {
        #[linked::object]
        struct Empty {}

        impl Empty {
            pub fn new() -> Self {
                linked::new!(Self)
            }
        }

        _ = Empty::new();
    }
}
