// Copyright (c) Microsoft Corporation.

//! Mechanisms for creating linked objects.
//!
//! Linked objects are types whose instances:
//!
//! 1. Are internally connected with other instances from the same family.
//! 2. Share some state with each other via messaging or synchronized state.
//! 3. Perform the collaboration independently and out of view of user code.
//!
//! Instances belong to the same family if they:
//! - are created via cloning.
//! - are created by obtaining a thread-safe [Handle] and converting it to a new instance.
//! - are obtained from the same static variable in a [link!][folo::linked::link] macro block.
//!
//! The linked object pattern integrates with the folo highly isolated and data-local execution
//! model by providing explicit paths for cross-thread transfer of instances and keeping each
//! instance natively single-threaded.
//!
//! # Using and defining linked objects
//!
//! A very basic and contrived example is a `Thing` that shares a `value` between all its instances.
//!
//! This object can generally be used like any other Rust type. All linked objects support cloning,
//! since that is one of the primary mechanisms for creating additional linked instances.
//!
//! ```
//! # use folo::linked;
//! # use std::sync::{Arc, Mutex};
//! # #[linked::object]
//! # struct Thing {
//! #     value: Arc<Mutex<String>>,
//! # }
//! # impl Thing {
//! #     pub fn new(initial_value: String) -> Self {
//! #         let shared_value = Arc::new(Mutex::new(initial_value));
//! #         linked::new!(Self {
//! #             value: shared_value.clone(),
//! #         })
//! #     }
//! #     pub fn value(&self) -> String {
//! #         self.value.lock().unwrap().clone()
//! #     }
//! #     pub fn set_value(&self, value: String) {
//! #         *self.value.lock().unwrap() = value;
//! #     }
//! # }
//! let thing1 = Thing::new("hello".to_string());
//! let thing2 = thing1.clone();
//! assert_eq!(thing1.value(), "hello");
//! assert_eq!(thing2.value(), "hello");
//!
//! thing1.set_value("world".to_string());
//! assert_eq!(thing1.value(), "world");
//! assert_eq!(thing2.value(), "world");
//! ```
//!
//! We can compare this example to the linked object definition above:
//!
//! * The relation is established via cloning.
//! * The “value” is shared.
//! * Implementing the collaboration does not require action (e.g. Mutex) from user code.
//!
//! The implementation of this type is the following:
//!
//! ```
//! use folo::linked;
//! use std::sync::{Arc, Mutex};
//!
//! #[linked::object]
//! pub struct Thing {
//!     value: Arc<Mutex<String>>,
//! }
//!
//! impl Thing {
//!     pub fn new(initial_value: String) -> Self {
//!         let shared_value = Arc::new(Mutex::new(initial_value));
//!
//!         linked::new!(Self {
//!             value: Arc::clone(&shared_value),
//!         })
//!     }
//!
//!     pub fn value(&self) -> String {
//!         self.value.lock().unwrap().clone()
//!     }
//!
//!     pub fn set_value(&self, value: String) {
//!         *self.value.lock().unwrap() = value;
//!     }
//! }
//! ```
//!
//! The implementation steps are:
//!
//! * Apply [`#[linked::object]`][folo::linked::object] on the struct. This will automatically
//!   derive `Linked`, `Clone` and other mechanisms required for the linked objects pattern.
//! * In the constructor, call [`linked::new!`][folo::linked::new] to create the first instance.
//!
//! [`linked::new!`][folo::linked::new] is a wrapper around a `Self` struct-expression. What makes
//! it special is that it will be called for every instance that is ever created in the same family
//! of linked objects. This expression captures the state of the constructor (e.g. in the above
//! example, it captures `shared_value`). Use the captured state to set up any shared connections
//! between instances (e.g. by sharing an `Arc` or connecting message channels).
//!
//! The captured values must be thread-safe (`Send` + `Sync` + `'static`).
//!
//! # Linked objects on multiple threads
//!
//! Each instance of a linked object is single-threaded (enforced at compile time). To create a
//! related instance on a different thread, you must either use a static variable inside a
//! [link!][folo::linked::link] block or use a [Handle] to transfer the instance to another thread.
//!
//! Example of using a static variable to connect instances on different threads:
//!
//! ```
//! # use folo::linked;
//! # use std::sync::{Arc, Mutex};
//! # #[linked::object]
//! # struct Thing {
//! #     value: Arc<Mutex<String>>,
//! # }
//! # impl Thing {
//! #     pub fn new(initial_value: String) -> Self {
//! #         let shared_value = Arc::new(Mutex::new(initial_value));
//! #         linked::new!(Self {
//! #             value: shared_value.clone(),
//! #         })
//! #     }
//! #     pub fn value(&self) -> String {
//! #         self.value.lock().unwrap().clone()
//! #     }
//! #     pub fn set_value(&self, value: String) {
//! #         *self.value.lock().unwrap() = value;
//! #     }
//! # }
//! use folo::linked::link;
//! use std::thread;
//!
//! link!(static THE_THING: Thing = Thing::new("hello".to_string()));
//!
//! let thing = THE_THING.get();
//! assert_eq!(thing.value(), "hello");
//!
//! thing.set_value("world".to_string());
//!
//! thread::spawn(|| {
//!     let thing = THE_THING.get();
//!     assert_eq!(thing.value(), "world");
//! }).join().unwrap();
//! ```
//!
//! Example of using a [Handle] to transfer an instance to another thread:
//!
//! ```
//! # use folo::linked;
//! # use std::sync::{Arc, Mutex};
//! # #[linked::object]
//! # struct Thing {
//! #     value: Arc<Mutex<String>>,
//! # }
//! # impl Thing {
//! #     pub fn new(initial_value: String) -> Self {
//! #         let shared_value = Arc::new(Mutex::new(initial_value));
//! #         linked::new!(Self {
//! #             value: shared_value.clone(),
//! #         })
//! #     }
//! #     pub fn value(&self) -> String {
//! #         self.value.lock().unwrap().clone()
//! #     }
//! #     pub fn set_value(&self, value: String) {
//! #         *self.value.lock().unwrap() = value;
//! #     }
//! # }
//! use folo::linked::Linked; // This brings .handle() into scope.
//! use std::thread;
//!
//! let thing = Thing::new("hello".to_string());
//! assert_eq!(thing.value(), "hello");
//!
//! thing.set_value("world".to_string());
//!
//! let thing_handle = thing.handle();
//!
//! thread::spawn(|| {
//!     let thing: Thing = thing_handle.into();
//!     assert_eq!(thing.value(), "world");
//! }).join().unwrap();
//! ```
//!
//! # Using linked objects via abstractions
//!
//! You may find yourself in a situation where you need to use a linked object type `T` through
//! a trait object of a trait `Xyz` that `T` implements, as `dyn Xyz`. This is a common pattern
//! in Rust but with the linked objects pattern there is a choice you must make:
//!
//! * If the linked objects are **always** to be accessed via trait objects, wrap the instances in
//!   [`linked::Box`][Box], already returning such a box in the constructor.
//! * If the linked objects are **sometimes** to be accessed via trait objects, you can on-demand
//!   wrap them into a [`Box<dyn Xyz>`][std::boxed::Box].
//!
//! The difference is that [`linked::Box`][Box] preserves the linked object functionality - you can
//! clone the box, obtain a [`Handle<linked::Box<dyn Xyz>>`][Handle] to transfer the box to another
//! thread and store such a box in a static variable in a [link!][folo::linked::link] block.
//! However, when you use a [`Box<dyn Xyz>`][std::boxed::Box], you lose the linked object
//! functionality.
//!
//! ```
//! # trait ConfigSource {}
//! # struct XmlConfig { config: String }
//! # impl ConfigSource for XmlConfig {}
//! use folo::linked;
//!
//! impl XmlConfig {
//!     pub fn new_as_config_source() -> linked::Box<dyn ConfigSource> {
//!         linked::new_box!(
//!             dyn ConfigSource,
//!             Self {
//!                 config: "xml".to_string(),
//!             }
//!         )
//!     }
//! }
//! ```
//!
//! # Additional examples
//!
//! See `examples/linked_*.rs` for more examples of using linked objects in different scenarios.

#[doc(hidden)]
pub mod __private;

mod block;
pub use block::*;

mod block_rc;
pub use block_rc::*;

mod r#box;
pub use r#box::*;

#[allow(clippy::module_inception)] // On purpose, to keep this file clean.
mod linked;
pub use linked::*;

/// Marks a struct as implementing the linked object pattern. See `folo::linked` for details.
///
/// # Usage
///
/// Apply attribute on a struct block.
///
/// # Example
///
/// ```
/// use folo::linked;
///
/// #[linked::object]
/// pub struct TokenCache {
///     some_value: usize,
/// }
///
/// impl TokenCache {
///     pub fn new(some_value: usize) -> Self {
///         linked::new!(Self {
///             some_value,
///         })
///     }
/// }
/// ```
///
/// # Effects
///
/// Applying this macro has the following effects:
///
/// 1. Generates the necessary wiring to support calling `folo::linked::new!` in constructors.
/// 2. Implements `Clone` for the struct. All linked objects can be cloned to create new linked
///    instances.
/// 3. Implements `folo::linked::Linked` for the struct, enabling additional linked object
///    pattern mechanisms such as calling `.handle()` on instances.
/// 4. Implements `From<folo::linked::Handle<T>>` for the struct. This allows creating a new
///    linked instance from a handle previously obtained from `.handle()`.
/// 5. Removes `Send` and `Sync` traits implementation for the struct.
///
/// # Constraints
///
/// Only structs with the named fields form are supported (no tuple structs).
pub use folo_proc_macros::__macro_linked_object as object;

/// Creates the first instance in a linked object family. You are expected to use this in the
/// constructor of a [linked object][folo::linked], except when you want to express the linked
/// object via trait objects, in which case you should use [`linked::new_box`][folo::linked::new_box].
///
/// The macro body must be a struct-expression of the `Self` type. Any variables the macro body
/// captures must be thread-safe (`Send` + `Sync` + `'static`).
///
/// # Example
///
/// ```ignore
/// linked::new!(Self {
///     field1: value1,
///     field2: value2,
///     field3: Arc::clone(&value3),
///     another_field,
/// })
/// ```
///
/// Complex expressions as values are supported within the `Self` struct-expression:
///
/// ```ignore
/// linked::new!(Self {
///     sources: source_handles
///         .iter()
///         .cloned()
///         .map(Handle::into)
///         .collect_vec()
/// })
/// ```
///
/// For a complete example, see `examples/linked_basic.rs`.
#[doc(inline)]
pub use folo_decl_macros::__macro_linked_new as new;
