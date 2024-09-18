// Copyright (c) Microsoft Corporation.

use std::boxed::Box as StdBox;
use std::ops::{Deref, DerefMut};

use crate::linked;

/// A linked object that acts like a `Box<T>` over linked instances of `T`. This is primarily meant
/// to be used with the `T` being a trait object, for types exposed to user code via trait objects.
///
/// The `Box` itself implements the linked object mechanics like storing the `Link<T>`. The type
/// `T` within does not need to be extended with any elements of the linked object pattern.
///
/// # Usage
///
/// Use it like a regular `Box<T>` that also happens to support the linked objects API
/// via the [`link!` macro][folo::linked::link] and offers the option for handle-facilitated
/// transfer across threads.
///
/// # Implementation
///
/// Instead of a typical constructor, create one that returns `linked::Box<T>`. Inside this
/// constructor, create a `linked::Box` instance using the
/// [`linked::new_box!` macro][folo::linked::new_box].
///
/// ```
/// # trait ConfigSource {}
/// # struct XmlConfig { config: String }
/// # impl ConfigSource for XmlConfig {}
/// use folo::linked;
///
/// impl XmlConfig {
///     pub fn new_as_config_source() -> linked::Box<dyn ConfigSource> {
///         linked::new_box!(
///             dyn ConfigSource,
///             Self {
///                 config: "xml".to_string(),
///             }
///         )
///     }
/// }
/// ```
///
/// Any connections between the instances should be established via the captured state of this
/// closure (e.g. sharing an `Arc` or setting up messaging channels).
///
/// # Example
///
/// ```
/// use folo::linked;
///
/// trait ConfigSource {
///     fn config(&self) -> String;
/// }
///
/// struct XmlConfig {}
/// struct IniConfig {}
///
/// impl ConfigSource for XmlConfig {
///     fn config(&self) -> String {
///         "xml".to_string()
///     }
/// }
///
/// impl ConfigSource for IniConfig {
///     fn config(&self) -> String {
///         "ini".to_string()
///     }
/// }
///
/// impl XmlConfig {
///     pub fn new_as_config_source() -> linked::Box<dyn ConfigSource> {
///         linked::new_box!(
///             dyn ConfigSource,
///             XmlConfig {}
///         )
///     }
/// }
///
/// impl IniConfig {
///     pub fn new_as_config_source() -> linked::Box<dyn ConfigSource> {
///         linked::new_box!(
///             dyn ConfigSource,
///             IniConfig {}
///         )
///     }
/// }
///
/// let xml_config = XmlConfig::new_as_config_source();
/// let ini_config = IniConfig::new_as_config_source();
///
/// let configs = [xml_config, ini_config];
///
/// assert_eq!(configs[0].config(), "xml".to_string());
/// assert_eq!(configs[1].config(), "ini".to_string());
/// ```
#[linked::object]
#[derive(Debug)]
pub struct Box<T: ?Sized + 'static> {
    value: StdBox<T>,
}

impl<T: ?Sized> Box<T> {
    pub fn new(instance_factory: impl Fn() -> StdBox<T> + Send + Sync + 'static) -> Self {
        linked::new!(Self {
            value: (instance_factory)(),
        })
    }
}

impl<T: ?Sized + 'static> Deref for Box<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T: ?Sized + 'static> DerefMut for Box<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/// Shorthand macro for creating a new [`linked::Box`][Box] instance, as the exact syntax for that
/// can be cumbersome. This macro is meant to be used in the context of creating a new instance of a
/// linked object `T` that is meant to be always expressed via an abstraction (`dyn SomeTrait`).
///
/// # Arguments
///
/// * `$dyn_trait` - The trait object that the linked object is to be used as (e.g. `dyn SomeTrait`).
/// * `$ctor` - The template for constructing new instances of the linked object on demand. This
///   will be used in a factory function and it will move-capture any referenced state. All captured
///   values must be thread-safe (`Send` + `Sync` + `'static`).
///
/// # Example
///
/// See `examples/linked_box.rs`.
#[doc(inline)]
pub use folo_decl_macros::__macro_linked_new_box as new_box;

#[cfg(test)]
mod tests {
    use std::thread;

    use crate::linked::{self, Linked};

    #[test]
    fn linked_box() {
        trait ConfigSource {
            fn config(&self) -> String;
            fn set_config(&mut self, config: String);
        }

        struct XmlConfig {
            config: String,
        }
        struct IniConfig {
            config: String,
        }

        impl ConfigSource for XmlConfig {
            fn config(&self) -> String {
                self.config.clone()
            }

            fn set_config(&mut self, config: String) {
                self.config = config;
            }
        }

        impl ConfigSource for IniConfig {
            fn config(&self) -> String {
                self.config.clone()
            }

            fn set_config(&mut self, config: String) {
                self.config = config;
            }
        }

        impl XmlConfig {
            pub fn new_as_config_source() -> linked::Box<dyn ConfigSource> {
                linked::new_box!(
                    dyn ConfigSource,
                    Self {
                        config: "xml".to_string(),
                    }
                )
            }
        }

        impl IniConfig {
            pub fn new_as_config_source() -> linked::Box<dyn ConfigSource> {
                linked::new_box!(
                    dyn ConfigSource,
                    Self {
                        config: "ini".to_string(),
                    }
                )
            }
        }

        let xml_config = XmlConfig::new_as_config_source();
        let ini_config = IniConfig::new_as_config_source();

        let mut configs = [xml_config, ini_config];

        assert_eq!(configs[0].config(), "xml".to_string());
        assert_eq!(configs[1].config(), "ini".to_string());

        configs[0].set_config("xml2".to_string());
        configs[1].set_config("ini2".to_string());

        assert_eq!(configs[0].config(), "xml2".to_string());
        assert_eq!(configs[1].config(), "ini2".to_string());

        let xml_config_handle = configs[0].handle();
        let ini_config_handle = configs[1].handle();

        thread::spawn(move || {
            let xml_config: linked::Box<dyn ConfigSource> = xml_config_handle.into();
            let ini_config: linked::Box<dyn ConfigSource> = ini_config_handle.into();

            // Note that the "config" is local to each instance, so it reset to the initial value.
            assert_eq!(xml_config.config(), "xml".to_string());
            assert_eq!(ini_config.config(), "ini".to_string());
        })
        .join()
        .unwrap();
    }
}
