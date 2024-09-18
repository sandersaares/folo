// Copyright (c) Microsoft Corporation.

#[doc(hidden)]
#[macro_export]
macro_rules! __macro_linked_new {
    (Self) => {
        folo::linked::new!(Self {})
    };
    (Self { }) => {
        ::folo::linked::__private::new(move |__private_folo_linked_link| Self {
            __private_folo_linked_link,
        })
    };
    (Self { $($field:ident $( : $value:expr )?),* $(,)? }) => {
        ::folo::linked::__private::new(move |__private_folo_linked_link| Self {
            $($field: folo::linked::new!(@expand $field $( : $value )?)),*,
            __private_folo_linked_link,
        })
    };
    (@expand $field:ident : $value:expr) => {
        $value
    };
    (@expand $field:ident) => {
        $field
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __macro_linked_link {
    () => {};

    ($(#[$attr:meta])* $vis:vis static $NAME:ident: $t:ty = $e:expr; $($rest:tt)*) => (
        folo::linked::link!($(#[$attr])* $vis static $NAME: $t = $e);
        folo::linked::link!($($rest)*);
    );

    ($(#[$attr:meta])* $vis:vis static $NAME:ident: $t:ty = $e:expr) => {
        paste::paste! {
            #[doc(hidden)]
            #[allow(non_camel_case_types)]
            struct [<__lookup_key_ $NAME>];

            $(#[$attr])* $vis const $NAME: ::folo::linked::Variable<$t> =
                ::folo::linked::Variable::new(
                    ::std::any::TypeId::of::<[<__lookup_key_ $NAME>]>,
                    move || $e);
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __macro_linked_link_ref {
    () => {};

    ($(#[$attr:meta])* $vis:vis static $NAME:ident: $t:ty = $e:expr; $($rest:tt)*) => (
        folo::linked::link_ref!($(#[$attr])* $vis static $NAME: $t = $e);
        folo::linked::link_ref!($($rest)*);
    );

    ($(#[$attr:meta])* $vis:vis static $NAME:ident: $t:ty = $e:expr) => {
        paste::paste! {
            folo::linked::link!(#[doc(hidden)] static [< $NAME _INITIALIZER >]: $t = $e;);

            thread_local!(#[doc(hidden)] static [< $NAME _RC >]: ::std::rc::Rc<$t> = ::std::rc::Rc::new([< $NAME _INITIALIZER >].get()));

            $(#[$attr])* $vis const $NAME: ::folo::linked::VariableByRc<$t> =
                ::folo::linked::VariableByRc::new(move || &[< $NAME _RC >]);
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __macro_linked_new_box {
    ($dyn_trait:ty, $ctor:expr) => {
        ::folo::linked::Box::new(move || {
            ::std::boxed::Box::new($ctor) as ::std::boxed::Box<$dyn_trait>
        })
    };
}
