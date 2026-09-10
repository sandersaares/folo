// Copyright (c) Microsoft Corporation.
// Copyright (c) Folo authors.

use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Fields, FieldsNamed, Item, ItemStruct, parse_quote};

use crate::syn_helpers::token_stream_and_error;

#[must_use]
pub fn entrypoint(_attr: &TokenStream, input: &TokenStream) -> TokenStream {
    let item_ast = syn::parse2::<Item>(input.clone());

    let result = match item_ast {
        Ok(Item::Struct(item)) => core(item),
        Ok(x) => Err(syn::Error::new(
            x.span(),
            "the `linked::object` attribute must be applied to a struct",
        )),
        Err(e) => Err(e),
    };

    match result {
        Ok(r) => r,
        Err(e) => token_stream_and_error(input, &e),
    }
}

fn core(mut item: ItemStruct) -> Result<TokenStream, syn::Error> {
    let (impl_generics, type_generics, where_clause) = &item.generics.split_for_impl();
    let name = &item.ident;

    let Fields::Named(FieldsNamed { named: fields, .. }) = &mut item.fields else {
        return Err(syn::Error::new(
            item.span(),
            "the `linked::object` attribute must be applied to a struct with named fields",
        ));
    };

    // We add a field to store the Link<Self>, which is later referenced by other macros.
    fields
        .push(parse_quote!(#[doc(hidden)] __private_linked_link: ::linked::__private::Link<Self>));

    let extended = quote! {
        #item

        impl #impl_generics ::linked::Object for #name #type_generics #where_clause {
            fn family(&self) -> ::linked::Family<Self> {
                self.__private_linked_link.family()
            }
        }

        impl #impl_generics Clone for #name #type_generics #where_clause {
            fn clone(&self) -> Self {
                ::linked::__private::clone(self)
            }
        }

        impl #impl_generics ::std::convert::From<::linked::Family<#name #type_generics>> for #name #type_generics #where_clause {
            fn from(family: ::linked::Family<#name #type_generics>) -> Self {
                family.__private_into()
            }
        }
    };

    Ok(extended)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use quote::quote;

    use super::*;
    use crate::syn_helpers::contains_compile_error;

    #[test]
    fn smoke_test() {
        let input = quote! {
            struct Foo {
            }
        };

        let result = entrypoint(&TokenStream::new(), &input);

        let expected = quote! {
            struct Foo {
                #[doc(hidden)]
                __private_linked_link: ::linked::__private::Link<Self>
            }

            impl ::linked::Object for Foo {
                fn family(&self) -> ::linked::Family<Self> {
                    self.__private_linked_link.family()
                }
            }

            impl Clone for Foo {
                fn clone(&self) -> Self {
                    ::linked::__private::clone(self)
                }
            }

            impl ::std::convert::From<::linked::Family<Foo>> for Foo {
                fn from(family: ::linked::Family<Foo>) -> Self {
                    family.__private_into()
                }
            }
        };

        assert_eq!(result.to_string(), expected.to_string());
    }

    #[test]
    fn smoke_test_with_generics() {
        let input = quote! {
            struct Foo<'y, T: Clone>
            where
                T: Debug
            {
                x: &'y T,
            }
        };

        let result = entrypoint(&TokenStream::new(), &input).to_string();

        // The non-generic smoke test checks complete method bodies under Miri. This case needs
        // only the generic-bearing declarations there, while native runs compare the full output.
        #[cfg(miri)]
        for declaration in [
            quote!(struct Foo<'y, T: Clone> where T: Debug),
            quote!(x: &'y T),
            quote!(impl<'y, T: Clone> ::linked::Object for Foo<'y, T> where T: Debug),
            quote!(impl<'y, T: Clone> Clone for Foo<'y, T> where T: Debug),
            quote!(
                impl<'y, T: Clone> ::std::convert::From<::linked::Family<Foo<'y, T> >>
                    for Foo<'y, T> where T: Debug
            ),
            quote!(fn from(family: ::linked::Family<Foo<'y, T> >) -> Self),
        ] {
            assert!(result.contains(&declaration.to_string()));
        }

        #[cfg(not(miri))]
        let expected = quote! {
            struct Foo<'y, T: Clone>
            where
                T: Debug
            {
                x: &'y T,
                #[doc(hidden)]
                __private_linked_link: ::linked::__private::Link<Self>
            }

            impl<'y, T: Clone> ::linked::Object for Foo<'y, T>
            where
                T: Debug
            {
                fn family(&self) -> ::linked::Family<Self> {
                    self.__private_linked_link.family()
                }
            }

            impl<'y, T: Clone> Clone for Foo<'y, T>
            where
                T: Debug
            {
                fn clone(&self) -> Self {
                    ::linked::__private::clone(self)
                }
            }

            impl<'y, T: Clone> ::std::convert::From<::linked::Family<Foo<'y, T> >> for Foo<'y, T>
            where
                T: Debug
            {
                fn from(family: ::linked::Family<Foo<'y, T> >) -> Self {
                    family.__private_into()
                }
            }
        };

        #[cfg(not(miri))]
        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn with_unnamed_fields_fails() {
        let input = quote! {
            struct Foo(usize, String);
        };

        let result = entrypoint(&TokenStream::new(), &input);
        assert!(contains_compile_error(&result));
    }

    #[test]
    fn with_enum_fails() {
        let input = quote! {
            enum Direction { Up, Down }
        };

        let result = entrypoint(&TokenStream::new(), &input);
        assert!(contains_compile_error(&result));
    }

    #[test]
    fn with_invalid_syntax_fails() {
        // This input cannot be parsed by syn because it is not valid Rust syntax.
        let input = quote! {
            struct { missing name }
        };

        let result = entrypoint(&TokenStream::new(), &input);
        assert!(contains_compile_error(&result));
    }
}
