// Copyright (c) Microsoft Corporation.

//! This module contains helper functions for consuming and producing Rust syntax elements.

use proc_macro2::TokenStream;
use quote::quote;

/// Combines a token stream with a syn-originating contextual error message that contains
/// all the necessary metadata to emit rich errors (with red underlines and all that).
///
/// Also preserves the original token stream, merely appending the error instead of replacing.
pub fn token_stream_and_error(s: TokenStream, e: syn::Error) -> TokenStream {
    let error = e.to_compile_error();

    // We preserve both the original input and emit the compiler error message.
    // This ensures that we do not cause extra problems by removing the original input
    // from the code file (which would result in "trait not found" and similar errors).
    quote! {
        #s
        #error
    }
}

/// Attempts to identify any compile-time error in the token stream. This is useful for unit
/// testing macros - if the macro is expected to produce a compile-time error, we can check
/// whether one exists.
///
/// We deliberately do not take an error message as input here. Testing for error messages is
/// fragile and creates maintenance headaches - be satisfied with OK/NOK testing and keep it simple.
#[cfg(test)]
pub fn contains_compile_error(tokens: &TokenStream) -> bool {
    // String-based implementation, so vulnerable to false positives in very unlikely cases.
    tokens.to_string().contains(":: core :: compile_error ! {")
}

#[cfg(test)]
mod tests {
    use proc_macro2::Span;

    use super::*;

    #[test]
    fn token_stream_and_error_outputs_both() {
        // This is a bit tricky because we do not know the specific form the compiler error
        // is going to be. However, we know it must contain our error message, so just check that.
        let canary = "nrtfynjcrtupyh6rhdoj85m7yoi";

        // We also need to ensure it contains this function (that it did not get overwritten).
        let s = quote! {
            fn gkf5dj8yhuldri58uygdkiluyot() {}
        };

        let e = syn::Error::new(proc_macro2::Span::call_site(), canary);

        let merged = token_stream_and_error(s, e);

        let merged_str = merged.to_string();
        assert!(merged_str.contains(canary));
        assert!(merged_str.contains("gkf5dj8yhuldri58uygdkiluyot"));
    }

    #[test]
    fn contains_compile_error_yes_raw() {
        let tokens = quote! {
            let foo = "Some random stuff may also be here";
            blah! { blah }
            ::core::compile_error! { "This is a test error message." };
            let bar = "More random stuff here"
        };

        assert!(contains_compile_error(&tokens));
    }

    #[test]
    fn contains_compile_error_yes_generated() {
        let tokens = quote! {
            let foo = "Some random stuff may also be here";
            blah! { blah }
            ::core::compile_error!("This is a test error message.");
            let bar = "More random stuff here"
        };

        let tokens = token_stream_and_error(tokens, syn::Error::new(Span::call_site(), "Testing"));

        assert!(contains_compile_error(&tokens));
    }

    #[test]
    fn contains_compile_error_no() {
        let tokens = quote! {
            let foo = "No compile error here!"
        };

        assert!(!contains_compile_error(&tokens));
    }
}
