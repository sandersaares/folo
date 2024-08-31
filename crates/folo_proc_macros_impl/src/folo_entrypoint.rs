use crate::util::token_stream_and_error;
use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

#[derive(Debug, Eq, PartialEq)]
pub enum EntrypointType {
    Main,
    Test,
}

/// Implements the Folo entrypoint macro for both types of entry points (main and test functions).
pub fn entrypoint(
    attr: TokenStream,
    input: TokenStream,
    entrypoint_type: EntrypointType,
) -> TokenStream {
    if !attr.is_empty() {
        return token_stream_and_error(
            input,
            syn::Error::new_spanned(attr, "this macro does not accept any arguments"),
        );
    }

    let item_ast = syn::parse2::<ItemFn>(input.clone());

    let result = match item_ast {
        Ok(item) => core(item, entrypoint_type),
        Err(e) => Err(e),
    };

    match result {
        Ok(r) => r,
        Err(e) => token_stream_and_error(input, e),
    }
}

fn core(mut item: ItemFn, entrypoint_type: EntrypointType) -> Result<TokenStream, syn::Error> {
    let sig = &mut item.sig;

    // We do not "care" but the IDE might not understand what is happening without "async".
    // We might in the future also support intentionally sync functions. After all, the runtime can
    // also schedule synchronous work - we should not assume it is for async work in every use case.
    if sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            sig.fn_token,
            "function must be async to use the #[folo::main] or #[folo::test] attribute",
        ));
    }

    sig.asyncness = None;

    let attrs = &item.attrs;
    let vis = &item.vis;
    let body = &item.block;

    // If we are emitting a test entrypoint, stick the test attribute in front. No-op otherwise.
    let test_attr = match entrypoint_type {
        EntrypointType::Main => quote! {},
        EntrypointType::Test => quote! { #[test] },
    };

    Ok(match &sig.output {
        syn::ReturnType::Default => quote! {
            #(#attrs)*
            #test_attr
            #vis #sig {
                let executor = ::folo::ExecutorBuilder::new().build().unwrap();
                let executor_clone = ::std::sync::Arc::clone(&executor);

                executor.spawn_on_any(async move {
                    (async move #body).await;
                    executor_clone.stop();
                });

                executor.wait();
            }
        },
        syn::ReturnType::Type(_, ty) => {
            quote! {
                #(#attrs)*
                #test_attr
                #vis #sig {
                    let executor = ::folo::ExecutorBuilder::new().build().unwrap();
                    let executor_clone = ::std::sync::Arc::clone(&executor);

                    let result_rx = ::std::sync::Arc::new(::std::sync::Mutex::new(Option::<#ty>::None));
                    let result_tx = ::std::sync::Arc::clone(&result_rx);

                    executor.spawn_on_any(async move {
                        let result = (async move #body).await;

                        *result_tx
                            .lock()
                            .expect("poisoned lock") = Some(result);

                        executor_clone.stop();
                    });

                    // If the test fails, generally we panic from here because we detect that a
                    // worker thread panicked.
                    executor.wait();

                    // Reaching this point is highly unlikely if a test fails - at least no
                    // currently known execution path takes us here. Only used for success case.
                    let result = result_rx
                        .lock()
                        .expect("posioned lock")
                        .take()
                        .expect("entrypoint terminated before returning result");

                    result
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::contains_compile_error;
    use syn::parse_quote;

    #[test]
    fn main_returns_default() {
        let input = parse_quote! {
            async fn main() {
                println!("Hello, world!");
                yield_now().await;
            }
        };

        let expected = quote! {
            fn main() {
                let executor = ::folo::ExecutorBuilder::new().build().unwrap();
                let executor_clone = ::std::sync::Arc::clone(&executor);

                executor.spawn_on_any(async move {
                    (async move {
                        println!("Hello, world!");
                        yield_now().await;
                    }).await;

                    executor_clone.stop();
                });

                executor.wait();
            }
        };

        assert_eq!(
            entrypoint(TokenStream::new(), input, EntrypointType::Main).to_string(),
            expected.to_string()
        );
    }

    #[test]
    fn main_returns_result() {
        let input = parse_quote! {
            async fn main() -> Result<(), Box<dyn std::error::Error + Send + 'static> > {
                println!("Hello, world!");
                yield_now().await;
                Ok(())
            }
        };

        let expected = quote! {
            fn main() -> Result<(), Box<dyn std::error::Error + Send + 'static> > {
                let executor = ::folo::ExecutorBuilder::new().build().unwrap();
                let executor_clone = ::std::sync::Arc::clone(&executor);

                let result_rx = ::std::sync::Arc::new(::std::sync::Mutex::new(Option::<Result<(), Box<dyn std::error::Error + Send + 'static> > >::None));
                let result_tx = ::std::sync::Arc::clone(&result_rx);

                executor.spawn_on_any(async move {
                    let result = (async move {
                        println!("Hello, world!");
                        yield_now().await;
                        Ok(())
                    }).await;

                    *result_tx
                        .lock()
                        .expect("poisoned lock") = Some(result);

                    executor_clone.stop();
                });

                executor.wait();

                let result = result_rx
                    .lock()
                    .expect("posioned lock")
                    .take()
                    .expect("entrypoint terminated before returning result");

                result
            }
        };

        assert_eq!(
            entrypoint(TokenStream::new(), input, EntrypointType::Main).to_string(),
            expected.to_string()
        );
    }

    #[test]
    fn main_not_async_is_error() {
        let input = parse_quote! {
            fn main() {
                println!("Hello, world!");
            }
        };

        assert!(contains_compile_error(&entrypoint(
            TokenStream::new(),
            input,
            EntrypointType::Main,
        )));
    }
}
