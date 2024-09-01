use crate::util::token_stream_and_error;
use darling::{ast::NestedMeta, FromMeta};
use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

#[derive(Debug, Eq, PartialEq)]
pub enum EntrypointType {
    Main,
    Test,
}

#[derive(Debug, FromMeta)]
struct EntrypointOptions {
    /// Function to call before starting the runtime, e.g. to do global telemetry setup or similar.
    /// Note that this is called for each test in test code, so be careful what you use it for. In
    /// general, this only makes sense for the `main()` entrypoint.
    global_init_fn: Option<syn::Ident>,

    /// Function to call on each worker thread before starting the runtime, e.g. to do per-thread
    /// telemetry setup. This is the preferred way to do "global" setup, as the thread state can be
    /// initialized for each test. Note that the thread that called the entrypoint is parked and
    /// not used for running tasks, so this function is not called on the entrypoint thread.
    worker_init_fn: Option<syn::Ident>,
}

impl EntrypointOptions {
    fn parse(attr: TokenStream) -> syn::Result<Self> {
        let attr_args = NestedMeta::parse_meta_list(attr)?;
        Ok(EntrypointOptions::from_list(&attr_args)?)
    }
}

/// Implements the Folo entrypoint macro for both types of entry points (main and test functions).
pub fn entrypoint(
    attr: TokenStream,
    input: TokenStream,
    entrypoint_type: EntrypointType,
) -> TokenStream {
    let item_ast = syn::parse2::<ItemFn>(input.clone());

    let result = match item_ast {
        Ok(item) => core(item, entrypoint_type, attr),
        Err(e) => Err(e),
    };

    match result {
        Ok(r) => r,
        Err(e) => token_stream_and_error(input, e),
    }
}

fn core(
    mut item: ItemFn,
    entrypoint_type: EntrypointType,
    attr: TokenStream,
) -> Result<TokenStream, syn::Error> {
    let options = EntrypointOptions::parse(attr)?;

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

    let global_init = match options.global_init_fn {
        Some(ident) => quote! {
            #ident();
        },
        None => quote! {},
    };

    let worker_init = match options.worker_init_fn {
        Some(ident) => quote! {
            .worker_init(move || { #ident(); })
        },
        None => quote! {},
    };

    Ok(match &sig.output {
        syn::ReturnType::Default => quote! {
            #(#attrs)*
            #test_attr
            #vis #sig {
                #global_init

                let executor = ::folo::ExecutorBuilder::new()
                    #worker_init
                    .build()
                    .unwrap();
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
                    #global_init

                    let executor = ::folo::ExecutorBuilder::new()
                        #worker_init
                        .build()
                        .unwrap();
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
                let executor = ::folo::ExecutorBuilder::new()
                    .build()
                    .unwrap();
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
                let executor = ::folo::ExecutorBuilder::new()
                    .build()
                    .unwrap();
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

    #[test]
    fn main_with_init_functions() {
        let attr = parse_quote! {
            global_init_fn = setup_global,
            worker_init_fn = setup_worker,
        };

        let input = parse_quote! {
            async fn main() {
                println!("Hello, world!");
                yield_now().await;
            }
        };

        let expected = quote! {
            fn main() {
                setup_global();

                let executor = ::folo::ExecutorBuilder::new()
                    .worker_init(move || { setup_worker(); } )
                    .build()
                    .unwrap();
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
            entrypoint(attr, input, EntrypointType::Main).to_string(),
            expected.to_string()
        );
    }
}
