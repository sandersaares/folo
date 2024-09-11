use crate::util::token_stream_and_error;
use darling::{ast::NestedMeta, FromMeta};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{token::Async, ItemFn};

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

    /// Limits the number of processors the runtime will use. Just for debugging purposes - not
    /// flexible enough to be used as a resource management tool.
    max_processors: Option<usize>,

    /// If set, emits a dump of collected worker metrics to stdout when the runtime stops.
    #[darling(default)]
    print_metrics: bool,
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

    let mut inner_sig = sig.clone();
    inner_sig.ident = format_ident!("__inner_{}", sig.ident);
    inner_sig.asyncness = Some(Async::default());

    let inner_ident = &inner_sig.ident;

    // We emit the body of the entrypoint as a separate function to ensure the correct return
    // type gets inferred.
    let inner = quote! {
        #inner_sig #body
    };

    let metrics_init = match options.print_metrics {
        true => quote! {
            .metrics_tx(__entrypoint_metrics_collector.tx())
        },
        false => quote! {},
    };

    let max_processors = match options.max_processors {
        Some(n) => quote! {
            .max_processors(#n)
        },
        None => quote! {},
    };

    Ok(match &sig.output {
        syn::ReturnType::Default => quote! {
            #(#attrs)*
            #test_attr
            #vis #sig {
                #global_init

                let __entrypoint_metrics_collector = ::folo::__private::MetricsCollector::new();

                let __entrypoint_runtime = ::folo::rt::RuntimeBuilder::new()
                    #worker_init
                    #metrics_init
                    #max_processors
                    .build()
                    .unwrap();
                let __entrypoint_runtime_clone = __entrypoint_runtime.clone();

                __entrypoint_runtime.spawn_on_any(|| async move {
                    #inner_ident().await;
                    __entrypoint_runtime_clone.stop();
                });

                __entrypoint_runtime.wait();
            }

            #inner
        },
        syn::ReturnType::Type(_, ty) => quote! {
            #(#attrs)*
            #test_attr
            #vis #sig {
                #global_init

                let __entrypoint_metrics_collector = ::folo::__private::MetricsCollector::new();

                let __entrypoint_runtime = ::folo::rt::RuntimeBuilder::new()
                    #worker_init
                    #metrics_init
                    #max_processors
                    .build()
                    .unwrap();
                let __entrypoint_runtime_clone = __entrypoint_runtime.clone();

                let __entrypoint_result_rx = ::std::sync::Arc::new(::std::sync::Mutex::new(Option::<#ty>::None));
                let __entrypoint_result_tx = ::std::sync::Arc::clone(&__entrypoint_result_rx);

                __entrypoint_runtime.spawn_on_any(|| async move {
                    let __entrypoint_result = #inner_ident().await;

                    *__entrypoint_result_tx
                        .lock()
                        .expect("poisoned lock") = Some(__entrypoint_result);

                        __entrypoint_runtime_clone.stop();
                });

                // If the test fails, generally we panic from here because we detect that a
                // worker thread panicked.
                __entrypoint_runtime.wait();

                // Reaching this point is highly unlikely if a test fails - at least no
                // currently known execution path takes us here. Only used for success case.
                let __entrypoint_result = __entrypoint_result_rx
                    .lock()
                    .expect("posioned lock")
                    .take()
                    .expect("entrypoint terminated before returning result");

                __entrypoint_result
            }

            #inner
        },
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
                let __entrypoint_metrics_collector = ::folo::__private::MetricsCollector::new();

                let __entrypoint_runtime = ::folo::rt::RuntimeBuilder::new()
                    .build()
                    .unwrap();
                let __entrypoint_runtime_clone = __entrypoint_runtime.clone();

                __entrypoint_runtime.spawn_on_any(|| async move {
                    __inner_main().await;
                    __entrypoint_runtime_clone.stop();
                });

                __entrypoint_runtime.wait();
            }

            async fn __inner_main() {
                println!("Hello, world!");
                yield_now().await;
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
                let __entrypoint_metrics_collector = ::folo::__private::MetricsCollector::new();

                let __entrypoint_runtime = ::folo::rt::RuntimeBuilder::new()
                    .build()
                    .unwrap();
                let __entrypoint_runtime_clone = __entrypoint_runtime.clone();

                let __entrypoint_result_rx = ::std::sync::Arc::new(::std::sync::Mutex::new(Option::<Result<(), Box<dyn std::error::Error + Send + 'static> > >::None));
                let __entrypoint_result_tx = ::std::sync::Arc::clone(&__entrypoint_result_rx);

                __entrypoint_runtime.spawn_on_any(|| async move {
                    let __entrypoint_result = __inner_main().await;

                    *__entrypoint_result_tx
                        .lock()
                        .expect("poisoned lock") = Some(__entrypoint_result);

                    __entrypoint_runtime_clone.stop();
                });

                __entrypoint_runtime.wait();

                let __entrypoint_result = __entrypoint_result_rx
                    .lock()
                    .expect("posioned lock")
                    .take()
                    .expect("entrypoint terminated before returning result");

                __entrypoint_result
            }

            async fn __inner_main() -> Result<(), Box<dyn std::error::Error + Send + 'static> > {
                println!("Hello, world!");
                yield_now().await;
                Ok(())
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

                let __entrypoint_metrics_collector = ::folo::__private::MetricsCollector::new();

                let __entrypoint_runtime = ::folo::rt::RuntimeBuilder::new()
                    .worker_init(move || { setup_worker(); } )
                    .build()
                    .unwrap();
                let __entrypoint_runtime_clone = __entrypoint_runtime.clone();

                __entrypoint_runtime.spawn_on_any(|| async move {
                    __inner_main().await;

                    __entrypoint_runtime_clone.stop();
                });

                __entrypoint_runtime.wait();
            }

            async fn __inner_main() {
                println!("Hello, world!");
                yield_now().await;
            }
        };

        assert_eq!(
            entrypoint(attr, input, EntrypointType::Main).to_string(),
            expected.to_string()
        );
    }

    #[test]
    fn main_with_metrics() {
        let attr = parse_quote! {
            print_metrics
        };

        let input = parse_quote! {
            async fn main() {
                println!("Hello, world!");
                yield_now().await;
            }
        };

        let expected = quote! {
            fn main() {
                let __entrypoint_metrics_collector = ::folo::__private::MetricsCollector::new();

                let __entrypoint_runtime = ::folo::rt::RuntimeBuilder::new()
                    .metrics_tx(__entrypoint_metrics_collector.tx())
                    .build()
                    .unwrap();
                let __entrypoint_runtime_clone = __entrypoint_runtime.clone();

                __entrypoint_runtime.spawn_on_any(|| async move {
                    __inner_main().await;

                    __entrypoint_runtime_clone.stop();
                });

                __entrypoint_runtime.wait();
            }

            async fn __inner_main() {
                println!("Hello, world!");
                yield_now().await;
            }
        };

        assert_eq!(
            entrypoint(attr, input, EntrypointType::Main).to_string(),
            expected.to_string()
        );
    }

    #[test]
    fn main_with_max_processors() {
        let attr = parse_quote! {
            max_processors = 3
        };

        let input = parse_quote! {
            async fn main() {
                println!("Hello, world!");
                yield_now().await;
            }
        };

        let expected = quote! {
            fn main() {
                let __entrypoint_metrics_collector = ::folo::__private::MetricsCollector::new();

                let __entrypoint_runtime = ::folo::rt::RuntimeBuilder::new()
                    .max_processors(3usize)
                    .build()
                    .unwrap();
                let __entrypoint_runtime_clone = __entrypoint_runtime.clone();

                __entrypoint_runtime.spawn_on_any(|| async move {
                    __inner_main().await;

                    __entrypoint_runtime_clone.stop();
                });

                __entrypoint_runtime.wait();
            }

            async fn __inner_main() {
                println!("Hello, world!");
                yield_now().await;
            }
        };

        assert_eq!(
            entrypoint(attr, input, EntrypointType::Main).to_string(),
            expected.to_string()
        );
    }

    #[test]
    fn test_returns_default() {
        let input = parse_quote! {
            async fn my_test() {
                yield_now().await;
                assert_eq!(2 + 2, 4);
            }
        };

        let expected = quote! {
            #[test]
            fn my_test() {
                let __entrypoint_metrics_collector = ::folo::__private::MetricsCollector::new();
                
                let __entrypoint_runtime = ::folo::rt::RuntimeBuilder::new()
                    .build()
                    .unwrap();
                let __entrypoint_runtime_clone = __entrypoint_runtime.clone();

                __entrypoint_runtime.spawn_on_any(|| async move {
                    __inner_my_test().await;

                    __entrypoint_runtime_clone.stop();
                });

                __entrypoint_runtime.wait();
            }

            async fn __inner_my_test() {
                yield_now().await;
                assert_eq!(2 + 2, 4);
            }
        };

        assert_eq!(
            entrypoint(TokenStream::new(), input, EntrypointType::Test).to_string(),
            expected.to_string()
        );
    }
}
