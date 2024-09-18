use folo_proc_macros_impl::folo_entrypoint::EntrypointType;
use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn __macro_main(attr: TokenStream, item: TokenStream) -> TokenStream {
    folo_proc_macros_impl::folo_entrypoint::entrypoint(
        attr.into(),
        item.into(),
        EntrypointType::Main,
    )
    .into()
}

#[proc_macro_attribute]
pub fn __macro_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    folo_proc_macros_impl::folo_entrypoint::entrypoint(
        attr.into(),
        item.into(),
        EntrypointType::Test,
    )
    .into()
}

#[proc_macro_attribute]
pub fn __macro_linked_object(attr: TokenStream, item: TokenStream) -> TokenStream {
    folo_proc_macros_impl::linked_object::entrypoint(attr.into(), item.into()).into()
}
