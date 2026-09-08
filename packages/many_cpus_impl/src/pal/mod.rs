//! Platform Abstraction Layer (PAL). This is private API, though (hidden) `pub` in parts to allow
//! benchmark code to bypass public API layers for more accurate benchmarking.

mod abstractions;
pub(crate) use abstractions::*;

mod facade;
pub(crate) use facade::*;

std::cfg_select! {
    all(target_os = "linux", not(miri)) => {
        mod linux;
        pub(crate) use linux::*;
    }
    all(windows, not(miri)) => {
        mod windows;
        pub use windows::*;
    }
    _ => {
        pub(crate) use fallback::*;
    }
}

// The fallback module is compiled in test mode on all platforms, under Miri, and as the primary
// implementation on unsupported platforms. However, we only glob-import it when it is the primary
// implementation (i.e. on unsupported platforms or under Miri). On supported platforms in test
// mode, it must be accessed via the explicit path `fallback::` to avoid ambiguity with the
// platform-specific implementation.
#[cfg(any(test, miri, not(any(target_os = "linux", windows))))]
pub(crate) mod fallback;
