mod abstractions;
mod facade;

pub(crate) use abstractions::*;
pub(crate) use facade::*;

std::cfg_select! {
    all(target_os = "linux", not(miri)) => {
        mod linux;
        pub(crate) use linux::*;
    }
    all(windows, not(miri)) => {
        mod windows;
        pub(crate) use windows::*;
    }
    _ => {
        pub(crate) use rust::*;
    }
}

#[cfg(test)]
mod mock;
#[cfg(test)]
pub(crate) use mock::*;

// We do not cfg(miri) this simply because that disables IDE editor support, which is annoying.
mod rust;
