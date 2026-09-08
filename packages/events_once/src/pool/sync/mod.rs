//! Thread-safe event pool implementation.

mod pool;
#[cfg(test)]
mod tests;

pub use pool::*;
