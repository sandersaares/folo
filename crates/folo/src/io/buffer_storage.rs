use std::pin::Pin;

/// Methods to obtain the backing storage of a buffer (not limited to the active region).
/// Used internally to implement higher-level primitive operations on the active region.
pub(super) trait Storage {
    fn as_slice(&self) -> Pin<&[u8]>;
    fn as_mut_slice(&mut self) -> Pin<&mut [u8]>;
}
