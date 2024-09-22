/// Marker to indicate that a resource is thread-isolated - it is only valid on the current thread.
#[derive(Debug)]
pub struct Isolated {}

/// Marker to indicate that a resource is shared and may be accessed from any thread (though
/// performance may still be optimal if accessed from only a single thread).
#[derive(Debug)]
pub struct Shared {}

impl markers::IsolationMode for Shared {}
impl markers::IsolationMode for Isolated {}

pub(crate) mod markers {
    pub trait IsolationMode {}
}
