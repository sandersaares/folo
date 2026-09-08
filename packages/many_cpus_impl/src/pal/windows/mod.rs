mod bindings;
mod group_mask;
mod platform;
mod processor;

use bindings::*;
pub(crate) use group_mask::*;
pub use platform::*;
pub(crate) use processor::*;

type ProcessorGroupIndex = u16;
type ProcessorIndexInGroup = u8;
