/// Determines what item handling is permitted when a collection is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropPolicy {
    /// The collection will drop its items when it is dropped.
    MayDropItems,

    /// The collection will panic if it still contains items when it is dropped.
    MustNotDropItems,
}
