//! Opaque PAL handles used by logic.
//!
//! A handle names something a PAL implementation created and still owns. The
//! integer inside is that implementation's bookkeeping, so it is visible only
//! within the PAL: code above it holds handles, and cannot invent one that no
//! implementation ever issued.

/// Listener for incoming client connections.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ListenerId(pub(crate) u64);

/// One connected client or startup channel.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ConnId(pub(crate) u64);

/// App-lifetime job object.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct JobId(pub(crate) u64);

/// Supervisor-owned pseudoconsole.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PtyId(pub(crate) u64);

/// Spawned app process waitable.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct AppId(pub(crate) u64);

/// One outstanding takeover of the local console for a relay.
///
/// Names the console state that takeover replaced, so handing the console back
/// undoes exactly what was taken and nothing a later takeover owns.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RelayLeaseId(pub(crate) u64);

// A stand-in PAL written inside a test module issues handles the same way a real
// implementation does, so it needs the same minting operation. Nothing else may
// invent a handle: a value that no implementation issued names nothing.
#[cfg(test)]
macro_rules! test_minted {
    ($($t:ty),+ $(,)?) => {
        $(
            impl $t {
                /// Mints a handle for a PAL stand-in written in a test module.
                pub(crate) const fn for_test(value: u64) -> Self {
                    Self(value)
                }
            }
        )+
    };
}

#[cfg(test)]
test_minted!(ConnId, JobId, AppId, RelayLeaseId);
