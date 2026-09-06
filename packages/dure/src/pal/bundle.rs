//! Bundle of PAL facades used by command dispatch.

use std::path::PathBuf;

use crate::pal::error::PalError;
use crate::pal::local_console::LocalConsoleFacade;
use crate::pal::processes::ProcessesFacade;
use crate::pal::pseudoconsole::PseudoconsoleFacade;
use crate::pal::session_store::SessionStoreFacade;
use crate::pal::store_root::resolve_store_root;
use crate::pal::transport::TransportFacade;

/// PAL facades selected for this process.
///
/// Command dispatch holds this bundle so each command receives the store,
/// process, transport, console, and pseudoconsole facades without constructing
/// them itself. Ref: docs/implementation.md, "PAL slicing".
#[derive(Clone, Debug)]
pub(crate) struct Pal {
    pub(crate) store: SessionStoreFacade,
    pub(crate) processes: ProcessesFacade,
    pub(crate) transport: TransportFacade,
    pub(crate) console: LocalConsoleFacade,
    pub(crate) pty: PseudoconsoleFacade,
}

impl Pal {
    /// Real PAL with an optional store-root override.
    pub(crate) fn target(store_root: Option<PathBuf>) -> Result<Self, PalError> {
        let root = resolve_store_root(store_root)?;
        Ok(Self {
            store: SessionStoreFacade::target(root),
            processes: ProcessesFacade::target(),
            transport: TransportFacade::target(),
            console: LocalConsoleFacade::target(),
            pty: PseudoconsoleFacade::target(),
        })
    }
}
