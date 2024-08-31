use crate::constants;
use std::sync::{Condvar, Mutex};

/// Carries the result from an async app entrypoint to the sync app entrypoint.
/// Used in both regular and test binaries to expose the result of the async entrypoint.
pub struct EntrypointResultBox<R> {
    result: Mutex<Option<R>>,
    cvar: Condvar,
}

impl<R> EntrypointResultBox<R> {
    pub fn new() -> Self {
        Self {
            result: Mutex::new(None),
            cvar: Condvar::new(),
        }
    }

    pub fn set(&self, result: R) {
        let mut guard = self.result.lock().expect(constants::POISONED_LOCK);

        if guard.is_some() {
            panic!("result already set");
        }

        *guard = Some(result);
        self.cvar.notify_all();
    }

    pub fn wait(&self) -> R {
        let mut guard = self.result.lock().expect(constants::POISONED_LOCK);
        while guard.is_none() {
            guard = self.cvar.wait(guard).unwrap();
        }
        guard.take().unwrap()
    }
}

impl<R> Default for EntrypointResultBox<R> {
    fn default() -> Self {
        Self::new()
    }
}
