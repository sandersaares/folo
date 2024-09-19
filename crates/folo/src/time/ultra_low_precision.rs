use std::cell::RefCell;
use windows::Win32::System::SystemInformation::GetTickCount64;

/// A far cheaper version of `Instant` that is capable of representing time with less precision. The
/// granularity is typically around 15-20 ms or once per async task engine tick, whichever is less,
/// so no point trying to see differences below that.
#[derive(Clone, Copy, Debug)]
pub struct UltraLowPrecisionInstant {
    value: u64,
}

impl UltraLowPrecisionInstant {
    pub fn now() -> Self {
        let value = match MODE.with(|m| *m.borrow()) {
            // SAFETY: Nothing unsafe, just harmless FFI.
            Mode::LowPrecision => unsafe { GetTickCount64() },
            Mode::UltraLowPrecision(value) => value,
        };

        UltraLowPrecisionInstant { value }
    }

    pub fn duration_since(&self, earlier: UltraLowPrecisionInstant) -> std::time::Duration {
        if self.value < earlier.value {
            // This is possible because different threads update clock at different moments
            return std::time::Duration::ZERO;
        }

        std::time::Duration::from_millis(self.value - earlier.value)
    }

    pub fn elapsed(&self) -> std::time::Duration {
        UltraLowPrecisionInstant::now().duration_since(*self)
    }

    /// Updates the instant the current thread observes to match the system clock.
    /// The expectation is that the async task runtime calls this once per cycle.
    pub fn update() {
        let value = unsafe { GetTickCount64() };
        MODE.with(|m| *m.borrow_mut() = Mode::UltraLowPrecision(value));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// We are in low-precision mode by default and just fall back to the low-precision instant
    /// because we might not even be on an async task engine thread that can update us.
    LowPrecision,

    /// We appear to be on an async worker thread because someone is updating our value. We use
    /// the explicit value that has been set for us and do not poll the system clock at all.
    UltraLowPrecision(u64),
}

thread_local! {
    static MODE: RefCell<Mode> = const { RefCell::new(Mode::LowPrecision) };
}
