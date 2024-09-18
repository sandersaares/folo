use windows::Win32::System::SystemInformation::GetTickCount64;

/// A cheaper version of `Instant` that is capable of representing time with less precision. The
/// granularity is typically around 15-20 ms, so no point trying to see differences below that.
/// 
/// TODO: Some thread local variable we update once per tick might be even better for performance,
/// so we can avoid the FFI call (which is fast but still expensive compared to a variable read).
#[derive(Clone, Copy, Debug)]
pub struct LowPrecisionInstant {
    value: u64,
}

impl LowPrecisionInstant {
    pub fn now() -> Self {
        LowPrecisionInstant {
            // SAFETY: Nothing unsafe about this, just an FFI call.
            value: unsafe { GetTickCount64() },
        }
    }

    pub fn duration_since(&self, earlier: LowPrecisionInstant) -> std::time::Duration {
        std::time::Duration::from_millis(self.value - earlier.value)
    }

    pub fn elapsed(&self) -> std::time::Duration {
        LowPrecisionInstant::now().duration_since(*self)
    }
}
