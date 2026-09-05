//! Wall-clock reading for session timestamps.
//!
//! Session identity and liveness never consult the wall clock, because a clock
//! that moves cannot decide whether a process is alive. The clock is read only
//! to record when a session started and to age that timestamp for display.
//! Ref: docs/implementation.md, "Session age".

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch.
#[must_use]
pub(crate) fn unix_now_ms() -> u64 {
    unix_ms_at(SystemTime::now())
}

/// Milliseconds since the Unix epoch at `time`.
///
/// A clock set before the epoch reads as the epoch. That is not reported as a
/// failure: the value feeds a display column, so an unusable clock is worth a
/// meaningless age rather than a command that refuses to run.
#[must_use]
fn unix_ms_at(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).map_or(0, since_epoch_ms)
}

/// Converts an elapsed duration to whole milliseconds.
///
/// A duration too large to express saturates, for the same reason: an age is a
/// display value and no command fails over one.
#[must_use]
fn since_epoch_ms(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    /// 2020-01-01T00:00:00Z. Any machine running these tests is past it.
    const WELL_BEFORE_NOW_MS: u64 = 1_577_836_800_000;

    /// 2100-01-01T00:00:00Z. Any machine running these tests is short of it.
    const WELL_AFTER_NOW_MS: u64 = 4_102_444_800_000;

    #[test]
    #[cfg_attr(miri, ignore)] // Reads the host clock, which Miri's isolation refuses.
    fn the_clock_reads_a_plausible_present() {
        let now = unix_now_ms();
        assert!(now > WELL_BEFORE_NOW_MS, "{now} is not a present-day clock");
        assert!(now < WELL_AFTER_NOW_MS, "{now} is not a present-day clock");
    }

    #[test]
    fn whole_milliseconds_are_what_survive_the_conversion() {
        assert_eq!(since_epoch_ms(Duration::ZERO), 0);
        assert_eq!(since_epoch_ms(Duration::from_millis(1_500)), 1_500);
        // Sub-millisecond precision is below what an age column shows.
        assert_eq!(since_epoch_ms(Duration::from_micros(1_999)), 1);
    }

    #[test]
    fn a_duration_too_large_to_express_saturates() {
        assert_eq!(since_epoch_ms(Duration::MAX), u64::MAX);
    }

    #[test]
    fn a_clock_set_before_the_epoch_reads_as_the_epoch() {
        let before = UNIX_EPOCH
            .checked_sub(Duration::from_secs(1))
            .expect("an instant one second before the epoch is representable");
        assert_eq!(unix_ms_at(before), 0);
    }
}
