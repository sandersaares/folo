// Copyright (c) Microsoft Corporation.

use std::time::{Duration, Instant};

use super::Clock;

/// A stopwatch that facilitates the measurement of elapsed time.
#[derive(Debug)]
pub struct Stopwatch {
    start: Instant,

    #[cfg(feature = "fakes")]
    clock: Clock,
}

impl Stopwatch {
    /// Creates a high-accuracy stopwatch that simplifies measurements of time.
    pub fn with_clock(clock: &Clock) -> Self {
        Self {
            start: clock.instant_now(),
            #[cfg(feature = "fakes")]
            clock: clock.clone(),
        }
    }

    /// Returns the elapsed time since the stopwatch was created.
    pub fn elapsed(&self) -> Duration {
        // This method is mutated, but cannot be tested due to tests
        // running with the "fakes" feature.
        #[cfg(not(feature = "fakes"))]
        fn elapsed_core(watch: &Stopwatch) -> Duration {
            watch.start.elapsed()
        }

        #[cfg(feature = "fakes")]
        fn elapsed_core(watch: &Stopwatch) -> Duration {
            watch
                .clock
                .instant_now()
                .saturating_duration_since(watch.start)
        }

        elapsed_core(self)
    }
}
