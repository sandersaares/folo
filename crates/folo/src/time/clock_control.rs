// Copyright (c) Microsoft Corporation.

use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::{Duration, Instant, SystemTime};

use super::{TimerKey, Timers};

/// The clock control allows controlling the flow of time in tests.
#[derive(Clone, Debug)]
pub struct ClockControl {
    /// Clock control requires to control the flow of time across threads.
    /// For this reason, we need to use the mutex to ensure that state is consistent
    /// across all threads.
    ///
    /// Albeit not the most efficient way to handle this, this is only ever
    /// used in tests and should not be a bottleneck.
    state: Arc<Mutex<State>>,
}

impl Default for ClockControl {
    fn default() -> Self {
        Self::new()
    }
}

impl ClockControl {
    pub fn new() -> ClockControl {
        ClockControl {
            state: Arc::new(Mutex::new(State::new())),
        }
    }

    pub fn auto_advance(&self) -> Duration {
        self.with_state(|v| v.auto_advance)
    }

    pub fn set_auto_advance(&mut self, duration: Duration) {
        self.with_state(|v| v.auto_advance = duration);
    }

    pub fn advance_millis(&mut self, millis: u64) {
        self.advance(Duration::from_millis(millis));
    }

    pub fn advance(&mut self, duration: Duration) {
        self.with_state(|v| v.advance(duration));
    }

    pub(super) fn now(&self) -> SystemTime {
        self.with_state(State::now)
    }

    pub(super) fn instant_now(&self) -> Instant {
        self.with_state(State::instant_now)
    }

    pub(super) fn register_timer(&self, when: Instant, waker: Waker) -> TimerKey {
        self.with_state(|s| s.timers.register(when, waker))
    }

    pub(super) fn unregister_timer(&self, key: TimerKey) {
        self.with_state(|s| s.timers.unregister(key));
    }

    #[cfg(test)]
    pub(super) fn timers_len(&self) -> usize {
        self.with_state(|s| s.timers.len())
    }

    fn with_state<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut State) -> R,
    {
        f(&mut self
            .state
            .lock()
            .expect("acquiring lock must always succeed"))
    }
}

#[derive(Debug)]
struct State {
    instant: Instant,
    timestamp: SystemTime,
    timers: Timers,
    auto_advance: Duration,
}

impl State {
    fn new() -> Self {
        State {
            instant: Instant::now(),
            timestamp: SystemTime::UNIX_EPOCH,
            timers: Timers::new(),
            auto_advance: Duration::ZERO,
        }
    }

    fn auto_advance(&mut self) {
        self.advance(self.auto_advance);
    }

    fn advance(&mut self, duration: Duration) {
        const MESSAGE: &str =
            "advancing the clock past the maximum supported time range is not possible";

        self.instant = self.instant.checked_add(duration).expect(MESSAGE);
        self.timestamp = self.timestamp.checked_add(duration).expect(MESSAGE);
        self.timers.advance_timers(self.instant);
    }

    fn now(&mut self) -> SystemTime {
        self.auto_advance();
        self.timestamp
    }

    fn instant_now(&mut self) -> Instant {
        self.auto_advance();
        self.instant
    }
}
