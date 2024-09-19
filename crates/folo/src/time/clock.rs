// Copyright (c) Microsoft Corporation.

use std::{task::Waker, time::SystemTime};
use std::time::Instant;

#[cfg(feature = "fakes")]
use super::clock_control::ClockControl;
use super::{TimerKey, LOCAL_TIMERS};

#[derive(Debug, Clone)]
pub struct Clock {
    _private: (),

    #[cfg(feature = "fakes")]
    clock_control: Option<ClockControl>,
}

impl Clock {
    pub fn new() -> Self {
        Clock::new_core()
    }

    // This method is mutated, but cannot be tested due to tests
    // running with the "fakes" feature.
    #[cfg(not(feature = "fakes"))]
    fn new_core() -> Self {
        Self { _private: () }
    }

    #[cfg(feature = "fakes")]
    fn new_core() -> Self {
        Self {
            _private: (),
            clock_control: None
        }
    }

    #[cfg(feature = "fakes")]
    pub fn with_control(clock_control: &ClockControl) -> Self {
        let clone = clock_control.clone();

        Self {
            _private: (),
            clock_control: Some(clone.clone())
        }
    }

    pub fn now(&self) -> SystemTime {
        // This method is mutated, but cannot be tested due to tests
        // running with the "fakes" feature.
        #[cfg(not(feature = "fakes"))]
        fn now_core(_clock: &Clock) -> SystemTime {
            SystemTime::now()
        }

        #[cfg(feature = "fakes")]
        fn now_core(clock: &Clock) -> SystemTime {
            match &clock.clock_control {
                None => SystemTime::now(),
                Some(control) => control.now(),
            }
        }

        now_core(self)
    }

    /// Retrieves the current [`Instant`] time.
    pub(crate) fn instant_now(&self) -> Instant {
        // This method is mutated, but cannot be tested due to tests
        // running with the "fakes" feature.
        #[cfg(not(feature = "fakes"))]
        fn now_core(_clock: &Clock) -> Instant {
            Instant::now()
        }

        #[cfg(feature = "fakes")]
        fn now_core(clock: &Clock) -> Instant {
            match &clock.clock_control {
                None => Instant::now(),
                Some(control) => control.instant_now(),
            }
        }

        now_core(self)
    }

    // This method is mutated, but cannot be tested due to tests
    // running with the "fakes" feature.
    #[cfg(not(feature = "fakes"))]
    pub(super) fn register_timer(&mut self, when: Instant, waker: Waker) -> TimerKey {
        LOCAL_TIMERS.with_borrow_mut(|t| t.register(when, waker))
    }

    #[cfg(feature = "fakes")]
    pub(super) fn register_timer(&mut self, when: Instant, waker: Waker) -> TimerKey {
        match &self.clock_control {
            None => LOCAL_TIMERS.with_borrow_mut(|t| t.register(when, waker)),
            Some(control) => control.register_timer(when, waker),
        }
    }

    // This method is mutated, but cannot be tested due to tests
    // running with the "fakes" feature.
    #[cfg(not(feature = "fakes"))]
    pub(super) fn unregister_timer(&mut self, key: TimerKey) {
        LOCAL_TIMERS.with_borrow_mut(|t| t.unregister(key));
    }

    #[cfg(feature = "fakes")]
    pub(super) fn unregister_timer(&mut self, key: TimerKey) {
        match &self.clock_control {
            None => LOCAL_TIMERS.with_borrow_mut(|t| t.unregister(key)),
            Some(control) => control.unregister_timer(key),
        }
    }
}

impl Default for Clock {
    fn default() -> Self {
        Clock::new()
    }
}
