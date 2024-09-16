// Copyright (c) Microsoft Corporation.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::mem;
use std::task::Waker;
use std::time::{Duration, Instant};

/// Unique identifier for a timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct TimerKey {
    tick: Instant,

    /// Discriminator that ensures two timer IDs with the same instant can be created.
    discriminator: u32,
}

impl TimerKey {
    fn new(tick: Instant, id: u32) -> TimerKey {
        TimerKey {
            tick,
            discriminator: id,
        }
    }

    /// Determines when the timer will fire.
    pub fn tick(&self) -> Instant {
        self.tick
    }
}

/// The default resolution of our timers. Timers with lover resolution will be rounded up to this value.
pub(crate) const TIMER_RESOLUTION: Duration = Duration::from_millis(1);

thread_local! {
    pub(super) static LOCAL_TIMERS: RefCell<Timers> = RefCell::new(Timers::new());
}

/// Processes all thread-local timers that are ready to fire.
pub(crate) fn advance_local_timers(now: Instant) {
    LOCAL_TIMERS.with_borrow_mut(|timer_manager| timer_manager.advance_timers(now));
}

/// The management of one-shot timers, inspired by [glommio runtime](https://github.com/DataDog/glommio/blob/d3f6e7a2ee7fb071ada163edcf90fc3286424c31/glommio/src/reactor.rs#L80)
///
/// The timers managed by this collection are one-shot, meaning after they fire they won't be fired again.
#[derive(Debug)]
pub(super) struct Timers {
    /// An ordered map of registered timers.
    ///
    /// Timers are in the order in which they fire.
    /// The [`Waker`] represents the task awaiting the timer.
    wakers: BTreeMap<TimerKey, Waker>,
    last_discriminator: u32,
}

impl Timers {
    pub fn new() -> Timers {
        Timers {
            last_discriminator: 0,
            wakers: BTreeMap::new(),
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.wakers.len()
    }

    #[cfg(test)]
    fn contains(&self, id: TimerKey) -> bool {
        self.wakers.contains_key(&id)
    }

    pub fn register(&mut self, when: Instant, waker: Waker) -> TimerKey {
        // We can wrap discriminator because it's only used to distinguish timers with the same instant
        // and actual value can start from 0 again.
        self.last_discriminator = self.last_discriminator.wrapping_add(1);
        let key = TimerKey::new(when, self.last_discriminator);

        self.wakers.insert(key, waker);

        key
    }

    pub fn unregister(&mut self, id: TimerKey) {
        self.wakers.remove(&id);
    }

    /// Advance timers that are ready to be woken.
    ///
    /// Later, the signature of this method can be easily expanded to return more
    /// information about the timers that fired and when the next timer fires.
    pub fn advance_timers(&mut self, now: Instant) {
        // We are adding 1ns to the instant to ensure that even timers whose deadline is the current
        // instant are advanced. This is required because of the way BTreeMap::split_off works; it does
        // not include the keys that are equal to the split key. Adding 1ns to the value makes this work.
        let adjusted_now = now.checked_add(Duration::from_nanos(1)).unwrap_or(now);

        // Split timers into ready and pending timers.
        let pending = self.wakers.split_off(&TimerKey::new(adjusted_now, 0));
        let ready = mem::replace(&mut self.wakers, pending);

        // Invoke the wakers for timers that ticked.
        for (_, waker) in ready {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::task::noop_waker;

    use super::*;
    use crate::time::Clock;

    #[test]
    fn advance_local_timers_ok() {
        let anchor = Instant::now();
        let when = anchor + Duration::from_secs(2);
        let finished = anchor + Duration::from_secs(3);

        LOCAL_TIMERS.with_borrow_mut(|t| t.register(when, noop_waker()));

        let len = timers_len();
        advance_local_timers(anchor + Duration::from_secs(1));
        assert_eq!(timers_len(), len);

        advance_local_timers(finished);
        assert_eq!(timers_len(), len - 1);
    }

    #[test]
    fn two_timers_same_instant() {
        let anchor = Instant::now();
        let when = anchor + Duration::from_secs(2);

        let key1 = LOCAL_TIMERS.with_borrow_mut(|t| t.register(when, noop_waker()));
        let key2 = LOCAL_TIMERS.with_borrow_mut(|t| t.register(when, noop_waker()));

        assert_ne!(key1, key2);

        let len = timers_len();
        advance_local_timers(when + Duration::from_secs(1));
        assert_eq!(timers_len(), len - 2);
    }

    #[test]
    fn advance_local_timers_ensure_order() {
        let anchor = Instant::now();
        let timer1 = anchor + Duration::from_secs(1);
        let timer2 = anchor + Duration::from_secs(2);

        let id1 = LOCAL_TIMERS.with_borrow_mut(|t| t.register(timer1, noop_waker()));
        let id2 = LOCAL_TIMERS.with_borrow_mut(|t| t.register(timer2, noop_waker()));

        let len = timers_len();
        advance_local_timers(timer1 + Duration::from_nanos(1));
        assert_eq!(timers_len(), len - 1);
        assert!(!LOCAL_TIMERS.with_borrow(|t| t.contains(id1)));

        advance_local_timers(timer2 + Duration::from_nanos(1));
        assert_eq!(timers_len(), len - 2);

        assert!(!LOCAL_TIMERS.with_borrow(|t| t.contains(id2)));
    }

    #[test]
    fn timer_resolution_ensure_correct_value() {
        assert_eq!(TIMER_RESOLUTION, Duration::from_millis(1));
    }

    #[test]
    fn register_timer_with_clock() {
        let mut clock = Clock::new();
        let id = clock.register_timer(Instant::now(), noop_waker());
        assert!(LOCAL_TIMERS.with(|t| t.borrow().contains(id)));
    }

    #[test]
    fn unregister_timer_with_clock() {
        let mut clock = Clock::new();
        let len = LOCAL_TIMERS.with_borrow(Timers::len);
        let id = clock.register_timer(Instant::now(), noop_waker());
        clock.unregister_timer(id);
        assert_eq!(LOCAL_TIMERS.with_borrow(Timers::len), len);
    }

    #[test]
    fn unregister_ok() {
        let mut timers = Timers::new();
        let id = timers.register(Instant::now(), noop_waker());

        assert!(timers.contains(id));
        timers.unregister(id);
        assert!(!timers.contains(id));
    }

    fn timers_len() -> usize {
        LOCAL_TIMERS.with_borrow(Timers::len)
    }
}
