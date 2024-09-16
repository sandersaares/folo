// Copyright (c) Microsoft Corporation.

use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::Stream;
use negative_impl::negative_impl;

use super::timers::TimerKey;
use super::{Clock, TIMER_RESOLUTION};

/// A timer that periodically ticks.
#[derive(Debug)]
pub struct PeriodicTimer {
    period: Duration,
    clock: Clock,
    // Currently scheduled timer. This value is not initialized before
    // actually calling the "Stream::poll_next" method.
    current_timer: Option<TimerKey>,
}

#[negative_impl]
impl !Send for PeriodicTimer {}
#[negative_impl]
impl !Sync for PeriodicTimer {}

impl PeriodicTimer {
    /// Creates a timer that fires periodically.
    pub fn with_clock(clock: &Clock, period: Duration) -> PeriodicTimer {
        let mut period = period;

        if period < TIMER_RESOLUTION {
            period = TIMER_RESOLUTION;
        }

        PeriodicTimer {
            // The timer is not registered yet, it will be done on the first
            // call to the Stream::poll_next.
            current_timer: None,
            period,
            clock: clock.clone(),
        }
    }

    fn register_timer(&mut self, waker: Waker) {
        match self.clock.instant_now().checked_add(self.period) {
            Some(when) => {
                self.current_timer = Some(self.clock.register_timer(when, waker));
            }
            None => {
                // The timer would tick so far in the future that we can assume
                // it never fires. For this reason, there is no point of even registering id.
                // The period is set to Duration::MAX to prevent further registrations.
                self.period = Duration::MAX;
            }
        }
    }
}

impl Stream for PeriodicTimer {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.period == Duration::MAX {
            return Poll::Pending;
        }

        match this.current_timer {
            Some(key) if key.tick() <= this.clock.instant_now() => {
                // Reset the timer. It will be registered again in the next poll.
                this.current_timer = None;

                // Unregister timer, just in case this call was explicit and not due to
                // timers advancing.
                this.clock.unregister_timer(key);

                Poll::Ready(Some(()))
            }
            // Timer is registered and will fire later in the future.
            Some(_) => Poll::Pending,

            // Timer is not registered yet, let's register it.
            // The registration is lazy, when someone polls the future. This means
            // that the work between two ticks is not taken into account when scheduling
            // the next tick. When thread is busy, the timer may tick later than expected.
            None => {
                this.register_timer(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
