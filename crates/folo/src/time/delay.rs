// Copyright (c) Microsoft Corporation.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use negative_impl::negative_impl;

use super::timers::TimerKey;
use super::Clock;

/// Asynchronously delays for the specified duration.
#[derive(Debug)]
pub struct Delay {
    // Currently scheduled timer. This value is not initialized before
    // actually calling the "Future::poll" method.
    current_timer: Option<TimerKey>,
    clock: Clock,
    duration: Duration,
}

#[negative_impl]
impl !Send for Delay {}
#[negative_impl]
impl !Sync for Delay {}

impl Delay {
    /// Creates a new delay that will finish after the specified duration.
    pub fn with_clock(clock: &Clock, duration: Duration) -> Self {
        Self {
            duration,
            current_timer: None,
            clock: clock.clone(),
        }
    }

    fn register_timer(&mut self, waker: &Waker) -> Poll<()> {
        let when = self.clock.instant_now().checked_add(self.duration);

        match when {
            Some(when) => {
                self.current_timer = Some(self.clock.register_timer(when, waker.clone()));
                Poll::Pending
            }
            None => {
                // We have moved past the maximum instant value, this delay never finishes.
                self.duration = Duration::MAX;
                self.current_timer = None;
                Poll::Pending
            }
        }
    }
}

impl Future for Delay {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match this.current_timer {
            None if this.duration == Duration::MAX => Poll::Pending,
            None if this.duration == Duration::ZERO => Poll::Ready(()),
            None => this.register_timer(cx.waker()),
            Some(key) if key.tick() <= this.clock.instant_now() => {
                this.current_timer = None;

                // Unregister timer, just in case this call was explicit
                // and not due to timers advancing.
                this.clock.unregister_timer(key);

                Poll::Ready(())
            }
            Some(_) => Poll::Pending,
        }
    }
}

