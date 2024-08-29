use std::future::Future;
use crate::ready_after_poll::ReadyAfterPoll;

pub fn yield_now() -> impl Future<Output = ()> {
    ReadyAfterPoll::default()
}
