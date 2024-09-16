// This example explores the time primitives in the FOLO runtime.
use folo::time::{Delay, PeriodicTimer, Stopwatch};
use futures::StreamExt;
use std::{error::Error, time::Duration};

#[folo::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt::init();
    let clock = folo::time::Clock::new();

    println!("Waiting for 2 seconds...");
    Delay::with_clock(&clock, Duration::from_secs(2)).await;
    println!("Waiting for 2 seconds...done");

    // Create a periodic timer that fires every 1 second.
    let periodic = PeriodicTimer::with_clock(&clock, Duration::from_secs(1));
    periodic
        .take(3)
        .for_each(|()| async {
            println!("Periodic timer fired!");
        })
        .await;

    let watch = Stopwatch::with_clock(&clock);
    Delay::with_clock(&clock, Duration::from_secs(2)).await;
    println!("Operation took {}ms", watch.elapsed().as_millis());

    Ok(())
}
