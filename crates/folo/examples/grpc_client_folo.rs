//! This example runs a server that responds to any request with "Hello, world!"

use std::{
    error::Error,
    net::ToSocketAddrs,
};

use folo::{
    net::TcpStream,
    rt::{spawn_sync, SynchronousTaskType},
    time::{Clock, Stopwatch},
};

#[folo::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let clock = Clock::new();

    // This can be blocking call, run it in a synchronous task
    let sockets = spawn_sync(SynchronousTaskType::Syscall, || {
        "microsoft.com:443".to_socket_addrs()
    })
    .await?;

    for socket in sockets {
        let stopwatch = Stopwatch::with_clock(&clock);

        match TcpStream::connect(socket).await {
            Ok(stream) => {
                println!(
                    "Connected: {}, Took: {}ms",
                    stream.peer_addr(),
                    stopwatch.elapsed().as_millis()
                );
            }
            Err(e) => {
                eprintln!("Failed to connect: {}", e);
            }
        }
    }

    Ok(())
}
