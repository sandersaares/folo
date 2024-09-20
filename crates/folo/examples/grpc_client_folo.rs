//! This example runs a server that responds to any request with "Hello, world!"

use std::{error::Error, net::SocketAddr};

use folo::net::TcpStream;

#[folo::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let ip: SocketAddr = "127.0.0.1:1234".parse().unwrap();

    match TcpStream::connect(ip).await {
        Ok(stream) => {
            println!("Connected: {}", stream.peer_addr());
        }
        Err(e) => {
            eprintln!("Failed to connect: {}", e);
        }
    } 

    Ok(())
}

