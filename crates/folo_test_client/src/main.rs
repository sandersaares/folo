use clap::Parser;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{self, Duration};

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    address: String,
    #[clap(short, long)]
    port: u16,
    #[clap(short, long)]
    connections: usize,
}

async fn handle_connection(mut stream: TcpStream, throughput_bytes: Arc<AtomicU64>) {
    let mut buf = vec![0; 1024 * 1024];

    loop {
        // Send data
        if let Err(_) = stream.write_all(&buf).await {
            break;
        }

        // Receive data
        let total_bytes = match stream.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n as u64,
            Err(_) => break,
        };

        throughput_bytes.fetch_add(total_bytes, Ordering::Relaxed);
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();
    let address = format!("{}:{}", args.address, args.port);
    let throughput_bytes = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for _ in 0..args.connections {
        let stream = TcpStream::connect(&address).await?;
        let throughput = Arc::clone(&throughput_bytes);
        handles.push(tokio::spawn(handle_connection(stream, throughput)));
    }

    let throughput_bytes = Arc::clone(&throughput_bytes);
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;

            let interval_bytes = throughput_bytes.swap(0, Ordering::Relaxed);
            let mbps = (interval_bytes * 8 / 5) as f64 / 1_000_000.0;

            println!("Throughput: {:.2} Mbps", mbps);
        }
    });

    for handle in handles {
        handle.await.unwrap();
    }

    Ok(())
}
