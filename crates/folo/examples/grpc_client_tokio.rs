// This example demonstrates the use of raw hyper HTTP/2 client to connect to a gRPC server.
// This allows us to have the complete control over connection and lock management.
use hello_world::greeter_client::GreeterClient;
use hello_world::HelloRequest;
use hyper::body::Incoming;
use hyper::client::conn::http2::{Builder, SendRequest};
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use std::error::Error;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use tokio::net::TcpStream;
use tonic::body::BoxBody;
use tower::Service;

pub mod hello_world {
    tonic::include_proto!("greet");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let uri = "http://localhost:1234".parse::<hyper::Uri>()?;
    let client = GrpcClient::connect(uri.clone()).await?;
    let mut client = GreeterClient::with_origin(client, uri);

    for index in 0..100 {
        let message = format!("Hello {}!", index);
        let response = client
            .say_hello(HelloRequest {
                name: message,
            })
            .await?;

        println!("Response: {}", response.into_inner().message);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

// Wrapper over hyper connection management
struct GrpcClient {
    sender: SendRequest<BoxBody>,
}

impl GrpcClient {
    async fn connect(uri: hyper::Uri) -> Result<GrpcClient, Box<dyn Error>> {
        // Define the URL and host
        let host = uri.host().unwrap();
        let port = uri.port_u16().unwrap();

        // Establish a TCP connection to the host
        let tcp_stream = TcpStream::connect((host, port)).await?;

        // Provide the IO and executor to hyper.
        // When ready, we can supply our own FOLO IO and executor.
        let io = TokioIo::new(tcp_stream);
        let executor = TokioExecutor::new();
        let timer = TokioTimer::new();

        // Build the HTTP/2 connection using hyper's http2::Builder
        let (sender, connection) = Builder::new(executor)
            .timer(timer)
            .keep_alive_interval(Some(Duration::from_secs(10)))
            .keep_alive_while_idle(true)
            .handshake::<_, BoxBody>(io)
            .await?;

        // Spawn a task to poll the connection for events in the background
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok(GrpcClient { sender })
    }
}

// Implementation of this trait allows gRPC client to be used when in "GreeterClient::with_origin" constructor.
impl Service<Request<BoxBody>> for GrpcClient {
    type Response = Response<Incoming>;
    type Error = hyper::Error;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        Box::pin(self.sender.send_request(req))
    }
}
