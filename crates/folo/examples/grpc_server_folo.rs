//! This example runs a server that responds to any request with "Hello, world!"

use bytes::Bytes;
use folo::{
    hyper::{FoloExecutor, FoloIo},
    net::TcpServerBuilder,
    time::{Clock, Delay},
};
use hello_world::{
    greeter_server::{Greeter, GreeterServer},
    HelloReply, HelloRequest,
};
use http::{Request, Response};
use http_body_util::combinators::UnsyncBoxBody;
use hyper::{body::Incoming, service::service_fn};
use hyper_util::server::conn::auto::Builder;
use std::{convert::Infallible, error::Error, time::Duration};
use tonic::{client::GrpcService, Status};

pub mod hello_world {
    tonic::include_proto!("greet");
}

/// Function from an incoming request to an outgoing response
async fn handle_request(
    request: Request<Incoming>,
) -> Result<Response<UnsyncBoxBody<Bytes, Status>>, Infallible> {
    // Let's create the GreeterServer that will handle this request with gRPC handling.
    let mut server = GreeterServer::new(MyGreeter::new());
    match server.call(request).await {
        Ok(res) => Ok(res),
        Err(_) => unreachable!()
    }
}

#[folo::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let clock = Clock::new();

    let mut server = TcpServerBuilder::new()
        .port(1234.try_into().unwrap())
        .on_accept(|conn| async {
            Builder::new(FoloExecutor::new())
                .http2_only()
                .serve_connection(FoloIo::new(conn), service_fn(handle_request))
                .await?;

            folo::io::Result::Ok(())
        })
        .build()
        .await?;

    println!("Server listening on http://localhost:1234");

    // Stop the server after N minutes
    Delay::with_clock(&clock, Duration::from_secs(300)).await;

    // Calling this is optional - just validating that it works if called early.
    // If we do not call this, it will happen automatically when the runtime shuts down workers.
    server.stop();

    Ok(())
}

#[derive(Default)]
pub struct MyGreeter {}

impl MyGreeter {
    pub fn new() -> Self {
        MyGreeter {}
    }
}

// We need to implement the behavior of the Greeter service
#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloReply>, Status> {
        let reply = hello_world::HelloReply {
            message: format!("Hello {}!", request.into_inner().name),
        };
        Ok(tonic::Response::new(reply))
    }
}
