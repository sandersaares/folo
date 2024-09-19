//! This example runs a server that responds to any request with "Hello, world!"
use hello_world::{
    greeter_server::{Greeter, GreeterServer},
    HelloReply, HelloRequest,
};
use std::error::Error;
use tonic::{transport::Server, Status};

pub mod hello_world {
    tonic::include_proto!("greet");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let addr = "0.0.0.0:1234".parse()?;
    let greeter = MyGreeter::default();
    println!("Listening on http://{addr}");

    Server::builder()
        .concurrency_limit_per_connection(1)
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await?;

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
