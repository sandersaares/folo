//! This example runs a server that responds to any request with "Hello, world!"

use bytes::Bytes;
use folo::{
    hyper::{FoloExecutor, FoloIo},
    net::TcpServerBuilder,
    time::{Clock, Delay},
};
use http::{header::CONTENT_TYPE, Request, Response};
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::{body::Incoming, service::service_fn};
use hyper_util::server::conn::auto::Builder;
use std::{convert::Infallible, error::Error, time::Duration};

/// Function from an incoming request to an outgoing response
async fn handle_request(
    _request: Request<Incoming>,
) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
    let response = Response::builder()
        .header(CONTENT_TYPE, "text/plain")
        .body(Full::new(Bytes::from("Hello, world!\n")).boxed())
        .expect("values provided to the builder should be valid");

    Ok(response)
}

#[folo::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let clock = Clock::new();

    let mut server = TcpServerBuilder::new()
        .port(1234.try_into().unwrap())
        .on_accept(|conn| async {
            println!("Accepted connection: {:?}", conn);

            Builder::new(FoloExecutor::new())
                .serve_connection(FoloIo::new(conn), service_fn(handle_request))
                .await?;

            folo::io::Result::Ok(())
        })
        .build()
        .await?;

    println!("Listening on http://localhost:1234");
    Delay::with_clock(&clock, Duration::from_secs(30)).await;
    println!("Shutting down the server...");

    server.stop();

    Ok(())
}
