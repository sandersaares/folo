fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .build_transport(false)
        .compile(&["proto/greet.proto"], &["proto"])?;

    Ok(())
}
