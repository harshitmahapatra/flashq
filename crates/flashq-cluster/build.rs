fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Ensure a working `protoc` is available (vendored) to avoid external toolchain dependency.
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }
    // Provide include path for well-known types like google.protobuf.Empty
    let include_path = protoc_bin_vendored::include_path()?;
    unsafe {
        std::env::set_var("PROTOC_INCLUDE", include_path.as_os_str());
    }

    // Compile the cluster gRPC/protobuf definitions into Rust code using tonic/prost.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/cluster.proto"],
            &["proto", &include_path.to_string_lossy()],
        )?;
    Ok(())
}
