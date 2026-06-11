fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        // `bytes` fields map to `bytes::Bytes`, so chunk payloads are
        // reference-counted slices instead of copied Vec<u8>s.
        .bytes(".")
        .compile_protos(&["proto/kalpak.proto"], &["proto"])?;
    Ok(())
}
