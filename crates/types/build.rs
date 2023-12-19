#[allow(unused_mut)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut compiler = tonic_build::configure();
    #[cfg(feature = "serde")]
    {
        compiler =
            compiler.message_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    }
    #[cfg(feature = "valuable")]
    {
        compiler = compiler.message_attribute(".", "#[derive(valuable::Valuable)]");
    }
    compiler.compile(&["./proto/ClientNamenodeProtocol.proto"], &["./proto"])?;
    Ok(())
}
