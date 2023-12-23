use std::{io::Read, net::TcpStream};

use hdfs_client::{hrpc::HRpc, BufStream, ReaderOptions, FS};

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .pretty()
        .init();
    let mut fs = FS::new(
        move || {
            let stream = TcpStream::connect("127.0.0.1:9000")?;
            let stream = BufStream::new(stream);
            let ipc = HRpc::connect(stream, "root", None, None)?;
            Ok(ipc)
        },
        move |node| {
            let stream = TcpStream::connect(("127.0.0.1", node.xfer_port as u16))?;
            let stream = BufStream::new(stream);
            Ok(stream)
        },
    )
    .unwrap();
    let mut fd = ReaderOptions {
        checksum: Some(true),
        ..Default::default()
    }
    .open("/test/hello.txt", &mut fs)
    .unwrap();
    let mut content = String::new();
    fd.read_to_string(&mut content).unwrap();
    println!("{content}");
}
