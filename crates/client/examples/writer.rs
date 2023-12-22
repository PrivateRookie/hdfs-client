use std::{io::Write, net::TcpStream};

use hdfs_client::{BufStream, IpcConnection, WriterOptions, FS};

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .pretty()
        .init();
    let mut fs = FS::new(
        move || {
            let stream = TcpStream::connect("127.0.0.1:9000")?;
            let stream = BufStream::new(stream);
            let ipc = IpcConnection::connect(stream, "root", None, None)?;
            Ok(ipc)
        },
        move |node| {
            let stream = TcpStream::connect(("127.0.0.1", node.xfer_port as u16))?;
            let stream = BufStream::new(stream);
            Ok(stream)
        },
    )
    .unwrap();

    let mut fd = WriterOptions::default()
        .checksum(None)
        .create("/test/hello.txt", &mut fs)
        .unwrap();
    fd.write_all(format!("Hello World\n",).as_bytes()).unwrap();
}
