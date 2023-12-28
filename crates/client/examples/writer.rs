use std::io::Write;

use hdfs_client::{WriterOptions, HDFS};

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .pretty()
        .init();
    let mut fs = HDFS::connect("localhost:9000", "root".to_string()).unwrap();

    let mut fd = WriterOptions::default()
        .checksum(None)
        .create("/test/hello.txt", &mut fs)
        .unwrap();
    // FIXME write 512 will hang
    fd.write_all(&vec![0].repeat(512)).unwrap();
    // for i in 0..1 {
    //     tracing::info!(seq = i);
    //     writeln!(fd, "[{i:0>2}] Hello World").unwrap();
    // }
    // fd.flush().unwrap();
}
