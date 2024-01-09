use std::io::Write;

use hdfs_client::HDFS;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .pretty()
        .init();
    let mut fs = HDFS::connect("localhost:9000", "root").unwrap();

    let mut fd = fs.create("/test/hello.txt").unwrap();
    fd.write_all(b"hello").unwrap();
    // for i in 0..1 {
    //     tracing::info!(seq = i);
    //     writeln!(fd, "[{i:0>2}] Hello World").unwrap();
    // }
    // fd.flush().unwrap();
}
