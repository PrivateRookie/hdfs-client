use std::io::Write;

use hdfs_client::HDFS;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .pretty()
        .init();
    let mut fs = HDFS::connect("localhost:9000", "root".to_string()).unwrap();

    let mut fd = fs.append("/test/hello.txt").unwrap();
    fd.write_all(b"hello").unwrap();
}
