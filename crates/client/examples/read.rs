use std::io::Read;

use hdfs_client::HDFS;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .pretty()
        .init();
    let mut fs = HDFS::connect("127.0.0.1:9000", "root").unwrap();
    let mut fd = fs.open("/test/hello.txt").unwrap();
    let mut content = String::new();
    fd.read_to_string(&mut content).unwrap();
    println!("{content}");
}
