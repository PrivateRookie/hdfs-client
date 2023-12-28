use std::io::Read;

use hdfs_client::{ReaderOptions, HDFS};

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .pretty()
        .init();
    let mut fs = HDFS::connect("127.0.0.1:9000", "root".to_string()).unwrap();
    let mut fd = ReaderOptions {
        checksum: Some(true),
        ..Default::default()
    }
    .open("/test/hello.txt", &mut fs)
    .unwrap();
    // let mut content = String::new();
    // fd.read_to_string(&mut content).unwrap();
    // println!("{content}");
    let mut buf = vec![];
    dbg!(fd.read_to_end(&mut buf)).unwrap();
    println!(" {}", buf.len());
}
