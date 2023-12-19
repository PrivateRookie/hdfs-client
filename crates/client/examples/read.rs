use std::io::Read;

use hdfs_client::FileReader;

fn main() {
    let file = std::env::args().skip(1).next().unwrap();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .pretty()
        .init();
    let conf = hdfs_client::FSConfig {
        name_node: "namenode".into(),
        port: 9000,
        user: "root".into(),
    };
    let mut ipc = conf.connect(None).unwrap();
    let mut fd = FileReader::new(&mut ipc, file).unwrap();
    let mut s = String::new();
    fd.read_to_string(&mut s).unwrap();
    println!("{s}");
}
