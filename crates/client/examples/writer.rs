use std::io::Write;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .pretty()
        .init();
    let conf = hdfs_client::FSConfig {
        name_node: "namenode".into(),
        port: 9000,
        user: "root".into(),
    };

    let ipc = conf.connect(None).unwrap();
    let mut fd = hdfs_client::FileWriter::create(ipc, "/test/hello.txt").unwrap();
    fd.write_all(b"Hello\n").unwrap();
}
