use std::io::Write;

use hdfs_client::HDFS;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .pretty()
        .init();
    let mut fs = HDFS::connect("localhost:9000", "root".to_string()).unwrap();

    let mut fd = fs.append("/test/hello.txt").unwrap();
    // FIXME write 512 will hang
    fd.write_all(include_bytes!("../../../target/test.txt"))
        .unwrap();
    // for i in 0..1 {
    //     tracing::info!(seq = i);
    //     writeln!(fd, "[{i:0>2}] Hello World").unwrap();
    // }
    // fd.flush().unwrap();
}
