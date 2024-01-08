use std::io::{Read, Seek, Write};

use hdfs_client::{WriterOptions, HDFS};
use tracing::Level;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .pretty()
        .init();
    let path = "/tmp.txt";
    let mut client = HDFS::connect("127.0.0.1:9000", "root").unwrap();
    client.remove_file(path).ok();
    let opt = WriterOptions::default().block_size(1048576);
    let mut fd = opt.create(path, &mut client).unwrap();
    let buf = [b'A'; 1048576];
    fd.write_all(&buf).unwrap();
    fd.write_all(b"Hello\n").unwrap();
    {
        drop(fd);
    }
    let mut fd = client.open(path).unwrap();
    fd.seek(std::io::SeekFrom::Start(1048570)).unwrap();
    let mut s = String::new();
    fd.read_to_string(&mut s).unwrap();
    println!("{s}");
}
