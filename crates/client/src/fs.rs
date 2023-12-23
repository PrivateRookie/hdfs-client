use std::{
    io::{self, BufReader, BufWriter, Read, Write},
    net::TcpStream,
    sync::Arc,
};

use crate::{hrpc::HRpc, HDFSError};
use hdfs_types::{common::RpcCallerContextProto, hdfs::DatanodeIdProto};


const CLIENT_NAME: &str = "hdfs-rust-client";

mod writer;
pub use writer::{FileWriter, WriterOptions};
mod reader;
pub use reader::{FileReader, ReaderOptions};

/// HDFS 协议配置
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FSConfig {
    /// name node host name or ip
    pub name_node: String,
    /// name node ipc port, default ipc port is 9000
    pub port: u16,
    /// username
    pub user: String,
}

pub struct BufStream<S: Read + Write>(pub BufReader<Wrapped<S>>);

impl<S: Read + Write> BufStream<S> {
    pub fn new(stream: S) -> Self {
        Self(BufReader::new(Wrapped(BufWriter::new(stream))))
    }

    pub fn with(stream: S, read_buf: usize, write_buf: usize) -> Self {
        Self(BufReader::with_capacity(
            read_buf,
            Wrapped(BufWriter::with_capacity(write_buf, stream)),
        ))
    }
}

pub struct Wrapped<S: Read + Write>(pub BufWriter<S>);

impl<S: Read + Write> Read for Wrapped<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.get_mut().read(buf)
    }
}

impl<S: Read + Write> Write for Wrapped<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<S: Read + Write> Read for BufStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl<S: Read + Write> Write for BufStream<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.get_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.get_mut().flush()
    }
}

impl FSConfig {
    pub fn connect<S: Read + Write>(
        &self,
        mut connect_fn: impl FnMut() -> io::Result<S>,
        ctx: impl Into<Option<RpcCallerContextProto>>,
    ) -> Result<HRpc<S>, HDFSError> {
        let stream = connect_fn()?;
        HRpc::connect(
            stream,
            &self.user,
            ctx.into().unwrap_or_else(|| RpcCallerContextProto {
                context: concat!("hdfs-rust-client-", env!("CARGO_PKG_VERSION")).into(),
                signature: None,
            }),
            None,
        )
    }

    pub fn fs(
        &self,
        ctx: impl Into<Option<RpcCallerContextProto>>,
    ) -> io::Result<FS<BufStream<TcpStream>, BufStream<TcpStream>>> {
        let config = self.clone();
        let ctx = ctx.into();
        FS::new(
            move || {
                let stream = TcpStream::connect(format!("{}:{}", config.name_node, config.port))?;
                let stream = BufStream::new(stream);
                let ipc = HRpc::connect(stream, &config.user, ctx.clone(), None)?;
                Ok(ipc)
            },
            |datanode| {
                let stream =
                    TcpStream::connect((datanode.ip_addr.clone(), datanode.xfer_port as u16))?;
                let stream = BufStream::new(stream);
                Ok(stream)
            },
        )
    }
}

pub struct FS<S: Read + Write, D: Read + Write> {
    client_name: String,
    ipc: HRpc<S>,
    create_ipc: Box<dyn Fn() -> io::Result<HRpc<S>>>,
    connect_data_node: Arc<dyn Fn(&DatanodeIdProto) -> io::Result<D> + 'static>,
}

impl<S: Read + Write, D: Read + Write> FS<S, D> {
    pub fn new(
        create_ipc: impl Fn() -> io::Result<HRpc<S>> + 'static,
        connect_datanode: impl Fn(&DatanodeIdProto) -> io::Result<D> + 'static,
    ) -> io::Result<Self> {
        let client_name = format!("{}_{}", CLIENT_NAME, uuid::Uuid::new_v4());
        let ipc = create_ipc()?;

        Ok(Self {
            client_name,
            ipc,
            create_ipc: Box::new(create_ipc),
            connect_data_node: Arc::new(connect_datanode),
        })
    }
}
