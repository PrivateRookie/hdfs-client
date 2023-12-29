use std::{
    io::{self, BufReader, BufWriter, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    sync::Arc,
};

use crate::hrpc::HRpc;
use hdfs_types::hdfs::DatanodeIdProto;

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

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub real_user: Option<String>,
    pub effective_user: Option<String>,
    pub name_node: Vec<String>,
    pub connection_timeout: u64,
    pub use_hostname: bool,
    pub write_buf_size: usize,
    pub read_buf_size: usize,
    pub tcp_keepalived: Option<u64>,
    pub no_delay: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            real_user: Default::default(),
            effective_user: Default::default(),
            name_node: vec!["127.0.0.1:9000".into()],
            use_hostname: true,
            write_buf_size: 8192,
            read_buf_size: 8192,
            connection_timeout: 30,
            tcp_keepalived: Some(30),
            no_delay: true,
        }
    }
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

#[derive(Debug, Clone, Copy)]
pub enum IOType {
    Read,
    Write,
    Append,
}

pub struct HDFS<S: Read + Write, D: Read + Write> {
    client_name: String,
    ipc: HRpc<S>,
    create_ipc: Box<dyn Fn() -> io::Result<HRpc<S>>>,
    connect_data_node: Arc<dyn Fn(&DatanodeIdProto, IOType) -> io::Result<D> + 'static>,
}

impl HDFS<BufStream<TcpStream>, BufStream<TcpStream>> {
    pub fn connect(name_node: &str, user: impl Into<Option<String>>) -> io::Result<Self> {
        let name_node = vec![name_node.to_string()];
        let config = ClientConfig {
            name_node,
            effective_user: user.into(),
            ..Default::default()
        };
        Self::connect_with(config)
    }

    pub fn connect_with(config: ClientConfig) -> io::Result<Self> {
        let ClientConfig {
            real_user,
            effective_user,
            name_node,
            connection_timeout,
            use_hostname,
            write_buf_size,
            read_buf_size,
            tcp_keepalived,
            no_delay,
        } = config;
        let timeout = std::time::Duration::from_secs(connection_timeout);
        HDFS::new(
            move || {
                let stream = name_node
                    .iter()
                    .filter_map(|addr| addr.to_socket_addrs().ok())
                    .flatten()
                    .find_map(|addr| {
                        tracing::debug!(message="connect name node", addr=?addr);
                        TcpStream::connect_timeout(&addr, timeout).ok()
                    });
                let stream = stream.ok_or(io::Error::new(
                    io::ErrorKind::Other,
                    "no available name node",
                ))?;
                let sk_ref = socket2::SockRef::from(&stream);
                if let Some(keep) = tcp_keepalived {
                    let keepalive = socket2::TcpKeepalive::new()
                        .with_time(std::time::Duration::from_secs(keep));
                    sk_ref.set_tcp_keepalive(&keepalive)?;
                }
                sk_ref.set_nodelay(no_delay)?;
                let stream = BufStream::with(stream, write_buf_size, read_buf_size);
                let ipc = HRpc::connect(
                    stream,
                    effective_user.clone(),
                    real_user.clone(),
                    None,
                    None,
                )?;
                Ok(ipc)
            },
            move |datanode, _| {
                let mut addrs = if use_hostname {
                    tracing::debug!(
                        message = "connect data node",
                        hostname = datanode.host_name,
                        port = datanode.xfer_port
                    );
                    ((datanode.host_name.as_str(), datanode.xfer_port as u16))
                        .to_socket_addrs()
                        .map_err(|_| io::Error::new(io::ErrorKind::Other, "invalid address"))?
                } else {
                    tracing::debug!(
                        message = "connect data node",
                        ip = datanode.ip_addr,
                        port = datanode.xfer_port
                    );
                    ((datanode.ip_addr.as_str(), datanode.xfer_port as u16))
                        .to_socket_addrs()
                        .map_err(|_| io::Error::new(io::ErrorKind::Other, "invalid ip addr"))?
                };
                let addr = addrs
                    .next()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "invalid ip address"))?;
                let stream = TcpStream::connect_timeout(&addr, timeout)?;
                let sk_ref = socket2::SockRef::from(&stream);
                if let Some(keep) = tcp_keepalived {
                    let keepalive = socket2::TcpKeepalive::new()
                        .with_time(std::time::Duration::from_secs(keep));
                    sk_ref.set_tcp_keepalive(&keepalive)?;
                }
                sk_ref.set_nodelay(no_delay)?;
                let stream = BufStream::with(stream, write_buf_size, read_buf_size);
                Ok(stream)
            },
        )
    }
}

impl<S: Read + Write, D: Read + Write> HDFS<S, D> {
    pub fn new(
        create_ipc: impl Fn() -> io::Result<HRpc<S>> + 'static,
        connect_datanode: impl Fn(&DatanodeIdProto, IOType) -> io::Result<D> + 'static,
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
