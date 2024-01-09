use std::{
    io::{self, BufReader, BufWriter, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    path::Path,
    sync::Arc,
};

use crate::{hrpc::HRpc, HDFSError};
use hdfs_types::hdfs::{DatanodeIdProto, GetListingRequestProto, HdfsFileStatusProto};

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
            write_buf_size: 64 * 1024,
            read_buf_size: 64 * 1024,
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

pub trait ToNameNodes {
    fn to_name_nodes(self) -> Vec<String>;
}

impl ToNameNodes for &str {
    fn to_name_nodes(self) -> Vec<String> {
        vec![self.to_string()]
    }
}
impl ToNameNodes for String {
    fn to_name_nodes(self) -> Vec<String> {
        vec![self]
    }
}

impl ToNameNodes for Vec<String> {
    fn to_name_nodes(self) -> Vec<String> {
        self
    }
}

impl<S: ToString> ToNameNodes for &[S] {
    fn to_name_nodes(self) -> Vec<String> {
        self.iter().map(|s| s.to_string()).collect()
    }
}

impl HDFS<BufStream<TcpStream>, BufStream<TcpStream>> {
    pub fn connect<S: ToString>(
        name_node: impl ToNameNodes,
        user: impl Into<Option<S>>,
    ) -> io::Result<Self> {
        let config = ClientConfig {
            name_node: name_node.to_name_nodes(),
            effective_user: user.into().map(|s| s.to_string()),
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
                    (datanode.host_name.as_str(), datanode.xfer_port as u16)
                        .to_socket_addrs()
                        .map_err(|_| io::Error::new(io::ErrorKind::Other, "invalid address"))?
                } else {
                    tracing::debug!(
                        message = "connect data node",
                        ip = datanode.ip_addr,
                        port = datanode.xfer_port
                    );
                    (datanode.ip_addr.as_str(), datanode.xfer_port as u16)
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

    pub fn client_name(&self) -> &str {
        &self.client_name
    }

    pub fn get_rpc(&mut self) -> &mut HRpc<S> {
        &mut self.ipc
    }

    pub fn new_rpc(&mut self) -> Result<HRpc<S>, io::Error> {
        (self.create_ipc)()
    }

    /// open a file
    pub fn open(&mut self, path: impl AsRef<Path>) -> Result<FileReader<D>, HDFSError> {
        ReaderOptions::default().open(path, self)
    }

    /// use reader option to custom file read options
    pub fn reader_options(&mut self) -> ReaderOptions {
        ReaderOptions::default()
    }

    /// create a file
    pub fn create(&mut self, path: impl AsRef<Path>) -> Result<FileWriter<S, D>, HDFSError> {
        WriterOptions::default().create(path, self)
    }

    /// open a existing file and append content to it
    pub fn append(&mut self, path: impl AsRef<Path>) -> Result<FileWriter<S, D>, HDFSError> {
        WriterOptions::default().append(path, self)
    }

    pub fn writer_options(&mut self) -> WriterOptions {
        WriterOptions::default()
    }

    pub fn create_dir(&mut self, path: impl AsRef<Path>) -> Result<(), HDFSError> {
        let req = hdfs_types::hdfs::MkdirsRequestProto {
            src: path.as_ref().to_string_lossy().to_string(),
            masked: hdfs_types::hdfs::FsPermissionProto { perm: 0o666 },
            create_parent: false,
            unmasked: None,
        };
        let (_, resp) = self.ipc.mkdirs(req)?;
        assert!(resp.result);
        Ok(())
    }

    pub fn create_dir_all(&mut self, path: impl AsRef<Path>) -> Result<(), HDFSError> {
        let req = hdfs_types::hdfs::MkdirsRequestProto {
            src: path.as_ref().to_string_lossy().to_string(),
            masked: hdfs_types::hdfs::FsPermissionProto { perm: 0o666 },
            create_parent: true,
            unmasked: None,
        };
        let (_, resp) = self.ipc.mkdirs(req)?;
        assert!(resp.result);
        Ok(())
    }

    /// read the entire contents of a file into a bytes vector.
    pub fn read(&mut self, path: impl AsRef<Path>) -> Result<Vec<u8>, HDFSError> {
        let mut fd = self.open(path)?;
        let mut buf = vec![0; fd.metadata().length as usize];
        fd.read_to_end(&mut buf)?;
        Ok(buf)
    }

    pub fn read_dir(
        &mut self,
        path: impl AsRef<Path>,
    ) -> Result<Vec<HdfsFileStatusProto>, HDFSError> {
        let mut start_after = vec![];
        let mut result = vec![];
        loop {
            let req = GetListingRequestProto {
                src: path.as_ref().to_string_lossy().to_string(),
                start_after: start_after.clone(),
                need_location: true,
            };
            let (_, resp) = self.get_rpc().get_listing(req)?;
            if let Some(mut dir) = resp.dir_list {
                result.append(&mut dir.partial_listing);
                if dir.remaining_entries == 0 {
                    break;
                }
                if let Some(last) = result.last() {
                    start_after = last.path.clone();
                }
            } else {
                break;
            }
        }
        Ok(result)
    }

    pub fn remove_dir(&mut self, path: impl AsRef<Path>) -> Result<(), HDFSError> {
        let req = hdfs_types::hdfs::DeleteRequestProto {
            src: path.as_ref().to_string_lossy().to_string(),
            recursive: false,
        };
        let (_, resp) = self.ipc.delete(req)?;
        assert!(resp.result);
        Ok(())
    }

    pub fn remove_dir_all(&mut self, path: impl AsRef<Path>) -> Result<(), HDFSError> {
        let req = hdfs_types::hdfs::DeleteRequestProto {
            src: path.as_ref().to_string_lossy().to_string(),
            recursive: true,
        };
        let (_, resp) = self.ipc.delete(req)?;
        assert!(resp.result);
        Ok(())
    }

    pub fn remove_file(&mut self, path: impl AsRef<Path>) -> Result<(), HDFSError> {
        let req = hdfs_types::hdfs::DeleteRequestProto {
            src: path.as_ref().to_string_lossy().to_string(),
            recursive: false,
        };
        self.ipc.delete(req)?;
        Ok(())
    }

    pub fn rename(
        &mut self,
        from: impl AsRef<Path>,
        to: impl AsRef<Path>,
    ) -> Result<(), HDFSError> {
        let req = hdfs_types::hdfs::Rename2RequestProto {
            src: from.as_ref().to_string_lossy().to_string(),
            dst: to.as_ref().to_string_lossy().to_string(),
            overwrite_dest: true,
            move_to_trash: Some(true),
        };
        self.ipc.rename2(req)?;
        Ok(())
    }
}
