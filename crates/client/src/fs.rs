use std::{
    io::{self, Read, Write},
    mem,
    net::TcpStream,
    path::Path,
};

use prost::{
    bytes::{BufMut, BytesMut},
    encoding::decode_varint,
    DecodeError, EncodeError, Message,
};

use crate::{
    protocol::{
        common::RpcCallerContextProto,
        hdfs::{
            hdfs_file_status_proto::FileType, BaseHeaderProto, BlockOpResponseProto,
            ClientOperationHeaderProto, ClientReadStatusProto, DatanodeIdProto,
            GetBlockLocationsRequestProto, GetFileInfoRequestProto, LocatedBlockProto,
            OpReadBlockProto, PacketHeaderProto, Status,
        },
    },
    IpcConnection, IpcError,
};

pub const DATA_TRANSFER_PROTO: u16 = 28;
pub const READ_BLOCK: u8 = 81;
const CLIENT_NAME: &str = "hdfs-rust-client";

#[derive(Debug, thiserror::Error)]
pub enum BlockError {
    #[error("{0}")]
    IOError(io::Error),
    #[error("{0}")]
    EncodeError(EncodeError),
    #[error("{0}")]
    DecodeError(DecodeError),
    #[error("{0:?}")]
    ServerError(Box<BlockOpResponseProto>),
    #[error("no available block")]
    NoAvailableBlock,
}

impl From<io::Error> for BlockError {
    fn from(value: io::Error) -> Self {
        Self::IOError(value)
    }
}

impl From<EncodeError> for BlockError {
    fn from(value: EncodeError) -> Self {
        Self::EncodeError(value)
    }
}

impl From<DecodeError> for BlockError {
    fn from(value: DecodeError) -> Self {
        Self::DecodeError(value)
    }
}

impl BlockOpResponseProto {
    pub fn into_err(self) -> Result<Self, BlockError> {
        if self.status() != Status::Success {
            Err(BlockError::ServerError(Box::new(self)))
        } else {
            Ok(self)
        }
    }
}

fn read_be_u32<S: Read>(stream: &mut S) -> io::Result<u32> {
    let mut bytes = [0; 4];
    stream.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_be_u16<S: Read>(stream: &mut S) -> io::Result<u16> {
    let mut bytes = [0; 2];
    stream.read_exact(&mut bytes)?;
    Ok(u16::from_be_bytes(bytes))
}

fn get_data_node_addr(node: &DatanodeIdProto) -> (String, u16) {
    tracing::debug!("block node ip: {} port: {}", node.ip_addr, node.xfer_port);
    (node.ip_addr.clone(), node.xfer_port as u16)
}

enum ReadState {
    Init,
    Handshake { stream: TcpStream },
    Reading(PacketReadState),
}

struct PacketReadState {
    stream: TcpStream,
    packet_remain: usize,
    buffer: BytesMut,
}

/// hdfs block reader
pub struct BlockReader {
    block: LocatedBlockProto,
    state: ReadState,
}

impl BlockReader {
    pub fn new(block: LocatedBlockProto) -> Self {
        Self {
            block,
            state: ReadState::Init,
        }
    }
}

impl Read for BlockReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "read buffer should not be empty",
            ));
        }

        let mut state = loop {
            match mem::replace(&mut self.state, ReadState::Init) {
                ReadState::Init => {
                    let mut ok = false;
                    while let Some(loc) = self.block.locs.pop() {
                        match TcpStream::connect(get_data_node_addr(&loc.id)) {
                            Ok(stream) => {
                                self.state = ReadState::Handshake { stream };
                                ok = true;
                                break;
                            }
                            Err(e) => {
                                tracing::warn!("create connection failed {e}");
                            }
                        }
                    }
                    if !ok {
                        return Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "no available location founded",
                        ));
                    }
                }
                ReadState::Handshake { mut stream } => {
                    let req = OpReadBlockProto {
                        header: ClientOperationHeaderProto {
                            base_header: BaseHeaderProto {
                                block: self.block.b.clone(),
                                token: Some(self.block.block_token.clone()),
                                trace_info: None,
                            },
                            client_name: CLIENT_NAME.to_string(),
                        },
                        offset: 0,
                        len: self.block.b.num_bytes(),
                        send_checksums: Some(false),
                        caching_strategy: None,
                    };
                    let mut buf = BytesMut::new();
                    buf.put_u16(DATA_TRANSFER_PROTO);
                    buf.put_u8(READ_BLOCK);
                    let length = req.encoded_len();
                    buf.reserve(prost::length_delimiter_len(length) + length + 2);
                    req.encode_length_delimited(&mut buf)?;
                    stream.write_all(&buf)?;
                    buf.clear();
                    let mut tmp_buf = [0u8];
                    let length = loop {
                        stream.read_exact(&mut tmp_buf)?;
                        buf.put_u8(tmp_buf[0]);
                        match decode_varint(&mut buf.clone()) {
                            Ok(length) => break length,
                            Err(_) => {
                                continue;
                            }
                        }
                    };
                    buf.clear();
                    buf.resize(length as usize, 0);
                    stream.read_exact(&mut buf)?;
                    let op_resp = BlockOpResponseProto::decode(buf.split())?;
                    if !matches!(op_resp.status(), Status::Success) {
                        tracing::warn!(
                            "init data transfer error {}",
                            op_resp.message.unwrap_or_default()
                        );
                        self.state = ReadState::Init;
                    } else {
                        self.state = ReadState::Reading(PacketReadState {
                            stream,
                            buffer: BytesMut::new(),
                            packet_remain: 0,
                        })
                    }
                }
                ReadState::Reading(state) => {
                    break state;
                }
            }
        };
        if state.packet_remain == 0 {
            let _length = read_be_u32(&mut state.stream)?;
            let header_size = read_be_u16(&mut state.stream)?;
            state.buffer.resize(header_size as usize, 0);
            state.stream.read_exact(&mut state.buffer)?;
            let header = PacketHeaderProto::decode(state.buffer.split())?;
            if header.data_len == 0 {
                let read_resp = ClientReadStatusProto { status: 0 };
                state
                    .stream
                    .write_all(&read_resp.encode_length_delimited_to_vec())?;
                return Ok(0);
            }
            state.packet_remain = header.data_len as usize;
        }
        // 不要读取超过本次 packet 边界的数据
        let num = if buf.len() >= state.packet_remain {
            state.stream.read_exact(&mut buf[..state.packet_remain])?;
            let num = state.packet_remain;
            state.packet_remain = 0;
            num
        } else {
            state.stream.read_exact(buf)?;
            state.packet_remain -= buf.len();
            buf.len()
        };
        // 读取完成后复位
        self.state = ReadState::Reading(state);
        Ok(num)
    }
}

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

impl FSConfig {
    pub fn connect(
        &self,
        ctx: impl Into<Option<RpcCallerContextProto>>,
    ) -> Result<IpcConnection, IpcError> {
        IpcConnection::connect(
            (self.name_node.clone(), self.port),
            &self.user,
            ctx.into().unwrap_or_else(|| RpcCallerContextProto {
                context: concat!("hdfs-rust-client-", env!("CARGO_PKG_VERSION")).into(),
                signature: None,
            }),
            None,
        )
    }
}

/// HDFS 文件 reader
#[allow(dead_code)]
pub struct FileReader {
    blocks: Vec<LocatedBlockProto>,
    active_blk: Option<BlockReader>,
}

impl FileReader {
    pub fn new(ipc: &mut IpcConnection, path: impl AsRef<Path>) -> io::Result<Self> {
        let (_, info) = ipc.get_file_info(GetFileInfoRequestProto {
            src: path.as_ref().display().to_string(),
        })?;
        let fs = match info.fs {
            Some(fs) => fs,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    path.as_ref().display().to_string(),
                ));
            }
        };
        if !matches!(fs.file_type(), FileType::IsFile) {
            return Err(io::Error::new(io::ErrorKind::Other, "expect file type"));
        }
        let (header, resp) = ipc.get_block_locations(GetBlockLocationsRequestProto {
            src: path.as_ref().display().to_string(),
            offset: 0,
            length: fs.length,
        })?;
        let locations = match resp.locations {
            Some(loc) => loc,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("response header {header:?}"),
                ));
            }
        };
        let mut blocks: Vec<LocatedBlockProto> = locations.blocks.into_iter().rev().collect();
        let active_blk = blocks.pop().map(BlockReader::new);
        Ok(Self { blocks, active_blk })
    }
}

impl Read for FileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if let Some(reader) = self.active_blk.as_mut() {
                let num = reader.read(buf)?;
                if num == 0 {
                    self.active_blk = self.blocks.pop().map(BlockReader::new);
                    continue;
                } else {
                    break Ok(num);
                }
            } else {
                break Ok(0);
            }
        }
    }
}