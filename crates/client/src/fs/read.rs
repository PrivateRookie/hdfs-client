use super::{
    get_data_node_addr, read_be_u16, read_be_u32, CLIENT_NAME, DATA_TRANSFER_PROTO, READ_BLOCK,
};
use crate::IpcConnection;
use hdfs_types::hdfs::{
    hdfs_file_status_proto::FileType, BaseHeaderProto, BlockOpResponseProto,
    ClientOperationHeaderProto, ClientReadStatusProto, GetBlockLocationsRequestProto,
    GetFileInfoRequestProto, LocatedBlockProto, OpReadBlockProto, PacketHeaderProto, Status,
};
use prost::{
    self,
    bytes::{BufMut, BytesMut},
    encoding::decode_varint,
    Message,
};
use std::{
    io::{self, Read, Write},
    mem,
    net::TcpStream,
    path::Path,
};

pub(crate) enum ReadState {
    Init,
    Handshake { stream: TcpStream },
    Reading(PacketReadState),
}

pub(crate) struct PacketReadState {
    pub(crate) stream: TcpStream,
    pub(crate) packet_remain: usize,
    pub(crate) buffer: BytesMut,
}

/// hdfs block reader
pub struct BlockReader {
    pub(crate) block: LocatedBlockProto,
    pub(crate) state: ReadState,
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

/// HDFS 文件 reader
#[allow(dead_code)]
pub struct FileReader {
    blocks: Vec<LocatedBlockProto>,
    active_blk: Option<BlockReader>,
}

impl FileReader {
    pub fn new<S: Read + Write>(
        ipc: &mut IpcConnection<S>,
        path: impl AsRef<Path>,
    ) -> io::Result<Self> {
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
