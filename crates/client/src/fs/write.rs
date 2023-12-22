use std::{
    io::{self, Read, Write},
    net::TcpStream,
    path::Path,
};

use prost::{
    bytes::{Buf, BufMut, BytesMut},
    encoding::decode_varint,
    Message,
};

use crate::{
    fs::{get_data_node_addr, CLIENT_NAME},
    IpcConnection, DATA_TRANSFER_PROTO, WRITE_BLOCK,
};
use hdfs_types::{
    common::{RequestHeaderProto, RpcRequestHeaderProto},
    hdfs::{
        op_write_block_proto::BlockConstructionStage, AddBlockRequestProto, AddBlockResponseProto,
        BaseHeaderProto, BlockOpResponseProto, ChecksumProto, ChecksumTypeProto,
        ClientOperationHeaderProto, CompleteRequestProto, CreateRequestProto, ExtendedBlockProto,
        FsPermissionProto, HdfsFileStatusProto, LocatedBlockProto, OpWriteBlockProto,
        PacketHeaderProto, PipelineAckProto, Status, UpdateBlockForPipelineRequestProto,
    },
};

pub struct BlockWriter {
    block: LocatedBlockProto,
    inner: Option<InnerWriter>,
    block_size: u64,
    total: u64,
}

struct InnerWriter {
    stream: TcpStream,
    offset: u64,
    seq_no: i64,
}

pub fn decode_hex(s: &str) -> Result<Vec<u8>, std::num::ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}

#[test]
fn x() {
    // let data = decode_hex("001c50ae010a670a410a350a2542502d3231383336383834312d3137322e31382e302e362d3137303237313936323434373610b68080800418a508208080804012080a0012001a0022001222444653436c69656e745f4e4f4e4d41505245445543455f3838373233373634335f31200628013000380040004a0508021080045200580168007000780082012744532d34346438626232652d373466322d346165342d626234322d346339323365313662376666").unwrap();
    // let slice = &data[3..];
    // let write_op = OpWriteBlockProto::decode_length_delimited(slice).unwrap();
    // dbg!(&write_op, write_op.encoded_len(), slice.len());
    // let data = decode_hex("0408001200").unwrap();
    // dbg!(data[0], data.len());
    // let resp = BlockOpResponseProto::decode(&data[1..]).unwrap();
    // dbg!(&resp);
    // let data = decode_hex("000001321d0805100018093a107c2fad4bcec4423cabcbfeb92d6d21b1400048f90292020a8f020a320a2542502d3231383336383834312d3137322e31382e302e362d3137303237313936323434373610b68080800418a508200010001a9c010a4b0a0a3137322e31382e302e35120c3661656366313839313738651a2463656334623263662d623430362d343163652d623466612d373264353165306133646437208a4d28884d308b4d38001080a084d9eb071880a00d2080e0e3e0d6062880a00d30b9ce84ebc7313801420d2f64656661756c742d7261636b4880c0c6d65f500058006000689a869d1278d6edf2e9c7318001b1a58b1188010020002a080a0012001a0022003201003801422744532d34346438626232652d373466322d346165342d626234322d346339323365313662376666").unwrap();
    // let mut data = BytesMut::from_iter(data);
    // data.advance(4);
    // let header_len = decode_varint(&mut data).unwrap();
    // data.advance(header_len as usize);
    // let resp = AddBlockResponseProto::decode_length_delimited(data).unwrap();
    // dbg!(&resp);
    // let data = decode_hex(
    //     "00000004001909060000000000000011010000000000000018012500000000",
    // )
    // .unwrap();
    // let header = PacketHeaderProto::decode_length_delimited(&data[4..]).unwrap();
    // dbg!(&header, header.encoded_len());
    // println!("{:x?}", &data[29..]);
    let data =
        decode_hex("000000d72108021000180e22107c2fad4bcec4423cabcbfeb92d6d21b128003a050a03434c493c0a08636f6d706c657465122e6f72672e6170616368652e6861646f6f702e686466732e70726f746f636f6c2e436c69656e7450726f746f636f6c1801770a192f746573742f68656c6c6f2e7478742e5f434f5059494e475f1222444653436c69656e745f4e4f4e4d41505245445543455f3838373233373634335f311a320a2542502d3231383336383834312d3137322e31382e302e362d3137303237313936323434373610b68080800418a508200620bf8001").unwrap();
    let mut data = BytesMut::from_iter(data);
    data.advance(4);
    let _ = RpcRequestHeaderProto::decode_length_delimited(&mut data).unwrap();
    let _ = RequestHeaderProto::decode_length_delimited(&mut data).unwrap();
    let req = CompleteRequestProto::decode_length_delimited(&mut data).unwrap();
    dbg!(&req);
    // let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
    // let data = decode_hex("09080210001800220100").unwrap();
    // dbg!(PipelineAckProto::decode(&data[1..]));
    // let data =
    //     decode_hex("000000211d0807100018093a107c2fad4bcec4423cabcbfeb92d6d21b1400048fa02020801")
    //         .unwrap();
    // let mut data = BytesMut::from_iter(data);
    // data.advance(4);
    // let header_len = decode_varint(&mut data).unwrap();
    // data.advance(header_len as usize);
    // let resp = crate::protocol::hdfs::CompleteResponseProto::decode_length_delimited(data);
    // dbg!(resp);
}

impl InnerWriter {
    fn new(mut stream: TcpStream, block: LocatedBlockProto) -> io::Result<Self> {
        let req = OpWriteBlockProto {
            header: ClientOperationHeaderProto {
                base_header: BaseHeaderProto {
                    block: block.b,
                    token: Some(block.block_token),
                    trace_info: None,
                },
                client_name: CLIENT_NAME.to_string(),
            },
            stage: BlockConstructionStage::PipelineSetupCreate as i32,
            pipeline_size: block.locs.len() as u32,
            targets: vec![],
            min_bytes_rcvd: 0,
            max_bytes_rcvd: 0,
            latest_generation_stamp: 0,
            requested_checksum: ChecksumProto {
                r#type: ChecksumTypeProto::ChecksumCrc32c as i32,
                bytes_per_checksum: 512,
            },
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        buf.put_u16(DATA_TRANSFER_PROTO);
        buf.put_u8(WRITE_BLOCK);
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "write block failed",
            ));
        }

        Ok(Self {
            stream,
            offset: 0,
            seq_no: 0,
        })
    }

    pub fn write(&mut self, data: &[u8], last: bool) -> io::Result<()> {
        let header = PacketHeaderProto {
            offset_in_block: self.offset as i64,
            seqno: self.seq_no,
            last_packet_in_block: last,
            data_len: data.len() as i32,
            sync_block: None,
        };
        tracing::debug!("write package  {header:?} ...");
        let chunks = if data.is_empty() {
            0
        } else {
            data.len() / 512 + 1
        };
        let total_len = data.len() + chunks * 4 + 4;
        let mut buffer = BytesMut::new();
        buffer.put_u32(total_len as u32);
        buffer.put_u16(header.encoded_len() as u16);
        header.encode(&mut buffer).unwrap();
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        for chunk in data.chunks(512) {
            buffer.put_u32(crc32.checksum(chunk));
        }
        buffer.extend_from_slice(&data);
        self.stream.write_all(&buffer)?;

        buffer.clear();

        let mut tmp_buf = [0u8];
        let length = loop {
            self.stream.read_exact(&mut tmp_buf)?;
            buffer.put_u8(tmp_buf[0]);
            match decode_varint(&mut buffer.clone()) {
                Ok(length) => break length,
                Err(_) => {
                    continue;
                }
            }
        };
        buffer.clear();
        buffer.resize(length as usize, 0);
        self.stream.read_exact(&mut buffer)?;

        let ack = PipelineAckProto::decode(buffer.split()).unwrap();
        tracing::debug!("package response {ack:?}");
        if ack.seqno != self.seq_no {
            // TODO check ack
        }
        self.seq_no += 1;
        self.offset += data.len() as u64;
        Ok(())
    }
}

impl BlockWriter {
    pub fn new<S: Read + Write>(
        ipc: &mut IpcConnection<S>,
        src: String,
        fs: &HdfsFileStatusProto,
        previous: Option<ExtendedBlockProto>,
        block_size: u64,
    ) -> io::Result<Self> {
        let req = AddBlockRequestProto {
            src,
            client_name: CLIENT_NAME.into(),
            previous,
            file_id: fs.file_id,
            ..Default::default()
        };
        let (_, resp) = ipc.add_block(req)?;
        let mut block = resp.block;
        block.locs = block.locs.into_iter().rev().collect();
        Ok(Self {
            block,
            inner: None,
            block_size,
            total: 0,
        })
    }

    pub fn get_offset(&mut self) -> io::Result<u64> {
        self.get_inner_writer().map(|i| i.offset)
    }

    pub fn finalize<S: Read + Write>(
        mut self,
        ipc: &mut IpcConnection<S>,
    ) -> io::Result<ExtendedBlockProto> {
        let inner = self.get_inner_writer()?;
        inner.write(&[], true)?;
        self.flush()?;
        self.block.b.num_bytes = Some(self.total);
        tracing::info!("{:?}", self.block.b);
        let req = UpdateBlockForPipelineRequestProto {
            block: self.block.b.clone(),
            client_name: CLIENT_NAME.into(),
        };
        ipc.update_block_for_pipeline(req)?;
        Ok(self.block.b)
    }

    fn get_inner_writer(&mut self) -> io::Result<&mut InnerWriter> {
        if self.inner.is_none() {
            let inner = loop {
                if let Some(loc) = self.block.locs.pop() {
                    match TcpStream::connect(get_data_node_addr(&loc.id))
                        .and_then(|stream| InnerWriter::new(stream, self.block.clone()))
                    {
                        Ok(stream) => break stream,
                        Err(e) => {
                            tracing::warn!("skip invalid block: {e}");
                        }
                    }
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "no usable location for new block",
                    ));
                }
            };
            self.inner = Some(inner);
        };
        let inner = self.inner.as_mut().unwrap();
        Ok(inner)
    }
}

impl Write for BlockWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let block_size = self.block_size;
        let inner = self.get_inner_writer()?;
        if inner.offset >= block_size || inner.offset + buf.len() as u64 > block_size {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "block is full"));
        }
        inner.write(buf, false)?;
        self.total += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let inner = self.get_inner_writer()?;
        inner.stream.flush()
    }
}

pub struct OldFileWriter<S: Read + Write> {
    total_size: u64,
    block_size: u64,
    fs: HdfsFileStatusProto,
    ipc: IpcConnection<S>,
    path: String,
    active_blk: Option<BlockWriter>,
}

impl<S: Read + Write> OldFileWriter<S> {
    pub fn create(mut ipc: IpcConnection<S>, path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_string_lossy().to_string();
        let req = CreateRequestProto {
            src: path.clone(),
            masked: FsPermissionProto { perm: 0644 },
            client_name: CLIENT_NAME.into(),
            create_flag: 1,
            create_parent: false,
            replication: 1,
            block_size: 1048576,
            ..Default::default()
        };
        let (_, resp) = ipc.create(req)?;
        let fs = resp.fs.unwrap();

        Ok(Self {
            total_size: 0,
            block_size: 1048576,
            fs,
            ipc,
            path,
            active_blk: None,
        })
    }
}

impl<S: Read + Write> Write for OldFileWriter<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.active_blk.is_none() {
            let blk = BlockWriter::new(
                &mut self.ipc,
                self.path.clone(),
                &self.fs,
                None,
                self.block_size,
            )?;
            self.active_blk = Some(blk)
        }
        let blk = self.active_blk.as_mut().unwrap();
        let offset = blk.get_offset()?;
        if offset + buf.len() as u64 >= self.block_size {
            let mut blk = self.active_blk.take().unwrap();
            let split_idx = (self.block_size - offset) as usize;
            let left = &buf[..split_idx];
            blk.write_all(left)?;
            let mut prev = blk.finalize(&mut self.ipc)?;
            let remain = &buf[split_idx..];
            let parts = remain.len() / self.block_size as usize;
            for (idx, chunk) in remain.chunks(self.block_size as usize).enumerate() {
                let is_last = idx + 1 == parts;
                let mut blk = BlockWriter::new(
                    &mut self.ipc,
                    self.path.clone(),
                    &self.fs,
                    Some(prev.clone()),
                    self.block_size,
                )?;
                blk.write_all(chunk)?;
                if is_last {
                    self.active_blk = Some(blk);
                } else {
                    prev = blk.finalize(&mut self.ipc)?;
                }
            }
        } else {
            blk.write_all(buf)?;
        }
        self.total_size += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(blk) = self.active_blk.as_mut() {
            blk.flush()?;
        }
        Ok(())
    }
}

impl<S: Read + Write> Drop for OldFileWriter<S> {
    fn drop(&mut self) {
        let b = self
            .active_blk
            .take()
            .and_then(|blk| blk.finalize(&mut self.ipc).ok());
        tracing::info!("{b:?}");
        let req = hdfs_types::hdfs::CompleteRequestProto {
            src: self.path.clone(),
            client_name: CLIENT_NAME.into(),
            last: b,
            file_id: self.fs.file_id,
        };
        self.ipc.complete(req).ok();
    }
}
