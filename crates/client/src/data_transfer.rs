use std::io::{self, Read, Write};

use crate::{crc32, hrpc::HRpc, HDFSError};
use crc::Digest;
use hdfs_types::hdfs::{
    op_write_block_proto::BlockConstructionStage, BaseHeaderProto, BlockOpResponseProto,
    ChecksumProto, ChecksumTypeProto, ClientOperationHeaderProto, ExtendedBlockProto,
    LocatedBlockProto, OpReadBlockProto, OpWriteBlockProto, PacketHeaderProto, PipelineAckProto,
    Status, UpdateBlockForPipelineRequestProto,
};
use prost::{
    bytes::{BufMut, BytesMut},
    Message,
};

const DATA_TRANSFER_PROTO: u16 = 28;
const READ_BLOCK: u8 = 81;
const WRITE_BLOCK: u8 = 80;

fn read_prefixed_message<S: Read, M: Message + Default>(stream: &mut S) -> Result<M, HDFSError> {
    use prost::encoding::decode_varint;
    let mut buf = BytesMut::new();
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
    let msg = M::decode(buf)?;
    Ok(msg)
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

macro_rules! trace_valuable {
    ($($s:stmt);*;) => {
        #[cfg(feature="trace_valuable")]
        {
            use valuable::Valuable;
            $($s)*
        }
    };
}

macro_rules! trace_dbg {
    ($($s:stmt);*;) => {
        #[cfg(feature="trace_dbg")]
        {
            $($s)*
        }
    };
}

#[allow(unused)]
pub struct BlockReadStream<S> {
    stream: S,
    pub(crate) packet_remain: usize,
    offset: u64,
    block: LocatedBlockProto,
    checksum: ChecksumProto,
    checksum_data: Vec<u32>,
    checksum_idx: usize,
    checksum_read: usize,
    digest_fn: Box<dyn Fn() -> Digest<'static, u32>>,
    digest: Digest<'static, u32>,
}

impl<S: Read + Write> BlockReadStream<S> {
    pub fn new(
        client_name: String,
        mut stream: S,
        offset: u64,
        send_checksums: Option<bool>,
        block: LocatedBlockProto,
    ) -> Result<Self, HDFSError> {
        let len = block.b.num_bytes() - offset;
        let req = OpReadBlockProto {
            header: ClientOperationHeaderProto {
                base_header: BaseHeaderProto {
                    block: block.b.clone(),
                    token: Some(block.block_token.clone()),
                    trace_info: None,
                },
                client_name,
            },
            offset,
            len,
            send_checksums,
            caching_strategy: None,
        };
        let mut buf = BytesMut::new();
        buf.put_u16(DATA_TRANSFER_PROTO);
        buf.put_u8(READ_BLOCK);
        let length = req.encoded_len();
        buf.reserve(prost::length_delimiter_len(length) + length + 2);
        trace_dbg! {
            tracing::trace!(target: "data-transfer", "\nreq: {req:#?}");
        }
        trace_valuable! {
            tracing::trace!(target: "data-transfer", req=req.as_value());

        }
        req.encode_length_delimited(&mut buf)?;
        stream.write_all(&buf)?;
        stream.flush()?;
        let resp: BlockOpResponseProto = read_prefixed_message(&mut stream)?;
        trace_dbg! {
            tracing::trace!(target: "data-transfer", "\nresp: {resp:#?}");
        }
        trace_valuable! {
            tracing::trace!(target: "data-transfer", resp=resp.as_value());
        }
        if !matches!(resp.status(), Status::Success) {
            tracing::warn!(
                "init data transfer error {}",
                resp.message.clone().unwrap_or_default()
            );
            return Err(HDFSError::DataNodeError(Box::new(resp)));
        }
        let checksum = resp
            .read_op_checksum_info
            .clone()
            .map(|c| c.checksum)
            .unwrap_or_else(|| ChecksumProto {
                bytes_per_checksum: 512,
                r#type: ChecksumTypeProto::ChecksumNull as i32,
            });
        let checksum_ty = checksum.r#type();
        let (header, checksum_data) = start_new_packet(&checksum, &mut stream)?;

        let digest_fn = move || match checksum_ty {
            ChecksumTypeProto::ChecksumNull | ChecksumTypeProto::ChecksumCrc32 => {
                crc32::CRC32.digest()
            }
            ChecksumTypeProto::ChecksumCrc32c => crc32::CRC32C.digest(),
        };
        let mut stream = Self {
            stream,
            block,
            offset,
            packet_remain: header.data_len as usize,
            checksum,
            checksum_data,
            checksum_read: 0,
            checksum_idx: 0,
            digest: digest_fn(),
            digest_fn: Box::new(digest_fn),
        };
        if let Some(ck_resp) = resp.read_op_checksum_info {
            if ck_resp.chunk_offset < offset {
                let diff = offset - ck_resp.chunk_offset;
                let mut buf = vec![0; diff as usize];
                stream.read_exact(&mut buf)?;
                stream.offset = offset;
            }
        }
        Ok(stream)
    }

    pub fn remaining(&self) -> u64 {
        self.block.b.num_bytes() - self.offset
    }

    fn inner_read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.packet_remain == 0 {
            if self.offset >= self.block.b.num_bytes() {
                return Ok(0);
            } else {
                let (header, checksum_data) = start_new_packet(&self.checksum, &mut self.stream)?;
                self.checksum_read = 0;
                self.checksum_idx = 0;
                self.packet_remain = header.data_len as usize;
                self.checksum_data = checksum_data;
            }
        }

        let max_read = self.packet_remain.min(buf.len());
        self.stream.read_exact(&mut buf[..max_read])?;
        self.packet_remain -= max_read;
        self.offset += max_read as u64;
        if matches!(
            self.checksum.r#type(),
            ChecksumTypeProto::ChecksumCrc32 | ChecksumTypeProto::ChecksumCrc32c
        ) {
            let bytes_per_checksum = self.checksum.bytes_per_checksum as usize;
            if self.checksum_read + max_read >= bytes_per_checksum || self.packet_remain == 0 {
                let step = max_read.div_ceil(bytes_per_checksum);
                for i in 0..step {
                    let (start, end) = if i == 0 {
                        (0, (bytes_per_checksum - self.checksum_read))
                    } else {
                        (
                            bytes_per_checksum * i - self.checksum_read,
                            (bytes_per_checksum * (i + 1) - self.checksum_read),
                        )
                    };
                    let end = end.min(max_read);
                    self.digest.update(&buf[start..end]);
                    if (self.packet_remain == 0 && i + 1 == step)
                        || (end + self.checksum_read) % bytes_per_checksum == 0
                    {
                        let checksum = self.checksum_data[self.checksum_idx];
                        let digest = (self.digest_fn)();
                        let old_digest = std::mem::replace(&mut self.digest, digest);
                        let cal = old_digest.finalize();
                        if cal != checksum {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "checksum validation failed expect: {} got: {}",
                                    checksum, cal
                                ),
                            ));
                        }
                    }
                    self.checksum_idx += 1;
                }
                self.checksum_read = (self.checksum_read + max_read) % bytes_per_checksum;
            } else {
                self.digest.update(&buf[..max_read]);
                self.checksum_read += max_read;
            }
        }
        Ok(max_read)
    }
}

fn start_new_packet<S: Read + Write>(
    checksum: &ChecksumProto,
    stream: &mut S,
) -> Result<(PacketHeaderProto, Vec<u32>), HDFSError> {
    let mut buf = BytesMut::new();
    let _length = read_be_u32(stream)?;
    let header_size = read_be_u16(stream)?;
    buf.resize(header_size as usize, 0);
    stream.read_exact(&mut buf)?;
    let header = PacketHeaderProto::decode(buf)?;
    trace_dbg! {
        tracing::trace!(target: "data-transfer", "\npacket header: {header:#?}");
    }
    trace_valuable! {
        tracing::trace!(target: "data-transfer", packet_header=header.as_value());
    }
    // if header.data_len == 0 {
    //     let read_resp = ClientReadStatusProto { status: 0 };
    //     trace_dbg! {
    //         tracing::trace!(target: "data-transfer", "\nresp: {read_resp:#?}");
    //     }
    //     trace_valuable! {
    //         tracing::trace!(target: "data-transfer", resp=read_resp.as_value());
    //     }
    //     stream.write_all(&read_resp.encode_length_delimited_to_vec())?;
    //     stream.flush()?;
    // }
    let checksum_data = match checksum.r#type() {
        ChecksumTypeProto::ChecksumNull => vec![],
        _ => {
            if header.data_len == 0 {
                vec![]
            } else {
                let len = (header.data_len as u32).div_ceil(checksum.bytes_per_checksum) * 4;
                let mut data = vec![0; len as usize];
                stream.read_exact(&mut data)?;
                data.as_slice()
                    .chunks_exact(4)
                    .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
                    .collect()
            }
        }
    };
    Ok((header, checksum_data))
}

impl<S: Read + Write> Read for BlockReadStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner_read(buf)
    }
}

pub struct BlockWriteStream<S> {
    pub(crate) stream: S,
    pub(crate) offset: u64,
    pub(crate) closed: bool,
    seq_no: i64,
    bytes_per_checksum: u32,
    checksum_ty: ChecksumTypeProto,
    block: LocatedBlockProto,
    client_name: String,
}

impl<S: Read + Write> BlockWriteStream<S> {
    pub fn close<D: Read + Write>(
        &mut self,
        ipc: &mut HRpc<D>,
    ) -> Result<ExtendedBlockProto, HDFSError> {
        if self.closed {
            return Ok(self.block.b.clone());
        }
        self.write(&[], true)?;
        self.stream.flush()?;
        self.block.b.num_bytes = Some(self.offset);
        let req = UpdateBlockForPipelineRequestProto {
            block: self.block.b.clone(),
            client_name: self.client_name.clone(),
        };
        ipc.update_block_for_pipeline(req)?;
        self.closed = true;
        Ok(self.block.b.clone())
    }

    pub fn create(
        client_name: String,
        mut stream: S,
        block: LocatedBlockProto,
        bytes_per_checksum: u32,
        checksum_ty: ChecksumTypeProto,
        offset: u64,
        append: bool,
    ) -> Result<Self, HDFSError> {
        let req = OpWriteBlockProto {
            header: ClientOperationHeaderProto {
                base_header: BaseHeaderProto {
                    block: block.b.clone(),
                    token: Some(block.block_token.clone()),
                    trace_info: None,
                },
                client_name: client_name.clone(),
            },
            stage: if append && offset != 0 {
                BlockConstructionStage::PipelineSetupAppend as i32
            } else {
                BlockConstructionStage::PipelineSetupCreate as i32
            },
            pipeline_size: block.locs.len() as u32,
            targets: vec![],
            min_bytes_rcvd: block.b.num_bytes(),
            max_bytes_rcvd: offset,
            latest_generation_stamp: block.b.generation_stamp,
            requested_checksum: ChecksumProto {
                r#type: checksum_ty as i32,
                bytes_per_checksum,
            },
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        buf.put_u16(DATA_TRANSFER_PROTO);
        buf.put_u8(WRITE_BLOCK);
        let length = req.encoded_len();
        buf.reserve(prost::length_delimiter_len(length) + length + 2);
        trace_dbg! {
            tracing::trace!(target: "data-transfer", "\nreq: {req:#?}");
        }
        trace_valuable! {
            tracing::trace!(target: "data-transfer", req=req.as_value());
        }
        req.encode_length_delimited(&mut buf)?;
        stream.write_all(&buf)?;
        stream.flush()?;

        let message: BlockOpResponseProto = read_prefixed_message(&mut stream)?;
        trace_dbg! {
            tracing::trace!(target: "data-transfer", "\nresp: {message:#?}");
        }
        trace_valuable! {
            tracing::trace!(target: "data-transfer", resp=message.as_value());
        }
        if !matches!(message.status(), Status::Success) {
            tracing::warn!(
                "init data transfer error {}",
                message.message.unwrap_or_default()
            );
            // TODO more proper error type
            return Err(io::Error::new(io::ErrorKind::InvalidData, "write block failed").into());
        }
        Ok(Self {
            stream,
            offset,
            seq_no: 0,
            closed: false,
            block,
            bytes_per_checksum,
            checksum_ty,
            client_name,
        })
    }

    fn inner_write(&mut self, data: &[u8], last: bool) -> Result<(), HDFSError> {
        let header = PacketHeaderProto {
            offset_in_block: self.offset as i64,
            seqno: self.seq_no,
            last_packet_in_block: last,
            data_len: data.len() as i32,
            sync_block: None,
        };

        #[cfg(any(feature = "trace_dbg", feature = "trace_valuable"))]
        {
            tracing::trace!(
                target: "data-transfer",
                seq = self.seq_no,
                offset = self.offset,
                data_len = data.len(),
                last
            );
        }
        let chunks = if data.is_empty() {
            0
        } else {
            data.len().div_ceil(self.bytes_per_checksum as usize)
        };
        let total_len = data.len() + chunks * 4 + 4;
        let mut buffer = BytesMut::new();
        buffer.put_u32(total_len as u32);
        buffer.put_u16(header.encoded_len() as u16);
        header.encode(&mut buffer)?;

        match self.checksum_ty {
            ChecksumTypeProto::ChecksumNull => {}
            ChecksumTypeProto::ChecksumCrc32 => {
                for chunk in data.chunks(self.bytes_per_checksum as usize) {
                    buffer.put_u32(crc32::CRC32.checksum(chunk));
                }
            }
            ChecksumTypeProto::ChecksumCrc32c => {
                for chunk in data.chunks(self.bytes_per_checksum as usize) {
                    buffer.put_u32(crc32::CRC32C.checksum(chunk));
                }
            }
        }
        self.stream.write_all(&buffer)?;
        self.stream.write_all(data)?;
        self.stream.flush()?;

        let ack: PipelineAckProto = read_prefixed_message(&mut self.stream)?;
        trace_dbg! {
            tracing::trace!(target: "data-transfer", "\nack: {ack:#?}");
        }
        trace_valuable! {
            tracing::trace!(target: "data-transfer", ack=ack.as_value());
        }
        if ack.seqno != self.seq_no {
            return Err(HDFSError::IOError(io::Error::new(
                io::ErrorKind::InvalidData,
                "mis match seq",
            )));
        }
        self.seq_no += 1;
        self.offset += data.len() as u64;
        Ok(())
    }

    pub fn write(&mut self, data: &[u8], last: bool) -> Result<(), HDFSError> {
        match (data.is_empty(), last) {
            (true, false) => Ok(()),
            (true, true) => self.inner_write(data, last),
            _ => {
                if self.offset % (self.bytes_per_checksum as u64) != 0
                    && self.offset + data.len() as u64 > self.bytes_per_checksum as u64
                {
                    let split = self.bytes_per_checksum as usize
                        - self.offset as usize % (self.bytes_per_checksum as usize);
                    let split = split.min(data.len());
                    self.write_by_i32_max(&data[..split], false)?;
                    self.write_by_i32_max(&data[split..], last)?;
                    Ok(())
                } else {
                    self.write_by_i32_max(data, last)
                }
            }
        }
    }

    fn write_by_i32_max(&mut self, data: &[u8], last: bool) -> Result<(), HDFSError> {
        let total = data.len().div_ceil(i32::MAX as usize);
        for (idx, part) in data.chunks(i32::MAX as usize).enumerate() {
            let last = (idx + 1 == total) && last;
            self.inner_write(part, last)?
        }
        Ok(())
    }
}
