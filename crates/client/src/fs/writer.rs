use std::{
    io::{self, Read, Write},
    path::Path,
    sync::Arc,
};

use hdfs_types::hdfs::{
    AddBlockRequestProto, ChecksumTypeProto, CompleteRequestProto, CreateRequestProto,
    DatanodeIdProto, ExtendedBlockProto, FsPermissionProto, FsServerDefaultsProto,
    GetServerDefaultsRequestProto, HdfsFileStatusProto,
};

use crate::{HrpcError, IpcConnection, FS};

use super::block::BlockWriteStream;

pub struct FileWriter<S: Read + Write, D: Read + Write> {
    written: u64,
    block_size: u64,
    ipc: IpcConnection<S>,
    connect_data_node: Arc<dyn Fn(&DatanodeIdProto) -> io::Result<D>>,
    fs: HdfsFileStatusProto,
    default: FsServerDefaultsProto,
    client_name: String,
    blk_stream: BlockWriteStream<D>,
    path: String,
}

impl<S: Read + Write, D: Read + Write> FileWriter<S, D> {
    pub fn options() -> WriterOptions {
        WriterOptions::default()
    }
}

impl<S: Read + Write, D: Read + Write> Write for FileWriter<S, D> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let offset = self.blk_stream.offset;
        if offset + buf.len() as u64 >= self.block_size {
            let split_idx = (self.block_size - offset) as usize;
            let left = &buf[..split_idx];
            self.blk_stream.write(left, false)?;
            let mut prev = self.blk_stream.close(&mut self.ipc)?;
            let remain = &buf[split_idx..];
            let parts = remain.len() / self.block_size as usize;
            for (idx, chunk) in remain.chunks(self.block_size as usize).enumerate() {
                let is_last = idx + 1 == parts;
                let mut blk = create_blk(
                    &mut self.ipc,
                    self.client_name.clone(),
                    self.path.clone(),
                    &self.fs,
                    self.connect_data_node.clone(),
                    &self.default,
                    Some(prev.clone()),
                )?;
                blk.write(chunk, false)?;
                if is_last {
                    self.blk_stream = blk;
                } else {
                    prev = blk.close(&mut self.ipc)?;
                }
            }
        } else {
            self.blk_stream.write(buf, false)?;
        }
        self.written += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.blk_stream.stream.flush()
    }
}

#[derive(Debug, Default)]
#[allow(unused)]
pub struct WriterOptions {
    replica: Option<u32>,
    checksum: Option<ChecksumTypeProto>,
    block_size: Option<u64>,
    perm: Option<u32>,
    unmask: Option<u32>,
    over_ride: bool,
}

impl WriterOptions {
    pub fn replica(self, replica: impl Into<Option<u32>>) -> Self {
        Self {
            replica: replica.into(),
            ..self
        }
    }

    pub fn checksum(self, checksum: impl Into<Option<ChecksumTypeProto>>) -> Self {
        Self {
            checksum: checksum.into(),
            ..self
        }
    }

    pub fn block_size(self, block_size: impl Into<Option<u64>>) -> Self {
        Self {
            block_size: block_size.into(),
            ..self
        }
    }

    // pub fn append(self, path: impl AsRef<Path>) -> FileWriter {
    //     todo!()
    // }

    pub fn create<S: Read + Write, D: Read + Write>(
        self,
        path: impl AsRef<Path>,
        fs: &mut FS<S, D>,
    ) -> Result<FileWriter<S, D>, HrpcError> {
        let (_, default) = fs
            .ipc
            .get_server_defaults(GetServerDefaultsRequestProto {})?;
        let default = default.server_defaults;
        let path = path.as_ref().to_string_lossy().to_string();
        let req = CreateRequestProto {
            src: path.clone(),
            masked: FsPermissionProto {
                perm: self.perm.unwrap_or(0644),
            },
            unmasked: self.unmask.map(|u| FsPermissionProto { perm: u }),
            client_name: fs.client_name.clone(),
            create_flag: 1,
            create_parent: false,
            replication: self.replica.unwrap_or_else(|| default.replication),
            block_size: self.block_size.unwrap_or_else(|| default.block_size),
            ..Default::default()
        };
        let (_, resp) = fs.ipc.create(req)?;
        let fs_status = resp
            .fs
            .ok_or_else(|| HrpcError::Custom("namenode return empty fs".into()))?;

        let active_blk = create_blk(
            &mut fs.ipc,
            fs.client_name.clone(),
            path.clone(),
            &fs_status,
            fs.connect_data_node.clone(),
            &default,
            None,
        )?;

        Ok(FileWriter {
            written: 0,
            block_size: self.block_size.unwrap_or_else(|| default.block_size),
            ipc: (fs.create_ipc)()?,
            connect_data_node: fs.connect_data_node.clone(),
            client_name: fs.client_name.clone(),
            fs: fs_status,
            default,
            blk_stream: active_blk,
            path,
        })
    }
}

fn create_blk<S: Read + Write, D: Read + Write>(
    ipc: &mut IpcConnection<S>,
    client_name: String,
    path: String,
    fs_status: &HdfsFileStatusProto,
    conn_fn: Arc<dyn Fn(&DatanodeIdProto) -> Result<D, io::Error>>,
    default: &FsServerDefaultsProto,
    previous: Option<ExtendedBlockProto>,
) -> Result<BlockWriteStream<D>, HrpcError> {
    let req = AddBlockRequestProto {
        src: path.clone(),
        client_name: client_name.clone(),
        previous,
        file_id: fs_status.file_id,
        ..Default::default()
    };
    let (_, resp) = ipc.add_block(req)?;
    let new_blk = resp.block;
    let stream = new_blk
        .locs
        .iter()
        .enumerate()
        .find_map(|(idx, loc)| match conn_fn(&loc.id) {
            Ok(stream) => Some(stream),
            Err(e) => {
                tracing::info!(
                    "try {} location of block {} failed {e}",
                    idx + 1,
                    new_blk.b.block_id
                );
                None
            }
        });
    let stream = stream.ok_or_else(|| HrpcError::Custom("no usable location".into()))?;
    let blk_stream = BlockWriteStream::create(
        client_name.clone(),
        stream,
        new_blk,
        default.bytes_per_checksum,
        default.checksum_type(),
    )?;
    Ok(blk_stream)
}

impl<S: Read + Write, D: Read + Write> Drop for FileWriter<S, D> {
    fn drop(&mut self) {
        self.blk_stream.close(&mut self.ipc).ok().map(|b| {
            let req = CompleteRequestProto {
                src: self.path.clone(),
                client_name: self.client_name.clone(),
                last: Some(b),
                file_id: self.fs.file_id,
            };
            self.ipc.complete(req).ok();
        });
    }
}