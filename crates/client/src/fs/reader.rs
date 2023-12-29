use std::{
    io::Read,
    io::{self, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

use hdfs_types::hdfs::{
    hdfs_file_status_proto::FileType, DatanodeIdProto, GetBlockLocationsRequestProto,
    GetFileInfoRequestProto, HdfsFileStatusProto, LocatedBlocksProto,
};

use crate::{data_transfer::BlockReadStream, HDFSError, IOType, HDFS};

#[derive(Default)]
pub struct ReaderOptions {
    pub checksum: Option<bool>,
}

impl ReaderOptions {
    pub fn open<S: Read + Write, D: Read + Write>(
        self,
        path: impl AsRef<Path>,
        fs: &mut HDFS<S, D>,
    ) -> Result<FileReader<D>, HDFSError> {
        let src = path.as_ref().to_string_lossy().to_string();
        let (_, info) = fs
            .ipc
            .get_file_info(GetFileInfoRequestProto { src: src.clone() })?;
        let info_fs = match info.fs {
            Some(fs) => fs,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    path.as_ref().display().to_string(),
                )
                .into());
            }
        };
        if !matches!(info_fs.file_type(), FileType::IsFile) {
            return Err(io::Error::new(io::ErrorKind::Other, "expect file type").into());
        }
        let (header, resp) = fs.ipc.get_block_locations(GetBlockLocationsRequestProto {
            src: src.clone(),
            offset: 0,
            length: info_fs.length,
        })?;
        let locations = match resp.locations {
            Some(loc) => loc,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("response header {header:?}"),
                )
                .into());
            }
        };

        if locations.blocks.is_empty() {
            return Err(HDFSError::NoAvailableBlock);
        }
        let client_name = fs.client_name.clone();
        let conn_fn = fs.connect_data_node.clone();
        let block = locations.blocks[0].clone();
        let blk_stream = create_blk_stream(block, &conn_fn, client_name.clone(), self.checksum, 0)?;
        Ok(FileReader {
            connect_data_node: conn_fn,
            locations,
            read: 0,
            block_idx: 0,
            blk_stream,
            client_name,
            checksum: self.checksum,
            metadata: info_fs,
        })
    }
}

fn create_blk_stream<D: Read + Write>(
    block: hdfs_types::hdfs::LocatedBlockProto,
    conn_fn: &Arc<dyn Fn(&DatanodeIdProto, IOType) -> Result<D, io::Error>>,
    client_name: String,
    checksum: Option<bool>,
    offset: u64,
) -> Result<BlockReadStream<D>, HDFSError> {
    let stream =
        block
            .locs
            .iter()
            .enumerate()
            .find_map(|(idx, loc)| match conn_fn(&loc.id, IOType::Read) {
                Ok(stream) => Some(stream),
                Err(e) => {
                    tracing::info!(
                        "try {} location of block {} failed {}",
                        idx + 1,
                        block.b.block_id,
                        e
                    );
                    None
                }
            });
    let stream = stream.ok_or_else(|| HDFSError::NoAvailableLocation)?;
    let blk_stream = BlockReadStream::new(client_name, stream, offset, checksum, block)?;
    Ok(blk_stream)
}

pub struct FileReader<D: Read + Write> {
    connect_data_node: Arc<dyn Fn(&DatanodeIdProto, IOType) -> io::Result<D>>,
    locations: LocatedBlocksProto,
    read: usize,
    block_idx: usize,
    client_name: String,
    blk_stream: BlockReadStream<D>,
    checksum: Option<bool>,
    metadata: HdfsFileStatusProto,
}

impl<D: Read + Write> Read for FileReader<D> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let num = self.blk_stream.read(buf)?;
        self.read += num;
        if self.blk_stream.packet_remain == 0 {
            self.block_idx += 1;
            if let Some(block) = self.locations.blocks.get(self.block_idx).cloned() {
                self.blk_stream = create_blk_stream(
                    block,
                    &self.connect_data_node,
                    self.client_name.clone(),
                    self.checksum,
                    0,
                )?;
            }
        }
        Ok(num)
    }
}

impl<D: Read + Write> io::Seek for FileReader<D> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Start(pos) => self.seek_from_start(pos),
            SeekFrom::End(pos) => {
                let pos = self.locations.file_length as i64 + pos;
                if pos < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "seek before byte 0",
                    ));
                }
                self.seek_from_start(pos as u64)
            }
            SeekFrom::Current(pos) => {
                let pos = self.read as i64 + pos;
                if pos < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "seek before byte 0",
                    ));
                }
                self.seek_from_start(pos as u64)
            }
        }
    }
}

impl<D: Read + Write> FileReader<D> {
    pub fn metadata(&self) -> HdfsFileStatusProto {
        self.metadata.clone()
    }

    fn seek_from_start(&mut self, pos: u64) -> Result<u64, io::Error> {
        let locations = self.locations.clone();
        if pos >= locations.file_length {
            self.read = locations.file_length as usize;
            if let Some(block) = locations.blocks.last().cloned() {
                let offset = block.b.num_bytes();
                self.blk_stream = create_blk_stream(
                    block,
                    &self.connect_data_node,
                    self.client_name.clone(),
                    self.checksum,
                    0,
                )?;
                // FIXME can not set direct offset of hdfs block
                let mut tmp = vec![0; offset as usize];
                self.blk_stream.read_exact(&mut tmp)?;
            }
            Ok(locations.file_length)
        } else {
            let block = locations
                .blocks
                .clone()
                .into_iter()
                .find(|blk| blk.offset + blk.b.num_bytes() > pos)
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no matched block found"))?;
            let offset = pos - block.offset;
            self.blk_stream = create_blk_stream(
                block,
                &self.connect_data_node,
                self.client_name.clone(),
                self.checksum,
                offset,
            )?;
            // FIXME can not set direct offset of hdfs block
            let mut tmp = vec![0; offset as usize];
            self.blk_stream.read_exact(&mut tmp)?;
            self.read = pos as usize;
            Ok(pos)
        }
    }
}
