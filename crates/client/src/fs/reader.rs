use std::{
    io::Read,
    io::{self, Write},
    path::Path,
    sync::Arc,
};

use hdfs_types::hdfs::{
    hdfs_file_status_proto::FileType, DatanodeIdProto, GetBlockLocationsRequestProto,
    GetFileInfoRequestProto, LocatedBlocksProto,
};

use crate::{fs::block::BlockReadStream, HrpcError, FS};

#[derive(Default)]
pub struct ReaderOptions {
    pub checksum: Option<bool>,
}

impl ReaderOptions {
    pub fn open<S: Read + Write, D: Read + Write>(
        self,
        path: impl AsRef<Path>,
        fs: &mut FS<S, D>,
    ) -> Result<FileReader<D>, HrpcError> {
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
            return Err(HrpcError::Custom("no block in file".into()));
        }
        let client_name = fs.client_name.clone();
        let conn_fn = fs.connect_data_node.clone();
        let block = locations.blocks[0].clone();
        let blk_stream = create_blk_stream(block, &conn_fn, client_name.clone(), self.checksum)?;
        Ok(FileReader {
            connect_data_node: conn_fn,
            locations,
            block_idx: 0,
            blk_stream,
            client_name,
            checksum: self.checksum,
        })
    }
}

fn create_blk_stream<D: Read + Write>(
    block: hdfs_types::hdfs::LocatedBlockProto,
    conn_fn: &Arc<dyn Fn(&DatanodeIdProto) -> Result<D, io::Error>>,
    client_name: String,
    checksum: Option<bool>,
) -> Result<BlockReadStream<D>, HrpcError> {
    let stream = block
        .locs
        .iter()
        .enumerate()
        .find_map(|(idx, loc)| match conn_fn(&loc.id) {
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
    let stream = stream.ok_or_else(|| HrpcError::Custom("no usable location".into()))?;
    let blk_stream = BlockReadStream::new(client_name, stream, 0, checksum, block)?;
    Ok(blk_stream)
}

pub struct FileReader<D: Read + Write> {
    connect_data_node: Arc<dyn Fn(&DatanodeIdProto) -> io::Result<D>>,
    locations: LocatedBlocksProto,
    block_idx: usize,
    client_name: String,
    blk_stream: BlockReadStream<D>,
    checksum: Option<bool>,
}

impl<D: Read + Write> Read for FileReader<D> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let num = self.blk_stream.read(buf)?;
        if self.blk_stream.packet_remain == 0 {
            self.block_idx += 1;
            if let Some(block) = self.locations.blocks.get(self.block_idx).cloned() {
                self.blk_stream = create_blk_stream(
                    block,
                    &self.connect_data_node,
                    self.client_name.clone(),
                    self.checksum,
                )?;
            }
        }
        Ok(num)
    }
}
