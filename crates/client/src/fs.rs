use std::io::{self, Read};

use prost::{DecodeError, EncodeError};

use crate::{
    protocol::{
        common::RpcCallerContextProto,
        hdfs::{BlockOpResponseProto, DatanodeIdProto, Status},
    },
    IpcConnection, IpcError,
};

pub const DATA_TRANSFER_PROTO: u16 = 28;
pub const READ_BLOCK: u8 = 81;
pub const WRITE_BLOCK: u8 = 80;
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

mod read;
pub use read::FileReader;
mod write;
pub use write::FileWriter;

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
