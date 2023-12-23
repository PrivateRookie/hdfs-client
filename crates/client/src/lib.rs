use std::io;

use hdfs_types::common::RpcResponseHeaderProto;
use hdfs_types::hdfs::BlockOpResponseProto;
use prost::{DecodeError, EncodeError};

mod fs;
pub use fs::*;
pub use hdfs_types as types;
mod crc32;
pub mod data_transfer;
pub mod hrpc;

const PROTOCOL: &str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";

/// name node 通信错误
#[derive(Debug, thiserror::Error)]
pub enum HrpcError {
    #[error("{0}")]
    IOError(io::Error),
    #[error("{0}")]
    EncodeError(EncodeError),
    #[error("{0}")]
    DecodeError(DecodeError),
    #[error("")]
    ChecksumError,
    #[error("{0:?}")]
    BlockError(Box<BlockOpResponseProto>),
    #[error("{0:?}")]
    ServerError(Box<RpcResponseHeaderProto>),
    #[error("{0:?}")]
    Custom(String),
}

impl From<io::Error> for HrpcError {
    fn from(value: io::Error) -> Self {
        Self::IOError(value)
    }
}

impl From<EncodeError> for HrpcError {
    fn from(value: EncodeError) -> Self {
        Self::EncodeError(value)
    }
}

impl From<DecodeError> for HrpcError {
    fn from(value: DecodeError) -> Self {
        Self::DecodeError(value)
    }
}

impl From<HrpcError> for io::Error {
    fn from(value: HrpcError) -> Self {
        match value {
            HrpcError::IOError(e) => e,
            HrpcError::EncodeError(e) => io::Error::new(io::ErrorKind::InvalidData, e),
            HrpcError::DecodeError(e) => io::Error::new(io::ErrorKind::InvalidData, e),
            HrpcError::ChecksumError => {
                io::Error::new(io::ErrorKind::InvalidData, "mismatch checksum")
            }
            HrpcError::Custom(e) => io::Error::new(io::ErrorKind::InvalidData, e),
            HrpcError::ServerError(e) => io::Error::new(
                io::ErrorKind::Other,
                format!("name node error response {e:?}"),
            ),
            HrpcError::BlockError(e) => io::Error::new(
                io::ErrorKind::Other,
                format!("block operation response {e:?}"),
            ),
        }
    }
}
