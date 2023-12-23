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


/// name node 通信错误
#[derive(Debug, thiserror::Error)]
pub enum HDFSError {
    #[error("{0}")]
    IOError(io::Error),
    #[error("{0}")]
    EncodeError(EncodeError),
    #[error("{0}")]
    DecodeError(DecodeError),
    #[error("")]
    ChecksumError,
    #[error("")]
    NoAvailableBlock,
    #[error("")]
    NoAvailableLocation,
    #[error("")]
    EmptyFS,
    #[error("{0:?}")]
    DataNodeError(Box<BlockOpResponseProto>),
    #[error("{0:?}")]
    NameNodeError(Box<RpcResponseHeaderProto>),
}

impl From<io::Error> for HDFSError {
    fn from(value: io::Error) -> Self {
        Self::IOError(value)
    }
}

impl From<EncodeError> for HDFSError {
    fn from(value: EncodeError) -> Self {
        Self::EncodeError(value)
    }
}

impl From<DecodeError> for HDFSError {
    fn from(value: DecodeError) -> Self {
        Self::DecodeError(value)
    }
}

impl From<HDFSError> for io::Error {
    fn from(value: HDFSError) -> Self {
        match value {
            HDFSError::IOError(e) => e,
            HDFSError::EncodeError(e) => io::Error::new(io::ErrorKind::InvalidData, e),
            HDFSError::DecodeError(e) => io::Error::new(io::ErrorKind::InvalidData, e),
            HDFSError::ChecksumError => {
                io::Error::new(io::ErrorKind::InvalidData, "mismatch checksum")
            }
            HDFSError::EmptyFS => {
                io::Error::new(io::ErrorKind::InvalidData, "namenode return empty fs")
            }
            HDFSError::NoAvailableBlock => {
                io::Error::new(io::ErrorKind::UnexpectedEof, "no available block")
            }
            HDFSError::NoAvailableLocation => {
                io::Error::new(io::ErrorKind::UnexpectedEof, "no available location")
            }
            HDFSError::NameNodeError(e) => io::Error::new(
                io::ErrorKind::Other,
                format!("name node error response {e:?}"),
            ),
            HDFSError::DataNodeError(e) => io::Error::new(
                io::ErrorKind::Other,
                format!("block operation response {e:?}"),
            ),
        }
    }
}
