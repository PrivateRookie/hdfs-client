use std::io;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;

use prost::{
    bytes::{BufMut, BytesMut},
    encoding::decode_varint,
    DecodeError, EncodeError, Message,
};
use protocol::common::{
    rpc_response_header_proto::RpcStatusProto, IpcConnectionContextProto, RpcKindProto,
    RpcResponseHeaderProto,
};

use crate::protocol::common::{
    rpc_request_header_proto::OperationProto, RequestHeaderProto, RpcCallerContextProto,
    RpcRequestHeaderProto,
};

/// HDFS 协议消息体
pub mod protocol {
    pub mod common {
        tonic::include_proto!("hadoop.common");
    }

    pub mod hdfs {
        tonic::include_proto!("hadoop.hdfs");
    }
}

mod client_data_node_impl;
mod client_name_node_impl;
mod fs;
pub use fs::*;

pub const PROTOCOL: &str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";

#[derive(Debug, Clone)]
pub struct Handshake {
    pub version: u8,
    pub server_class: u8,
    pub auth_protocol: u8,
}

impl Default for Handshake {
    fn default() -> Self {
        Self {
            version: 9,
            server_class: 0,
            auth_protocol: 0,
        }
    }
}

impl Handshake {
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        buf.put_slice(b"hrpc");
        buf.put_u8(self.version);
        buf.put_u8(self.server_class);
        buf.put_u8(self.auth_protocol);
        Ok(())
    }
}

/// name node 通信错误
#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    #[error("{0}")]
    IOError(io::Error),
    #[error("{0}")]
    EncodeError(EncodeError),
    #[error("{0}")]
    DecodeError(DecodeError),
    #[error("{0:?}")]
    ServerError(Box<RpcResponseHeaderProto>),
}

impl From<io::Error> for IpcError {
    fn from(value: io::Error) -> Self {
        Self::IOError(value)
    }
}

impl From<EncodeError> for IpcError {
    fn from(value: EncodeError) -> Self {
        Self::EncodeError(value)
    }
}

impl From<DecodeError> for IpcError {
    fn from(value: DecodeError) -> Self {
        Self::DecodeError(value)
    }
}

impl RpcResponseHeaderProto {
    pub fn into_err(self) -> Result<Self, IpcError> {
        if self.status() != RpcStatusProto::Success {
            Err(IpcError::ServerError(Box::new(self)))
        } else {
            Ok(self)
        }
    }
}

impl From<IpcError> for io::Error {
    fn from(value: IpcError) -> Self {
        match value {
            IpcError::IOError(e) => e,
            IpcError::EncodeError(e) => io::Error::new(io::ErrorKind::InvalidData, e),
            IpcError::DecodeError(e) => io::Error::new(io::ErrorKind::InvalidData, e),
            IpcError::ServerError(e) => io::Error::new(
                io::ErrorKind::Other,
                format!("name node error response {e:?}"),
            ),
        }
    }
}

/// hdfs client protocol 实现
pub struct IpcConnection {
    stream: TcpStream,
    call_id: i32,
    client_id: Vec<u8>,
    context: Option<RpcCallerContextProto>,
}

impl IpcConnection {
    pub fn connect(
        addr: impl ToSocketAddrs,
        user: &str,
        context: impl Into<Option<RpcCallerContextProto>>,
        handshake: impl Into<Option<Handshake>>,
    ) -> Result<Self, IpcError> {
        let mut stream = TcpStream::connect(addr)?;
        let client_id = uuid::Uuid::new_v4().to_bytes_le().to_vec();
        let context = context.into();
        let mut buf = BytesMut::new();
        let handshake = handshake.into().unwrap_or_default();
        handshake.encode(&mut buf)?;
        let req_header = RpcRequestHeaderProto {
            rpc_kind: Some(RpcKindProto::RpcProtocolBuffer as i32),
            rpc_op: Some(OperationProto::RpcFinalPacket as i32),
            call_id: -3,
            client_id: client_id.clone(),
            retry_count: Some(-1),
            trace_info: None,
            caller_context: context.clone(),
            state_id: None,
            router_federated_state: None,
        };
        let ipc_req = IpcConnectionContextProto {
            protocol: Some(PROTOCOL.into()),
            user_info: Some(protocol::common::UserInformationProto {
                effective_user: Some(user.into()),
                real_user: None,
            }),
        };
        let req_header_bytes = req_header.encode_length_delimited_to_vec();
        let ipc_bytes = ipc_req.encode_length_delimited_to_vec();
        let length = req_header_bytes.len() + ipc_bytes.len();
        buf.put_u32(length as u32);
        buf.extend_from_slice(&req_header_bytes);
        buf.extend_from_slice(&ipc_bytes);
        stream.write_all(&buf)?;
        Ok(Self {
            stream,
            call_id: Default::default(),
            client_id,
            context,
        })
    }

    pub fn send_raw_req(
        &mut self,
        method_name: &str,
        req: &[u8],
    ) -> Result<(RpcResponseHeaderProto, BytesMut), IpcError> {
        let call_id = self.call_id;
        let rpc_req_header = RpcRequestHeaderProto {
            rpc_kind: Some(RpcKindProto::RpcProtocolBuffer as i32),
            rpc_op: Some(OperationProto::RpcFinalPacket as i32),
            call_id,
            client_id: self.client_id.clone(),
            retry_count: Some(0),
            trace_info: None,
            caller_context: self.context.clone(),
            state_id: None,
            router_federated_state: None,
        };
        let rpc_req_bytes = rpc_req_header.encode_length_delimited_to_vec();
        self.call_id += 1;
        let req_header = RequestHeaderProto {
            method_name: method_name.into(),
            declaring_class_protocol_name: PROTOCOL.into(),
            client_protocol_version: 1,
        };
        let header_bytes = req_header.encode_length_delimited_to_vec();
        let total = rpc_req_bytes.len() + header_bytes.len() + req.len();
        let mut buf = BytesMut::with_capacity(total + 4);
        buf.put_u32(total as u32);
        buf.extend_from_slice(&rpc_req_bytes);
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(req);
        self.stream.write_all(&buf)?;
        self.read_resp()
    }

    fn read_resp(&mut self) -> Result<(RpcResponseHeaderProto, BytesMut), IpcError> {
        let mut raw_length = [0u8; 4];
        self.stream.read_exact(&mut raw_length)?;
        let length = u32::from_be_bytes(raw_length) as usize;
        let mut buf = BytesMut::with_capacity(length);
        buf.resize(length, 0);
        self.stream.read_exact(&mut buf)?;
        let header_length = decode_varint(&mut buf)?;
        let header_bytes = buf.split_to(header_length as usize);
        let resp_header = RpcResponseHeaderProto::decode(header_bytes)?;
        // into error if status is not success
        let resp_header = resp_header.into_err()?;
        Ok((resp_header, buf))
    }
}