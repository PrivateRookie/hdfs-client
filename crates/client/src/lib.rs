use std::io;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;

use hdfs_types::common::UserInformationProto;
use hdfs_types::common::{
    rpc_response_header_proto::RpcStatusProto, IpcConnectionContextProto, RpcKindProto,
    RpcResponseHeaderProto,
};
use prost::{
    bytes::{BufMut, BytesMut},
    encoding::decode_varint,
    DecodeError, EncodeError, Message,
};

use hdfs_types::common::{
    rpc_request_header_proto::OperationProto, RequestHeaderProto, RpcCallerContextProto,
    RpcRequestHeaderProto,
};

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

fn into_err(error: RpcResponseHeaderProto) -> Result<RpcResponseHeaderProto, IpcError> {
    if error.status() != RpcStatusProto::Success {
        Err(IpcError::ServerError(Box::new(error)))
    } else {
        Ok(error)
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
pub struct IpcConnection<S> {
    stream: S,
    call_id: i32,
    client_id: Vec<u8>,
    context: Option<RpcCallerContextProto>,
}

impl<S: Write + Read> IpcConnection<S> {
    pub fn connect(
        mut stream: S,
        user: &str,
        context: impl Into<Option<RpcCallerContextProto>>,
        handshake: impl Into<Option<Handshake>>,
    ) -> Result<Self, IpcError> {
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
            user_info: Some(UserInformationProto {
                effective_user: Some(user.into()),
                real_user: None,
            }),
        };
        let ori = buf.len();
        buf.put_u32(0);
        req_header.encode_length_delimited(&mut buf).unwrap();
        ipc_req.encode_length_delimited(&mut buf).unwrap();
        let length = buf.len() - ori - 4;
        buf[ori..(ori + 4)].copy_from_slice(&(length as u32).to_be_bytes());
        stream.write_all(&buf)?;
        stream.flush()?;
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
        let resp_header = into_err(resp_header)?;
        Ok((resp_header, buf))
    }
}

#[macro_export]
macro_rules! method {
    ($name:ident, $raw:literal, $req:ty, $resp:ty) => {
        pub fn $name(&mut self, req: $req) -> Result<(RpcResponseHeaderProto, $resp), IpcError> {
            #[cfg(feature = "trace")]
            {
                tracing::trace!("invoke method: {}, req: {:?}", $raw, req);
            }
            let req = req.encode_length_delimited_to_vec();
            let (header, resp) = self.send_raw_req($raw, &req)?;
            let resp = <$resp>::decode_length_delimited(resp)?;
            #[cfg(feature = "trace")]
            {
                tracing::trace!("response method: {}, header: {:?}, resp: {:?}", $raw, header, resp);
            }
            Ok((header, resp))
        }
    };

    ($ipc:ty => $($name:ident, $raw:literal, $req:ty, $resp:ty);+;) => {
        impl <S: std::io::Write + std::io::Read> $ipc {
            $(method!($name, $raw, $req, $resp);)+
        }
    }
}
