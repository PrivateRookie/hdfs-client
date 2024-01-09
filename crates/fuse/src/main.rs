use std::{
    collections::HashMap,
    io::{Read, Seek, Write},
    net::TcpStream,
    path::PathBuf,
    time::{Duration, SystemTime},
};

use clap::Parser;
use fuser::{FileAttr, MountOption, TimeOrNow, FUSE_ROOT_ID};
use hdfs_client::{
    types::hdfs::{
        CheckAccessRequestProto, CompleteRequestProto, CreateRequestProto, DatanodeReportTypeProto,
        DeleteRequestProto, FsPermissionProto, FsyncRequestProto, GetDatanodeReportRequestProto,
        GetFileInfoRequestProto, GetFsStatusRequestProto, GetServerDefaultsRequestProto,
        Rename2RequestProto, SetPermissionRequestProto, SetTimesRequestProto, TruncateRequestProto,
    },
    BufStream, HDFSError, HDFS,
};
use libc::{
    EACCES, EADDRINUSE, EADDRNOTAVAIL, ECONNABORTED, ECONNREFUSED, ECONNRESET, EEXIST, EIO, ENOENT,
    ENOMEM, ENOSYS, ENOTCONN, ENOTSUP, EPIPE, EREMOTEIO, ERESTART, ETIMEDOUT, EWOULDBLOCK, EXFULL,
};
use time::{OffsetDateTime, UtcOffset};
use tracing::Level;
use tracing_rolling::Checker;
use tracing_subscriber::fmt::time::OffsetTime;

/// HDFS via fuse
#[derive(Debug, Parser)]
struct Cmd {
    /// name node address, hostname is supported too
    #[arg(long, default_value = "127.0.0.1:9000")]
    name_node: String,
    #[arg(long)]
    /// backup name node address
    backup_name_node: Option<String>,
    /// connection user name
    #[arg(long, env = "USER")]
    user: String,
    /// log level
    #[arg(long, default_value = "INFO")]
    level: Level,
    /// log file path
    #[arg(long)]
    log: Option<PathBuf>,
    /// timezone offset hour
    #[arg(long, default_value = "0")]
    offset: i8,
    /// mouth point
    mount: PathBuf,
}

fn main() {
    let cmd = Cmd::parse();
    let offset = UtcOffset::from_hms(cmd.offset, 0, 0).expect("invalid offset");
    let fmt = tracing_subscriber::fmt()
        .with_max_level(cmd.level)
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_timer(OffsetTime::new(
            offset,
            time::format_description::well_known::Rfc3339,
        ));
    let token = if let Some(log) = cmd.log {
        let (writer, token) = tracing_rolling::ConstFile::new(log)
            .buffered()
            .build()
            .expect("failed to init log file");
        fmt.with_ansi(false).with_writer(writer).init();
        Some(token)
    } else {
        fmt.init();
        None
    };
    let mut addr = vec![cmd.name_node];
    if let Some(backup) = cmd.backup_name_node {
        addr.push(backup)
    };
    let client = HDFS::connect(addr, cmd.user).unwrap();
    let fs = FileSystem::new(client);
    fuser::mount2(
        fs,
        cmd.mount,
        &[MountOption::AutoUnmount, MountOption::AllowRoot],
    )
    .unwrap();
    drop(token);
}

struct FileSystem {
    client: HDFS<BufStream<TcpStream>, BufStream<TcpStream>>,
    ino_map: HashMap<u64, String>,
}

impl FileSystem {
    pub fn new(client: HDFS<BufStream<TcpStream>, BufStream<TcpStream>>) -> Self {
        let mut ino_map: HashMap<u64, String> = Default::default();
        ino_map.insert(FUSE_ROOT_ID, "/".into());
        tracing::info!("client id {}", client.client_name());
        Self { client, ino_map }
    }
}

const TTL: std::time::Duration = std::time::Duration::from_secs(30);

fn convert_ty(ty: hdfs_client::types::hdfs::hdfs_file_status_proto::FileType) -> fuser::FileType {
    match ty {
        hdfs_client::types::hdfs::hdfs_file_status_proto::FileType::IsDir => {
            fuser::FileType::Directory
        }
        hdfs_client::types::hdfs::hdfs_file_status_proto::FileType::IsFile => {
            fuser::FileType::RegularFile
        }
        hdfs_client::types::hdfs::hdfs_file_status_proto::FileType::IsSymlink => {
            fuser::FileType::Symlink
        }
    }
}

fn now_millis() -> u64 {
    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1000000) as u64
}

impl FileSystem {
    fn get_file_attr(&mut self, path: String) -> Result<FileAttr, HDFSError> {
        let req = GetFileInfoRequestProto { src: path.clone() };
        self.client
            .get_rpc()
            .get_file_info(req)
            .and_then(|(_, resp)| {
                resp.fs.ok_or_else(|| {
                    HDFSError::IOError(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("{} not found", path),
                    ))
                })
            })
            .map(|fs| {
                self.ino_map.insert(fs.file_id(), path);
                convert_fs(fs)
            })
    }
}

fn convert_fs(fs: hdfs_client::types::hdfs::HdfsFileStatusProto) -> FileAttr {
    FileAttr {
        ino: fs.file_id(),
        size: fs.length,
        blocks: fs
            .locations
            .clone()
            .map(|loc| loc.blocks.len() as u64)
            .unwrap_or_default(),
        atime: SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(fs.access_time))
            .unwrap_or(SystemTime::UNIX_EPOCH),
        mtime: SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(fs.modification_time))
            .unwrap_or(SystemTime::UNIX_EPOCH),
        ctime: SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(fs.modification_time))
            .unwrap_or(SystemTime::UNIX_EPOCH),
        crtime: SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(fs.modification_time))
            .unwrap_or(SystemTime::UNIX_EPOCH),
        kind: convert_ty(fs.file_type()),
        perm: fs.permission.perm as u16,
        nlink: 0,
        uid: users::get_user_by_name(&fs.owner)
            .map(|u| u.uid())
            .unwrap_or_else(users::get_current_uid),
        gid: users::get_user_by_name(&fs.group)
            .map(|u| u.uid())
            .unwrap_or_else(users::get_current_gid),
        rdev: 0,
        blksize: fs.blocksize() as u32,
        flags: fs.flags(),
    }
}

macro_rules! try_get {
    ($fs:ident, $ino:expr, $reply:ident) => {
        match $fs.ino_map.get(&$ino) {
            Some(path) => path,
            None => {
                $reply.error(ENOENT);
                return;
            }
        }
    };
}

macro_rules! handle {
    ($result:expr, $reply:ident, $ok:expr) => {
        #[allow(unused)]
        #[allow(clippy::redundant_closure_call)]
        match $result {
            Ok(result) => $ok(result),
            Err(e) => {
                match e {
                    HDFSError::IOError(e) => {
                        tracing::debug!("{e}");
                        let eno = match e.kind() {
                            std::io::ErrorKind::NotFound => ENOENT,
                            std::io::ErrorKind::PermissionDenied => EACCES,
                            std::io::ErrorKind::ConnectionRefused => ECONNREFUSED,
                            std::io::ErrorKind::ConnectionReset => ECONNRESET,
                            // std::io::ErrorKind::HostUnreachable => libc::EHOSTUNREACH,
                            // std::io::ErrorKind::NetworkUnreachable => libc::ENETUNREACH,
                            std::io::ErrorKind::ConnectionAborted => ECONNABORTED,
                            std::io::ErrorKind::NotConnected => ENOTCONN,
                            std::io::ErrorKind::AddrInUse => EADDRINUSE,
                            std::io::ErrorKind::AddrNotAvailable => EADDRNOTAVAIL,
                            // std::io::ErrorKind::NetworkDown => libc::ENETDOWN,
                            std::io::ErrorKind::BrokenPipe => EPIPE,
                            std::io::ErrorKind::AlreadyExists => EEXIST,
                            std::io::ErrorKind::WouldBlock => EWOULDBLOCK,
                            // std::io::ErrorKind::NotADirectory => libc::ENOTDIR,
                            // std::io::ErrorKind::IsADirectory => libc::EISDIR,
                            // std::io::ErrorKind::DirectoryNotEmpty => libc::ENOTEMPTY,
                            // std::io::ErrorKind::ReadOnlyFilesystem => libc::EROFS,
                            // std::io::ErrorKind::FilesystemLoop => libc::ELOOP,
                            // std::io::ErrorKind::StaleNetworkFileHandle => libc::ESTALE,
                            std::io::ErrorKind::InvalidInput => EIO,
                            std::io::ErrorKind::InvalidData => EIO,
                            std::io::ErrorKind::TimedOut => ETIMEDOUT,
                            std::io::ErrorKind::WriteZero => EXFULL,
                            // std::io::ErrorKind::StorageFull => libc::EXFULL,
                            // std::io::ErrorKind::NotSeekable => libc::ESPIPE,
                            // std::io::ErrorKind::FilesystemQuotaExceeded => libc::EDQUOT,
                            // std::io::ErrorKind::FileTooLarge => libc::EFBIG,
                            // std::io::ErrorKind::ResourceBusy => libc::EBUSY,
                            // std::io::ErrorKind::ExecutableFileBusy => libc::ETXTBSY,
                            // std::io::ErrorKind::Deadlock => libc::EDEADLOCK,
                            // std::io::ErrorKind::CrossesDevices => libc::EXDEV,
                            // std::io::ErrorKind::TooManyLinks => libc::EMLINK,
                            // std::io::ErrorKind::InvalidFilename => libc::EILSEQ,
                            // std::io::ErrorKind::ArgumentListTooLong => libc::EINVAL,
                            std::io::ErrorKind::Interrupted => ERESTART,
                            std::io::ErrorKind::Unsupported => ENOTSUP,
                            std::io::ErrorKind::UnexpectedEof => EIO,
                            std::io::ErrorKind::OutOfMemory => ENOMEM,
                            std::io::ErrorKind::Other => EIO,
                            _ => EIO,
                        };
                        $reply.error(eno);
                    }
                    HDFSError::EncodeError(_)
                    | HDFSError::DecodeError(_)
                    | HDFSError::ChecksumError
                    | HDFSError::NoAvailableBlock
                    | HDFSError::NoAvailableLocation
                    | HDFSError::EmptyFS => {
                        tracing::debug!("{e}");
                        $reply.error(EIO);
                    }
                    HDFSError::DataNodeError(e) => {
                        tracing::debug!("data node response error: {}", e.message());
                        $reply.error(EREMOTEIO);
                    }
                    HDFSError::NameNodeError(e) => {
                        tracing::debug!("name node response error: {}", e.error_msg());
                        $reply.error(EREMOTEIO);
                    }
                };
                return;
            }
        }
    };
    (= $result:expr, $reply:ident) => {
        match $result {
            Ok(result) => result,
            Err(e) => {
                match e {
                    HDFSError::IOError(e) => {
                        tracing::debug!("{e}");
                        let eno = match e.kind() {
                            std::io::ErrorKind::NotFound => ENOENT,
                            std::io::ErrorKind::PermissionDenied => EACCES,
                            std::io::ErrorKind::ConnectionRefused => ECONNREFUSED,
                            std::io::ErrorKind::ConnectionReset => ECONNRESET,
                            // std::io::ErrorKind::HostUnreachable => libc::EHOSTUNREACH,
                            // std::io::ErrorKind::NetworkUnreachable => libc::ENETUNREACH,
                            std::io::ErrorKind::ConnectionAborted => ECONNABORTED,
                            std::io::ErrorKind::NotConnected => ENOTCONN,
                            std::io::ErrorKind::AddrInUse => EADDRINUSE,
                            std::io::ErrorKind::AddrNotAvailable => EADDRNOTAVAIL,
                            // std::io::ErrorKind::NetworkDown => libc::ENETDOWN,
                            std::io::ErrorKind::BrokenPipe => EPIPE,
                            std::io::ErrorKind::AlreadyExists => EEXIST,
                            std::io::ErrorKind::WouldBlock => EWOULDBLOCK,
                            // std::io::ErrorKind::NotADirectory => libc::ENOTDIR,
                            // std::io::ErrorKind::IsADirectory => libc::EISDIR,
                            // std::io::ErrorKind::DirectoryNotEmpty => libc::ENOTEMPTY,
                            // std::io::ErrorKind::ReadOnlyFilesystem => libc::EROFS,
                            // std::io::ErrorKind::FilesystemLoop => libc::ELOOP,
                            // std::io::ErrorKind::StaleNetworkFileHandle => libc::ESTALE,
                            std::io::ErrorKind::InvalidInput => EIO,
                            std::io::ErrorKind::InvalidData => EIO,
                            std::io::ErrorKind::TimedOut => ETIMEDOUT,
                            std::io::ErrorKind::WriteZero => EXFULL,
                            // std::io::ErrorKind::StorageFull => libc::EXFULL,
                            // std::io::ErrorKind::NotSeekable => libc::ESPIPE,
                            // std::io::ErrorKind::FilesystemQuotaExceeded => libc::EDQUOT,
                            // std::io::ErrorKind::FileTooLarge => libc::EFBIG,
                            // std::io::ErrorKind::ResourceBusy => libc::EBUSY,
                            // std::io::ErrorKind::ExecutableFileBusy => libc::ETXTBSY,
                            // std::io::ErrorKind::Deadlock => libc::EDEADLOCK,
                            // std::io::ErrorKind::CrossesDevices => libc::EXDEV,
                            // std::io::ErrorKind::TooManyLinks => libc::EMLINK,
                            // std::io::ErrorKind::InvalidFilename => libc::EILSEQ,
                            // std::io::ErrorKind::ArgumentListTooLong => libc::EINVAL,
                            std::io::ErrorKind::Interrupted => ERESTART,
                            std::io::ErrorKind::Unsupported => ENOTSUP,
                            std::io::ErrorKind::UnexpectedEof => EIO,
                            std::io::ErrorKind::OutOfMemory => ENOMEM,
                            std::io::ErrorKind::Other => EIO,
                            _ => EIO,
                        };
                        $reply.error(eno);
                    }
                    HDFSError::EncodeError(_)
                    | HDFSError::DecodeError(_)
                    | HDFSError::ChecksumError
                    | HDFSError::NoAvailableBlock
                    | HDFSError::NoAvailableLocation
                    | HDFSError::EmptyFS => {
                        tracing::debug!("{e}");
                        $reply.error(EIO);
                    }
                    HDFSError::DataNodeError(e) => {
                        tracing::debug!("data node response error: {}", e.message());
                        $reply.error(EREMOTEIO);
                    }
                    HDFSError::NameNodeError(e) => {
                        tracing::debug!("name node response error: {}", e.error_msg());
                        $reply.error(EREMOTEIO);
                    }
                };
                return;
            }
        }
    };
    ($result:expr, & $reply:ident) => {
        handle!($result, $reply, |_| {})
    };
    ($result:expr, $reply:ident) => {
        handle!($result, $reply, |_| {
            $reply.ok();
        })
    };
}

impl fuser::Filesystem for FileSystem {
    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let parent_path = try_get!(self, parent, reply);
        let full_path = concat_name(parent_path, name);
        handle!(self.get_file_attr(full_path), reply, |attr| {
            reply.entry(&TTL, &attr, now_millis());
        })
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        let path = try_get!(self, ino, reply).clone();
        handle!(self.get_file_attr(path), reply, |attr| reply
            .attr(&TTL, &attr));
    }

    fn access(&mut self, _req: &fuser::Request<'_>, ino: u64, mask: i32, reply: fuser::ReplyEmpty) {
        let path = try_get!(self, ino, reply).clone();
        let req = CheckAccessRequestProto { path, mode: mask };
        handle!(self.client.get_rpc().check_access(req), reply);
    }

    fn symlink(
        &mut self,
        _req: &fuser::Request<'_>,
        _parent: u64,
        _link_name: &std::ffi::OsStr,
        _target: &std::path::Path,
        reply: fuser::ReplyEntry,
    ) {
        reply.error(ENOSYS);
    }

    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        let path = try_get!(self, ino, reply).clone();
        let attr = handle!(=self.get_file_attr(path.clone()), reply);
        if let Some(mode) = mode {
            let req = SetPermissionRequestProto {
                src: path.clone(),
                permission: FsPermissionProto { perm: mode },
            };
            handle!(self.client.get_rpc().set_permission(req), &reply);
        }
        if uid.is_some() || gid.is_some() {
            tracing::warn!("uid and gid is not supported");
        }
        if let Some(size) = size {
            match size.cmp(&attr.size) {
                std::cmp::Ordering::Less => {
                    let req = TruncateRequestProto {
                        src: path.clone(),
                        new_length: size,
                        client_name: self.client.client_name().to_string(),
                    };
                    handle!(self.client.get_rpc().truncate(req), &reply);
                }
                std::cmp::Ordering::Greater => {
                    let buf = [0; 819200];
                    let mut to_write = (size - attr.size) as usize;
                    let mut fd = handle!(=self.client.append(path.clone()), reply);
                    while to_write > 0 {
                        let idx = buf.len().min(to_write);
                        if let Err(e) = fd.write_all(&buf[..idx]) {
                            tracing::error!("{e}");
                            reply.error(EIO);
                            return;
                        };
                        to_write -= idx;
                    }
                }
                std::cmp::Ordering::Equal => {}
            }
        }
        let atime = atime.map(convert_time);
        let mtime = mtime.map(convert_time);
        if atime.is_some() || mtime.is_some() {
            let req = SetTimesRequestProto {
                src: path.clone(),
                mtime: mtime.unwrap_or(
                    attr.mtime
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                ),
                atime: atime.unwrap_or(
                    attr.atime
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                ),
            };
            handle!(self.client.get_rpc().set_times(req), &reply);
        }
        if ctime.is_some() {
            tracing::warn!("ctime is not supported");
        }
        if crtime.is_some() {
            tracing::warn!("crtime is not supported");
        }
        if chgtime.is_some() {
            tracing::warn!("chgtime is not supported");
        }
        if bkuptime.is_some() {
            tracing::warn!("bkuptime is not supported");
        }
        handle!(self.get_file_attr(path), reply, |attr| reply
            .attr(&TTL, &attr));
    }

    // fn readlink(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
    //     let path = try_get!(self, ino, reply).clone();
    //     let req = GetLinkTargetRequestProto { path };
    //     let (_, resp) = handle!(= self.client.get_rpc().get_link_target(req), reply);
    //     if let Some(p) = resp.target_path {
    //         reply.data(p.as_bytes());
    //     } else {
    //         reply.error(ENOENT);
    //     }
    // }

    fn mknod(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        _rdev: u32,
        reply: fuser::ReplyEntry,
    ) {
        let parent = try_get!(self, parent, reply).clone();
        let path = concat_name(&parent, name);
        let server_default = handle!(=
            self.client
                .get_rpc()
                .get_server_defaults(GetServerDefaultsRequestProto {}),
            reply
        )
        .1
        .server_defaults;
        let req = GetDatanodeReportRequestProto {
            r#type: DatanodeReportTypeProto::Live as i32,
        };
        let data_node_num = handle!(=self.client.get_rpc().get_datanode_report(req), reply)
            .1
            .di
            .len() as u32;

        let req = CreateRequestProto {
            src: path.clone(),
            masked: FsPermissionProto { perm: mode },
            client_name: self.client.client_name().to_string(),
            create_flag: 1,
            create_parent: false,
            replication: server_default.replication.min(data_node_num),
            block_size: server_default.block_size,
            unmasked: Some(FsPermissionProto { perm: umask }),
            ..Default::default()
        };
        match handle!(=self.client.get_rpc().create(req), reply).1.fs {
            Some(fs) => {
                let req = CompleteRequestProto {
                    src: path.clone(),
                    client_name: self.client.client_name().to_string(),
                    last: None,
                    file_id: fs.file_id,
                };
                handle!(self.client.get_rpc().complete(req), &reply);
                self.ino_map.insert(fs.file_id(), path);
                reply.entry(&TTL, &convert_fs(fs), now_millis())
            }
            None => {
                reply.error(EIO);
            }
        }
    }

    fn create(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let parent = try_get!(self, parent, reply).clone();
        let path = concat_name(&parent, name);
        match self.get_file_attr(path.clone()) {
            Ok(attr) => {
                reply.created(&TTL, &attr, now_millis(), 0, 0);
            }
            Err(_) => {
                let server_default = handle!(=
                    self.client
                        .get_rpc()
                        .get_server_defaults(GetServerDefaultsRequestProto {}),
                    reply
                )
                .1
                .server_defaults;
                let req = GetDatanodeReportRequestProto {
                    r#type: DatanodeReportTypeProto::Live as i32,
                };
                let data_node_num = handle!(=self.client.get_rpc().get_datanode_report(req), reply)
                    .1
                    .di
                    .len() as u32;
                let req = CreateRequestProto {
                    src: path.clone(),
                    masked: FsPermissionProto { perm: mode },
                    client_name: self.client.client_name().to_string(),
                    create_flag: 1,
                    create_parent: false,
                    replication: server_default.replication.min(data_node_num),
                    block_size: server_default.block_size,
                    unmasked: Some(FsPermissionProto { perm: umask }),
                    ..Default::default()
                };
                match handle!(=self.client.get_rpc().create(req), reply).1.fs {
                    Some(fs) => {
                        let req = CompleteRequestProto {
                            src: path.clone(),
                            client_name: self.client.client_name().to_string(),
                            last: None,
                            file_id: fs.file_id,
                        };
                        handle!(self.client.get_rpc().complete(req), &reply);
                        self.ino_map.insert(fs.file_id(), path);
                        reply.created(&TTL, &convert_fs(fs), now_millis(), 0, 0)
                    }
                    None => {
                        reply.error(EIO);
                    }
                }
            }
        }
    }

    fn mkdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let parent = try_get!(self, parent, reply);
        let path = concat_name(parent, name);
        let req = hdfs_client::types::hdfs::MkdirsRequestProto {
            src: path.clone(),
            masked: hdfs_client::types::hdfs::FsPermissionProto { perm: mode },
            create_parent: false,
            unmasked: Some(hdfs_client::types::hdfs::FsPermissionProto { perm: umask }),
        };
        handle!(self.client.get_rpc().mkdirs(req), &reply);
        handle!(self.get_file_attr(path), reply, |attr| reply.entry(
            &TTL,
            &attr,
            now_millis()
        ));
    }

    fn rmdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let parent = try_get!(self, parent, reply);
        let src = concat_name(parent, name);
        let req = hdfs_client::types::hdfs::DeleteRequestProto {
            src,
            recursive: true,
        };
        handle!(self.client.get_rpc().delete(req), reply);
    }

    fn rename(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        newparent: u64,
        newname: &std::ffi::OsStr,
        _flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let current_parent = try_get!(self, parent, reply);
        let current_name = concat_name(current_parent, name);
        let new_parent = try_get!(self, newparent, reply);
        let new_name = concat_name(new_parent, newname);
        let req = Rename2RequestProto {
            src: current_name,
            dst: new_name,
            overwrite_dest: true,
            move_to_trash: Some(true),
        };
        handle!(self.client.get_rpc().rename2(req), reply);
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let path = try_get!(self, ino, reply);
        let mut fd = handle!(= self.client.open(path), reply);
        if offset != 0 {
            if let Err(e) = fd.seek(std::io::SeekFrom::Start(offset as u64)) {
                tracing::warn!("{e}");
                reply.error(EIO);
                return;
            }
        }
        let max_length = fd.metadata().length - offset as u64;
        let max_size = (size as usize).min(max_length as usize);
        let mut data = vec![0; max_size];
        match fd.read_exact(&mut data) {
            Ok(_) => {
                reply.data(&data);
            }
            Err(e) => {
                tracing::error!("{e}");
                reply.error(EIO);
            }
        };
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let path = try_get!(self, ino, reply).clone();
        let attr = handle!(=self.get_file_attr(path.clone()), reply);
        let offset = offset as u64;
        if offset < attr.size {
            let req = TruncateRequestProto {
                src: path.clone(),
                new_length: offset,
                client_name: self.client.client_name().to_string(),
            };
            handle!(self.client.get_rpc().truncate(req), &reply);
        }
        let mut fd = handle!(= self.client.append(&path), reply);
        if offset > attr.size {
            let buf = [0; 8192];
            let mut to_write = (offset - attr.size) as usize;
            let mut fd = handle!(=self.client.append(path.clone()), reply);
            while to_write > 0 {
                let idx = buf.len().min(to_write);
                if let Err(e) = fd.write_all(&buf[..idx]) {
                    tracing::error!("{e}");
                    reply.error(EIO);
                    return;
                };
                to_write -= idx;
            }
        }
        match fd
            .write_all(data)
            .and_then(|_| fd.close().map_err(|e| e.into()))
        {
            Ok(_) => {
                reply.written(data.len() as u32);
            }
            Err(e) => {
                tracing::warn!("{e}");
                reply.error(EIO)
            }
        }
    }

    // fn fsync(
    //     &mut self,
    //     _req: &fuser::Request<'_>,
    //     ino: u64,
    //     _fh: u64,
    //     _datasync: bool,
    //     reply: fuser::ReplyEmpty,
    // ) {
    //     let src = try_get!(self, ino, reply).clone();
    //     let req = FsyncRequestProto {
    //         src,
    //         client: self.client.client_name().to_string(),
    //         last_block_length: None,
    //         file_id: None,
    //     };
    //     handle!(self.client.get_rpc().fsync(req), reply);
    // }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        if offset != 0 {
            reply.ok();
            return;
        }
        let path = try_get!(self, ino, reply).clone();
        let items = handle!(= self.client.read_dir(&path), reply);
        for (idx, item) in items.into_iter().enumerate() {
            let name = String::from_utf8(item.path.clone()).unwrap();
            let full_name = if path == "/" {
                format!("/{name}")
            } else {
                format!("{path}/{name}")
            };
            self.ino_map.insert(item.file_id(), full_name.clone());
            if reply.add(
                item.file_id(),
                idx as i64 + 1,
                convert_ty(item.file_type()),
                name,
            ) {
                break;
            };
        }
        reply.ok();
    }

    fn readdirplus(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectoryPlus,
    ) {
        if offset != 0 {
            reply.ok();
            return;
        }
        let path = try_get!(self, ino, reply).clone();
        let items = handle!(= self.client.read_dir(&path), reply);
        for (idx, item) in items.into_iter().enumerate() {
            let name = String::from_utf8(item.path.clone()).unwrap();
            let full_name = if path == "/" {
                format!("/{name}")
            } else {
                format!("{path}/{name}")
            };
            self.ino_map.insert(item.file_id(), full_name.clone());

            if reply.add(
                item.file_id(),
                idx as i64 + 1,
                name,
                &TTL,
                &convert_fs(item),
                now_millis(),
            ) {
                break;
            };
        }
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let src = try_get!(self, ino, reply).clone();
        let req = FsyncRequestProto {
            src,
            client: self.client.client_name().to_string(),
            last_block_length: None,
            file_id: None,
        };
        handle!(self.client.get_rpc().fsync(req), reply);
    }

    fn statfs(&mut self, _req: &fuser::Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        let stat =
            handle!(=self.client.get_rpc().get_fs_stats(GetFsStatusRequestProto {}), reply).1;
        reply.statfs(stat.capacity, stat.remaining, stat.remaining, 0, 0, 0, 0, 0);
    }

    // fn setxattr(
    //     &mut self,
    //     _req: &fuser::Request<'_>,
    //     ino: u64,
    //     name: &std::ffi::OsStr,
    //     value: &[u8],
    //     _flags: i32,
    //     _position: u32,
    //     reply: fuser::ReplyEmpty,
    // ) {
    //     let path = try_get!(self, ino, reply);
    //     let req = SetXAttrRequestProto {
    //         src: path.clone(),
    //         x_attr: Some(XAttrProto {
    //             namespace: 0,
    //             name: name.to_string_lossy().to_string(),
    //             value: Some(value.to_vec()),
    //         }),
    //         flag: None,
    //     };
    //     handle!(self.client.get_rpc().set_x_attr(req), reply);
    // }

    // fn getxattr(
    //     &mut self,
    //     _req: &fuser::Request<'_>,
    //     ino: u64,
    //     name: &std::ffi::OsStr,
    //     size: u32,
    //     reply: fuser::ReplyXattr,
    // ) {
    //     let path = try_get!(self, ino, reply);
    //     let req = GetXAttrsRequestProto {
    //         src: path.clone(),
    //         x_attrs: vec![XAttrProto {
    //             namespace: 0,
    //             name: name.to_string_lossy().to_string(),
    //             value: None,
    //         }],
    //     };
    //     let x_attrs = handle!(= self.client.get_rpc().get_x_attrs(req), reply)
    //         .1
    //         .x_attrs;
    //     if let Some(x) = x_attrs.first() {
    //         let v_len = x.value().len() as u32;
    //         if size == 0 {
    //             reply.size(v_len);
    //         } else {
    //             if v_len > size {
    //                 reply.error(ERANGE)
    //             } else {
    //                 reply.data(x.value())
    //             }
    //         }
    //     } else {
    //         reply.error(EIO)
    //     }
    // }

    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let parent = try_get!(self, parent, reply);
        let src = concat_name(parent, name);
        let req = DeleteRequestProto {
            src,
            recursive: false,
        };
        handle!(self.client.get_rpc().delete(req), reply);
    }
}

fn convert_time(t: TimeOrNow) -> u64 {
    match t {
        TimeOrNow::SpecificTime(st) => st
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_else(|_| now_millis()),
        TimeOrNow::Now => now_millis(),
    }
}

fn concat_name(current_parent: &String, name: &std::ffi::OsStr) -> String {
    if current_parent == "/" {
        format!("/{}", name.to_string_lossy())
    } else {
        format!("{current_parent}/{}", name.to_string_lossy())
    }
}
