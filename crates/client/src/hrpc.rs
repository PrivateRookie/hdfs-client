use std::io::{Read, Write};

use hdfs_types::common::*;
use hdfs_types::hdfs::*;
use prost::{
    bytes::{BufMut, BytesMut},
    EncodeError, Message,
};

use crate::HDFSError;

const PROTOCOL: &str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";

fn into_err(error: RpcResponseHeaderProto) -> Result<RpcResponseHeaderProto, HDFSError> {
    if error.status() != rpc_response_header_proto::RpcStatusProto::Success {
        Err(HDFSError::NameNodeError(Box::new(error)))
    } else {
        Ok(error)
    }
}

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

/// hdfs rpc
pub struct HRpc<S> {
    pub(crate) stream: S,
    pub(crate) call_id: i32,
    pub(crate) client_id: Vec<u8>,
    pub(crate) context: Option<RpcCallerContextProto>,
}

impl<S: Write + Read> HRpc<S> {
    pub fn connect(
        mut stream: S,
        effective_user: impl Into<Option<String>>,
        real_user: impl Into<Option<String>>,
        context: impl Into<Option<RpcCallerContextProto>>,
        handshake: impl Into<Option<Handshake>>,
    ) -> Result<Self, HDFSError> {
        let client_id = uuid::Uuid::new_v4().to_bytes_le().to_vec();
        let context = context.into();
        let mut buf = BytesMut::new();
        let handshake = handshake.into().unwrap_or_default();
        handshake.encode(&mut buf)?;
        let req_header = RpcRequestHeaderProto {
            rpc_kind: Some(RpcKindProto::RpcProtocolBuffer as i32),
            rpc_op: Some(rpc_request_header_proto::OperationProto::RpcFinalPacket as i32),
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
                effective_user: effective_user.into(),
                real_user: real_user.into(),
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
    ) -> Result<(RpcResponseHeaderProto, BytesMut), HDFSError> {
        let call_id = self.call_id;
        let rpc_req_header = RpcRequestHeaderProto {
            rpc_kind: Some(RpcKindProto::RpcProtocolBuffer as i32),
            rpc_op: Some(rpc_request_header_proto::OperationProto::RpcFinalPacket as i32),
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
        self.stream.flush()?;
        self.read_resp()
    }

    pub(crate) fn read_resp(&mut self) -> Result<(RpcResponseHeaderProto, BytesMut), HDFSError> {
        let mut raw_length = [0u8; 4];
        self.stream.read_exact(&mut raw_length)?;
        let length = u32::from_be_bytes(raw_length) as usize;
        let mut buf = BytesMut::with_capacity(length);
        buf.resize(length, 0);
        self.stream.read_exact(&mut buf)?;
        let header_length = prost::encoding::decode_varint(&mut buf)?;
        let header_bytes = buf.split_to(header_length as usize);
        let resp_header = RpcResponseHeaderProto::decode(header_bytes)?;
        // into error if status is not success
        let resp_header = into_err(resp_header)?;
        Ok((resp_header, buf))
    }
}

macro_rules! method {
    ($name:ident, $raw:literal, $req:ty, $resp:ty) => {
        pub fn $name(&mut self, req: $req) -> Result<(RpcResponseHeaderProto, $resp), HDFSError> {
            #[cfg(feature = "trace_dbg")]
            {
                tracing::trace!(target: "hrpc", "\nmethod: {}\nreq: {:#?}", $raw, req);
            }
            #[cfg(feature = "trace_valuable")]
            {
                use valuable::Valuable;
                tracing::trace!(target: "hrpc", method=$raw, req=req.as_value());
            }
            let req = req.encode_length_delimited_to_vec();
            let (header, resp) = self.send_raw_req($raw, &req)?;
            let resp = <$resp>::decode_length_delimited(resp)?;
            #[cfg(feature = "trace_dbg")]
            {
                tracing::trace!(target: "hrpc", "\nmethod: {}\nheader: {:#?}\nresp: {:#?}", $raw, header, resp);
            }
            #[cfg(feature = "trace_valuable")]
            {
                use valuable::Valuable;
                tracing::trace!(target: "hrpc", method=$raw, header=header.as_value(), resp=resp.as_value() );
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

// data node methods
method! { HRpc<S> =>
    get_replica_visible_length, "getReplicaVisibleLength", GetReplicaVisibleLengthRequestProto, GetReplicaVisibleLengthResponseProto;
    refresh_namenodes, "refreshNamenodes", RefreshNamenodesRequestProto, RefreshNamenodesResponseProto;
    delete_block_pool, "deleteBlockPool", DeleteBlockPoolRequestProto, DeleteBlockPoolResponseProto;
    get_block_local_path_info, "getBlockLocalPathInfo", GetBlockLocalPathInfoRequestProto, GetBlockLocalPathInfoResponseProto;
    shutdown_datanode, "shutdownDatanode", ShutdownDatanodeRequestProto, ShutdownDatanodeResponseProto;
    evict_writers, "evictWriters", EvictWritersRequestProto, EvictWritersResponseProto;
    get_datanode_info, "getDatanodeInfo", GetDatanodeInfoRequestProto, GetDatanodeInfoResponseProto;
    get_volume_report, "getVolumeReport", GetVolumeReportRequestProto, GetVolumeReportResponseProto;
    start_reconfiguration, "startReconfiguration", StartReconfigurationRequestProto, StartReconfigurationResponseProto;
    trigger_block_report, "triggerBlockReport", TriggerBlockReportRequestProto, TriggerBlockReportResponseProto;
    get_balancer_bandwidth, "getBalancerBandwidth", GetBalancerBandwidthRequestProto, GetBalancerBandwidthResponseProto;
    get_reconfiguration_status, "getReconfigurationStatus", GetReconfigurationStatusRequestProto, GetReconfigurationStatusResponseProto;
    get_disk_balancer_setting, "getDiskBalancerSetting", DiskBalancerSettingRequestProto, DiskBalancerSettingResponseProto;
}

// name node methods
method! { HRpc<S> =>
    create, "create", CreateRequestProto, CreateResponseProto;
    get_server_defaults, "getServerDefaults", GetServerDefaultsRequestProto,GetServerDefaultsResponseProto;
    get_block_locations, "getBlockLocations", GetBlockLocationsRequestProto, GetBlockLocationsResponseProto;
    append, "append", AppendRequestProto, AppendResponseProto;
    set_replication, "setReplication", SetReplicationRequestProto, SetReplicationResponseProto;
    set_storage_policy, "setStoragePolicy", SetStoragePolicyRequestProto, SetStoragePolicyResponseProto;
    unset_storage_policy, "unsetStoragePolicy", UnsetStoragePolicyRequestProto, UnsetStoragePolicyResponseProto;
    get_storage_policy, "getStoragePolicy", GetStoragePolicyRequestProto, GetStoragePolicyResponseProto;
    get_storage_policies, "getStoragePolicies", GetStoragePoliciesRequestProto, GetStoragePoliciesResponseProto;
    set_permission, "setPermission", SetPermissionRequestProto, SetPermissionResponseProto;
    set_owner, "setOwner", SetOwnerRequestProto, SetOwnerResponseProto;
    abandon_block, "abandonBlock", AbandonBlockRequestProto, AbandonBlockResponseProto;
    add_block, "addBlock", AddBlockRequestProto, AddBlockResponseProto;
    get_additional_datanode, "getAdditionalDatanode", GetAdditionalDatanodeRequestProto, GetAdditionalDatanodeResponseProto;
    complete, "complete", CompleteRequestProto, CompleteResponseProto;
    report_bad_blocks, "reportBadBlocks", ReportBadBlocksRequestProto, ReportBadBlocksResponseProto;
    concat, "concat", ConcatRequestProto, ConcatResponseProto;
    truncate, "truncate", TruncateRequestProto, TruncateResponseProto;
    rename, "rename", RenameRequestProto, RenameResponseProto;
    rename2, "rename2", Rename2RequestProto, Rename2ResponseProto;
    delete, "delete", DeleteRequestProto, DeleteResponseProto;
    mkdirs, "mkdirs", MkdirsRequestProto, MkdirsResponseProto;
    get_listing, "getListing", GetListingRequestProto, GetListingResponseProto;
    renew_lease, "renewLease", RenewLeaseRequestProto, RenewLeaseResponseProto;
    recover_lease, "recoverLease", RecoverLeaseRequestProto, RecoverLeaseResponseProto;
    get_fs_stats, "getFsStats", GetFsStatusRequestProto, GetFsStatsResponseProto;
    get_datanode_report, "getDatanodeReport", GetDatanodeReportRequestProto, GetDatanodeReportResponseProto;
    get_preferred_block_size, "getPreferredBlockSize", GetPreferredBlockSizeRequestProto, GetPreferredBlockSizeResponseProto;
    set_safe_mode, "setSafeMode", SetSafeModeRequestProto, SetSafeModeResponseProto;
    save_namespace, "saveNamespace", SaveNamespaceRequestProto, SaveNamespaceResponseProto;
    roll_edits, "rollEdits", RollEditsRequestProto, RollEditsResponseProto;
    restore_failed_storage, "restoreFailedStorage", RestoreFailedStorageRequestProto, RestoreFailedStorageResponseProto;
    refresh_nodes, "refreshNodes", RefreshNodesRequestProto, RefreshNodesResponseProto;
    finalize_upgrade, "finalizeUpgrade", FinalizeUpgradeRequestProto, FinalizeUpgradeResponseProto;
    upgrade_status, "upgradeStatus", UpgradeStatusRequestProto, UpgradeStatusResponseProto;
    rolling_upgrade, "rollingUpgrade", RollingUpgradeRequestProto, RollingUpgradeResponseProto;
    list_corrupt_file_blocks, "listCorruptFileBlocks", ListCorruptFileBlocksRequestProto, ListCorruptFileBlocksResponseProto;
    meta_save, "metaSave", MetaSaveRequestProto, MetaSaveResponseProto;
    get_file_info, "getFileInfo", GetFileInfoRequestProto, GetFileInfoResponseProto;
    get_located_file_info, "getLocatedFileInfo", GetLocatedFileInfoRequestProto, GetLocatedFileInfoResponseProto;
    add_cache_pool, "addCachePool", AddCachePoolRequestProto, AddCachePoolResponseProto;
    modify_cache_pool, "modifyCachePool", ModifyCachePoolRequestProto, ModifyCachePoolResponseProto;
    remove_cache_pool, "removeCachePool", RemoveCachePoolRequestProto, RemoveCachePoolResponseProto;
    list_cache_pools, "listCachePools", ListCachePoolsRequestProto, ListCachePoolsResponseProto;
    get_file_link_info, "getFileLinkInfo", GetFileLinkInfoRequestProto, GetFileLinkInfoResponseProto;
    get_content_summary, "getContentSummary", GetContentSummaryRequestProto, GetContentSummaryResponseProto;
    set_quota, "setQuota", SetQuotaRequestProto, SetQuotaResponseProto;
    fsync, "fsync", FsyncRequestProto, FsyncResponseProto;
    set_times, "setTimes", SetTimesRequestProto, SetTimesResponseProto;
    create_symlink, "createSymlink", CreateSymlinkRequestProto, CreateSymlinkResponseProto;
    get_link_target, "getLinkTarget", GetLinkTargetRequestProto, GetLinkTargetResponseProto;
    update_block_for_pipeline, "updateBlockForPipeline", UpdateBlockForPipelineRequestProto, UpdateBlockForPipelineResponseProto;
    update_pipeline, "updatePipeline", UpdatePipelineRequestProto, UpdatePipelineResponseProto;
    set_balancer_bandwidth, "setBalancerBandwidth", SetBalancerBandwidthRequestProto, SetBalancerBandwidthResponseProto;
    get_data_encryption_key, "getDataEncryptionKey", GetDataEncryptionKeyRequestProto, GetDataEncryptionKeyResponseProto;
    create_snapshot, "createSnapshot", CreateSnapshotRequestProto, CreateSnapshotResponseProto;
    rename_snapshot, "renameSnapshot", RenameSnapshotRequestProto, RenameSnapshotResponseProto;
    allow_snapshot, "allowSnapshot", AllowSnapshotRequestProto, AllowSnapshotResponseProto;
    disallow_snapshot, "disallowSnapshot", DisallowSnapshotRequestProto, DisallowSnapshotResponseProto;
    get_snapshot_listing, "getSnapshotListing", GetSnapshotListingRequestProto, GetSnapshotListingResponseProto;
    delete_snapshot, "deleteSnapshot", DeleteSnapshotRequestProto, DeleteSnapshotResponseProto;
    get_snapshot_diff_report, "getSnapshotDiffReport", GetSnapshotDiffReportRequestProto, GetSnapshotDiffReportResponseProto;
    is_file_closed, "isFileClosed", IsFileClosedRequestProto, IsFileClosedResponseProto;
    modify_acl_entries, "modifyAclEntries", ModifyAclEntriesRequestProto, ModifyAclEntriesResponseProto;
    remove_acl_entries, "removeAclEntries", RemoveAclEntriesRequestProto, RemoveAclEntriesResponseProto;
    remove_default_acl, "removeDefaultAcl", RemoveDefaultAclRequestProto, RemoveDefaultAclResponseProto;
    remove_acl, "removeAcl", RemoveAclRequestProto, RemoveAclResponseProto;
    set_acl, "setAcl", SetAclRequestProto, SetAclResponseProto;
    get_acl_status, "getAclStatus", GetAclStatusRequestProto, GetAclStatusResponseProto;
    set_x_attr, "setXAttr", SetXAttrRequestProto, SetXAttrResponseProto;
    get_x_attrs, "getXAttrs", GetXAttrsRequestProto, GetXAttrsResponseProto;
    list_x_attrs, "listXAttrs", ListXAttrsRequestProto, ListXAttrsResponseProto;
    remove_x_attr, "removeXAttr", RemoveXAttrRequestProto, RemoveXAttrResponseProto;
    check_access, "checkAccess", CheckAccessRequestProto, CheckAccessResponseProto;
    create_encryption_zone, "createEncryptionZone", CreateEncryptionZoneRequestProto, CreateEncryptionZoneResponseProto;
    list_encryption_zones, "listEncryptionZones", ListEncryptionZonesRequestProto, ListEncryptionZonesResponseProto;
    reencrypt_encryption_zone, "reencryptEncryptionZone", ReencryptEncryptionZoneRequestProto, ReencryptEncryptionZoneResponseProto;
    list_reencryption_status, "listReencryptionStatus", ListReencryptionStatusRequestProto, ListReencryptionStatusResponseProto;
    get_e_z_for_path, "getEZForPath", GetEzForPathRequestProto, GetEzForPathResponseProto;
    set_erasure_coding_policy, "setErasureCodingPolicy", SetErasureCodingPolicyRequestProto, SetErasureCodingPolicyResponseProto;
    get_current_edit_log_txid, "getCurrentEditLogTxid", GetCurrentEditLogTxidRequestProto, GetCurrentEditLogTxidResponseProto;
    get_edits_from_txid, "getEditsFromTxid", GetEditsFromTxidRequestProto, GetEditsFromTxidResponseProto;
    get_erasure_coding_policy, "getErasureCodingPolicy", GetErasureCodingPolicyRequestProto, GetErasureCodingPolicyResponseProto;
    get_erasure_coding_codecs, "getErasureCodingCodecs", GetErasureCodingCodecsRequestProto, GetErasureCodingCodecsResponseProto;
    get_quota_usage, "getQuotaUsage", GetQuotaUsageRequestProto, GetQuotaUsageResponseProto;
    list_open_files, "listOpenFiles", ListOpenFilesRequestProto, ListOpenFilesResponseProto;
    msync, "msync", MsyncRequestProto, MsyncResponseProto;
    satisfy_storage_policy, "satisfyStoragePolicy", SatisfyStoragePolicyRequestProto, SatisfyStoragePolicyResponseProto;
    get_ha_service_state, "getHAServiceState", HaServiceStateRequestProto, HaServiceStateResponseProto;
    get_datanode_storage_report, "getDatanodeStorageReport", GetDatanodeStorageReportRequestProto, GetDatanodeStorageReportResponseProto;
    get_snapshottable_dir_listing, "getSnapshottableDirListing", GetSnapshottableDirListingRequestProto, GetSnapshottableDirListingResponseProto;
    get_snapshot_diff_report_listing, "getSnapshotDiffReportListing", GetSnapshotDiffReportListingRequestProto, GetSnapshotDiffReportListingResponseProto;
    unset_erasure_coding_policy, "unsetErasureCodingPolicy", UnsetErasureCodingPolicyRequestProto, UnsetErasureCodingPolicyResponseProto;
    get_e_c_topology_result_for_policies, "getECTopologyResultForPolicies", GetEcTopologyResultForPoliciesRequestProto, GetEcTopologyResultForPoliciesResponseProto;
    get_erasure_coding_policies, "getErasureCodingPolicies", GetErasureCodingPoliciesRequestProto, GetErasureCodingPoliciesResponseProto;
    add_erasure_coding_policies, "addErasureCodingPolicies", AddErasureCodingPoliciesRequestProto, AddErasureCodingPoliciesResponseProto;
    remove_erasure_coding_policy, "removeErasureCodingPolicy", RemoveErasureCodingPolicyRequestProto, RemoveErasureCodingPolicyResponseProto;
    enable_erasure_coding_policy, "enableErasureCodingPolicy", EnableErasureCodingPolicyRequestProto, EnableErasureCodingPolicyResponseProto;
    disable_erasure_coding_policy, "disableErasureCodingPolicy", DisableErasureCodingPolicyRequestProto, DisableErasureCodingPolicyResponseProto;
    get_slow_datanode_report, "getSlowDatanodeReport", GetSlowDatanodeReportRequestProto, GetSlowDatanodeReportResponseProto;
}
