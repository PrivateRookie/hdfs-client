use prost::Message;

use hdfs_types::common::*;
use hdfs_types::hdfs::*;

use super::method;
use crate::IpcConnection;
use crate::IpcError;

super::method! { IpcConnection<S> =>
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
