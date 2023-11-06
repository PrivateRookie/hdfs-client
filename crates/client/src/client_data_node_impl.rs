use prost::Message;

use crate::protocol::common::*;
use crate::protocol::hdfs::*;

use crate::IpcConnection;
use crate::IpcError;

impl IpcConnection {
    pub fn get_replica_visible_length(
        &mut self,
        req: GetReplicaVisibleLengthRequestProto,
    ) -> Result<(RpcResponseHeaderProto, GetReplicaVisibleLengthResponseProto), IpcError> {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("getReplicaVisibleLength", &req)?;
        let resp = GetReplicaVisibleLengthResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }

    pub fn refresh_namenodes(
        &mut self,
        req: RefreshNamenodesRequestProto,
    ) -> Result<(RpcResponseHeaderProto, RefreshNamenodesResponseProto), IpcError> {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("refreshNamenodes", &req)?;
        let resp = RefreshNamenodesResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }

    pub fn delete_block_pool(
        &mut self,
        req: DeleteBlockPoolRequestProto,
    ) -> Result<(RpcResponseHeaderProto, DeleteBlockPoolResponseProto), IpcError> {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("deleteBlockPool", &req)?;
        let resp = DeleteBlockPoolResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }

    pub fn get_block_local_path_info(
        &mut self,
        req: GetBlockLocalPathInfoRequestProto,
    ) -> Result<(RpcResponseHeaderProto, GetBlockLocalPathInfoResponseProto), IpcError> {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("getBlockLocalPathInfo", &req)?;
        let resp = GetBlockLocalPathInfoResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }

    pub fn shutdown_datanode(
        &mut self,
        req: ShutdownDatanodeRequestProto,
    ) -> Result<(RpcResponseHeaderProto, ShutdownDatanodeResponseProto), IpcError> {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("shutdownDatanode", &req)?;
        let resp = ShutdownDatanodeResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }

    pub fn evict_writers(
        &mut self,
        req: EvictWritersRequestProto,
    ) -> Result<(RpcResponseHeaderProto, EvictWritersResponseProto), IpcError> {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("evictWriters", &req)?;
        let resp = EvictWritersResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }

    pub fn get_datanode_info(
        &mut self,
        req: GetDatanodeInfoRequestProto,
    ) -> Result<(RpcResponseHeaderProto, GetDatanodeInfoResponseProto), IpcError> {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("getDatanodeInfo", &req)?;
        let resp = GetDatanodeInfoResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }

    pub fn get_volume_report(
        &mut self,
        req: GetVolumeReportRequestProto,
    ) -> Result<(RpcResponseHeaderProto, GetVolumeReportResponseProto), IpcError> {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("getVolumeReport", &req)?;
        let resp = GetVolumeReportResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }

    pub fn get_reconfiguration_status(
        &mut self,
        req: GetReconfigurationStatusRequestProto,
    ) -> Result<
        (
            RpcResponseHeaderProto,
            GetReconfigurationStatusResponseProto,
        ),
        IpcError,
    > {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("getReconfigurationStatus", &req)?;
        let resp = GetReconfigurationStatusResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }

    pub fn start_reconfiguration(
        &mut self,
        req: StartReconfigurationRequestProto,
    ) -> Result<(RpcResponseHeaderProto, StartReconfigurationResponseProto), IpcError> {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("startReconfiguration", &req)?;
        let resp = StartReconfigurationResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }

    pub fn trigger_block_report(
        &mut self,
        req: TriggerBlockReportRequestProto,
    ) -> Result<(RpcResponseHeaderProto, TriggerBlockReportResponseProto), IpcError> {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("triggerBlockReport", &req)?;
        let resp = TriggerBlockReportResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }

    pub fn get_balancer_bandwidth(
        &mut self,
        req: GetBalancerBandwidthRequestProto,
    ) -> Result<(RpcResponseHeaderProto, GetBalancerBandwidthResponseProto), IpcError> {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("getBalancerBandwidth", &req)?;
        let resp = GetBalancerBandwidthResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }

    pub fn get_disk_balancer_setting(
        &mut self,
        req: DiskBalancerSettingRequestProto,
    ) -> Result<(RpcResponseHeaderProto, DiskBalancerSettingResponseProto), IpcError> {
        let req = req.encode_length_delimited_to_vec();
        let (header, resp) = self.send_raw_req("getDiskBalancerSetting", &req)?;
        let resp = DiskBalancerSettingResponseProto::decode_length_delimited(resp)?;
        Ok((header, resp))
    }
}
