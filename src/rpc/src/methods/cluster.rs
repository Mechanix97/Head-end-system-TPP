use serde_json::{Value, json};

use crate::error::RpcError;
use crate::protocol::JsonRpcResponse;
use crate::state::RpcState;

pub async fn peers(state: &RpcState, _params: Value, id: Value) -> JsonRpcResponse {
    let cluster = match &state.cluster_handle {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(id, &RpcError::ClusterNotEnabled);
        }
    };

    let m = cluster.membership.read().await;
    let nodes: Vec<Value> = m
        .nodes()
        .values()
        .map(|n| {
            json!({
                "node_id": n.node_id.to_string(),
                "node_name": n.node_name,
                "cluster_addr": n.cluster_addr.to_string(),
                "status": format!("{:?}", n.status),
                "device_count": n.device_count,
                "load_percent": n.load_percent,
                "last_heartbeat": n.last_heartbeat.to_rfc3339(),
            })
        })
        .collect();

    JsonRpcResponse::success(id, json!({ "peers": nodes }))
}

pub async fn status(state: &RpcState, _params: Value, id: Value) -> JsonRpcResponse {
    let cluster = match &state.cluster_handle {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(id, &RpcError::ClusterNotEnabled);
        }
    };

    let m = cluster.membership.read().await;
    let all: Vec<_> = m.nodes().values().collect();
    let active_count = all.iter().filter(|n| {
        matches!(n.status, cluster::node::NodeStatus::Active)
    }).count();
    // Peer device counts come from heartbeats; local node never heartbeats itself,
    // so read local count directly from DeviceManager.
    let local_device_count = {
        let dm = state.device_manager.read().await;
        dm.device_count() as u32
    };
    let peer_devices: u32 = all
        .iter()
        .filter(|n| n.node_id != cluster.node_id)
        .map(|n| n.device_count)
        .sum();
    let total_devices = local_device_count + peer_devices;

    JsonRpcResponse::success(
        id,
        json!({
            "total_nodes": all.len(),
            "active_nodes": active_count,
            "total_devices": total_devices,
            "local_node_id": cluster.node_id.to_string(),
        }),
    )
}
