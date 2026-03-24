use std::net::SocketAddr;

use serde_json::{Value, json};

use cluster::membership::send_message;
use cluster::protocol::ClusterMessage;
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

pub async fn join(state: &RpcState, params: Value, id: Value) -> JsonRpcResponse {
    let cluster = match &state.cluster_handle {
        Some(h) => h,
        None => return JsonRpcResponse::error(id, &RpcError::ClusterNotEnabled),
    };

    let seed_str = match params.get("seed").and_then(Value::as_str) {
        Some(s) => s.to_string(),
        None => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams("missing 'seed' (expected 'ip:port')".to_string()),
            )
        }
    };

    let seed_addr: SocketAddr = match seed_str.parse() {
        Ok(a) => a,
        Err(_) => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams(format!("'{seed_str}' is not a valid ip:port address")),
            )
        }
    };

    // Build NODE_JOIN from current local node state
    let (node_id, seq, payload) = {
        let mut m = cluster.membership.write().await;
        let node_id = m.local_node_id();
        let seq = m.next_seq();
        let payload = m.create_node_join_payload();
        (node_id, seq, payload)
    };

    let msg = ClusterMessage::node_join(node_id, seq, payload);

    if let Err(e) = send_message(&cluster.socket, seed_addr, msg).await {
        return JsonRpcResponse::error(
            id,
            &RpcError::Internal(format!("failed to send NODE_JOIN to {seed_addr}: {e}")),
        );
    }

    // Persist the seed so it survives restarts
    let current_seeds = state
        .config_store
        .get_config_value("cluster_seeds")
        .await
        .ok()
        .flatten()
        .unwrap_or_default();

    let new_seeds = if current_seeds.is_empty() {
        seed_str.clone()
    } else if current_seeds.split(',').any(|s| s.trim() == seed_str) {
        current_seeds // already present
    } else {
        format!("{current_seeds},{seed_str}")
    };

    if let Err(e) = state
        .config_store
        .set_config_value("cluster_seeds", &new_seeds)
        .await
    {
        tracing::warn!("NODE_JOIN sent but failed to persist seed: {e}");
    }

    JsonRpcResponse::success(
        id,
        json!({
            "status": "join_requested",
            "seed": seed_str,
            "note": "NODE_JOIN sent — peer list updates via incoming heartbeats",
        }),
    )
}
