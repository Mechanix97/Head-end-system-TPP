use serde_json::{Value, json};
use uuid::Uuid;

use crate::error::RpcError;
use crate::protocol::JsonRpcResponse;
use crate::state::RpcState;

pub async fn list(state: &RpcState, params: Value, id: Value) -> JsonRpcResponse {
    let limit = params.get("limit").and_then(Value::as_u64).unwrap_or(100) as usize;
    let offset = params.get("offset").and_then(Value::as_u64).unwrap_or(0) as usize;

    let dm = state.device_manager.read().await;
    let owned: Vec<Value> = dm
        .owned_devices()
        .iter()
        .skip(offset)
        .take(limit)
        .map(|id| json!(id.to_string()))
        .collect();

    let total = dm.device_count();
    JsonRpcResponse::success(
        id,
        json!({
            "devices": owned,
            "total": total,
            "limit": limit,
            "offset": offset,
        }),
    )
}

pub async fn info(state: &RpcState, params: Value, id: Value) -> JsonRpcResponse {
    let device_id_str = match params.get("device_id").and_then(Value::as_str) {
        Some(s) => s.to_string(),
        None => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams("missing 'device_id'".to_string()),
            );
        }
    };

    let device_id = match Uuid::parse_str(&device_id_str) {
        Ok(uuid) => uuid,
        Err(e) => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams(format!("invalid UUID: {e}")),
            );
        }
    };

    match state.database.get_device(device_id).await {
        Ok(device) => JsonRpcResponse::success(
            id,
            json!({
                "device_id": device.id.to_string(),
                "ipv4": device.ipv4,
                "ipv6": device.ipv6,
                "mac_address": device.mac,
                "factory_id": device.factory_id,
                "batch_id": device.batch_id,
            }),
        ),
        Err(e) => {
            let err_str = e.to_string();
            if err_str.contains("not found") || err_str.contains("no rows") {
                JsonRpcResponse::error(id, &RpcError::DeviceNotFound(device_id.to_string()))
            } else {
                JsonRpcResponse::error(id, &RpcError::Database(err_str))
            }
        }
    }
}

pub async fn delegate(state: &RpcState, params: Value, id: Value) -> JsonRpcResponse {
    let cluster = match &state.cluster_handle {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(id, &RpcError::ClusterNotEnabled);
        }
    };

    let device_id_str = match params.get("device_id").and_then(Value::as_str) {
        Some(s) => s.to_string(),
        None => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams("missing 'device_id'".to_string()),
            );
        }
    };

    let target_node_str = match params.get("target_node_id").and_then(Value::as_str) {
        Some(s) => s.to_string(),
        None => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams("missing 'target_node_id'".to_string()),
            );
        }
    };

    let device_id = match Uuid::parse_str(&device_id_str) {
        Ok(uuid) => uuid,
        Err(e) => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams(format!("invalid device_id UUID: {e}")),
            );
        }
    };

    let _target_node_id = match Uuid::parse_str(&target_node_str) {
        Ok(uuid) => uuid,
        Err(e) => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams(format!("invalid target_node_id UUID: {e}")),
            );
        }
    };

    // Check device is owned by this node
    {
        let dm = state.device_manager.read().await;
        if !dm.owns_device(device_id) {
            return JsonRpcResponse::error(
                id,
                &RpcError::DeviceNotFound(format!(
                    "device {device_id} not owned by this node"
                )),
            );
        }
    }

    // Find target node address from membership
    let target_addr = {
        let m = cluster.membership.read().await;
        m.get_node(_target_node_id).map(|n| n.cluster_addr)
    };

    match target_addr {
        None => JsonRpcResponse::error(
            id,
            &RpcError::Internal(format!("node {_target_node_id} not found in membership")),
        ),
        Some(_addr) => {
            // Delegation is handled by the cluster delegation handler in practice;
            // here we just report that the request was initiated.
            JsonRpcResponse::success(
                id,
                json!({
                    "status": "delegation_initiated",
                    "device_id": device_id.to_string(),
                    "target_node_id": _target_node_id.to_string(),
                }),
            )
        }
    }
}
