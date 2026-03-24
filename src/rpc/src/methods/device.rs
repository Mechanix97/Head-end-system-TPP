use serde_json::{Value, json};
use uuid::Uuid;

use cluster::delegation::DelegationHandler;
use cluster::protocol::DelegationReason;
use common::delegated_device::DelegatedDevice;
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

    let target_node_id = match Uuid::parse_str(&target_node_str) {
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

    // Verify target node exists in membership
    let target_exists = {
        let m = cluster.membership.read().await;
        m.get_node(target_node_id).is_some()
    };

    if !target_exists {
        return JsonRpcResponse::error(
            id,
            &RpcError::Internal(format!("node {target_node_id} not found in membership")),
        );
    }

    // Fetch device info from DB to build DelegatedDevice
    let device = match state.database.get_device(device_id).await {
        Ok(d) => d,
        Err(e) => return JsonRpcResponse::error(id, &RpcError::Database(e.to_string())),
    };

    // Get scheduled connection time; fall back to tomorrow if not found
    let schedule_time = match state.database.get_scheduled_connection(device_id).await {
        Ok(conn) => conn.schedule_time,
        Err(_) => chrono::Utc::now().naive_utc() + chrono::Duration::days(1),
    };

    let delegated = DelegatedDevice::new(device.id, device.ipv4, device.ipv6, schedule_time);

    let delegation_handler = DelegationHandler::new(
        cluster.node_id,
        state.device_manager.clone(),
        cluster.membership.clone(),
        cluster.socket.clone(),
        state.database.clone(),
    );

    match delegation_handler
        .request_delegation(vec![delegated], DelegationReason::Rebalance, Some(target_node_id))
        .await
    {
        Ok(_) => JsonRpcResponse::success(
            id,
            json!({
                "status": "delegation_initiated",
                "device_id": device_id.to_string(),
                "target_node_id": target_node_id.to_string(),
            }),
        ),
        Err(e) => JsonRpcResponse::error(id, &RpcError::Internal(e.to_string())),
    }
}
