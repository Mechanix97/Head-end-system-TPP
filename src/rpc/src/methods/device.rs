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

/// Resolves the list of device IDs to delegate from the RPC params.
///
/// Accepts one of three forms:
/// - `{ "device_ids": ["uuid", ...] }` — explicit list
/// - `{ "device_id": "uuid" }`         — single device (convenience)
/// - `{ "count": N }`                  — N random owned devices
fn resolve_device_ids(params: &Value, state_dm_snapshot: &[Uuid]) -> Result<Vec<Uuid>, RpcError> {
    // Explicit list
    if let Some(arr) = params.get("device_ids").and_then(Value::as_array) {
        let ids = arr
            .iter()
            .map(|v| {
                v.as_str()
                    .ok_or_else(|| RpcError::InvalidParams("device_ids must be strings".to_string()))
                    .and_then(|s| {
                        Uuid::parse_str(s)
                            .map_err(|e| RpcError::InvalidParams(format!("invalid UUID '{s}': {e}")))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if ids.is_empty() {
            return Err(RpcError::InvalidParams("device_ids is empty".to_string()));
        }
        return Ok(ids);
    }

    // Single device
    if let Some(s) = params.get("device_id").and_then(Value::as_str) {
        let uuid = Uuid::parse_str(s)
            .map_err(|e| RpcError::InvalidParams(format!("invalid device_id UUID: {e}")))?;
        return Ok(vec![uuid]);
    }

    // Count: pick N from owned devices (HashSet order ≈ random)
    if let Some(n) = params.get("count").and_then(Value::as_u64) {
        if n == 0 {
            return Err(RpcError::InvalidParams("count must be > 0".to_string()));
        }
        let picked: Vec<Uuid> = state_dm_snapshot.iter().copied().take(n as usize).collect();
        if picked.is_empty() {
            return Err(RpcError::Internal("no owned devices to delegate".to_string()));
        }
        return Ok(picked);
    }

    Err(RpcError::InvalidParams(
        "provide 'device_id', 'device_ids', or 'count'".to_string(),
    ))
}

pub async fn delegate(state: &RpcState, params: Value, id: Value) -> JsonRpcResponse {
    let cluster = match &state.cluster_handle {
        Some(h) => h,
        None => return JsonRpcResponse::error(id, &RpcError::ClusterNotEnabled),
    };

    // Optional target node
    let target_node_id = match params.get("target_node_id").and_then(Value::as_str) {
        Some(s) => match Uuid::parse_str(s) {
            Ok(uuid) => {
                let m = cluster.membership.read().await;
                if m.get_node(uuid).is_none() {
                    return JsonRpcResponse::error(
                        id,
                        &RpcError::Internal(format!("node {uuid} not found in membership")),
                    );
                }
                Some(uuid)
            }
            Err(e) => {
                return JsonRpcResponse::error(
                    id,
                    &RpcError::InvalidParams(format!("invalid target_node_id UUID: {e}")),
                );
            }
        },
        None => None,
    };

    // Snapshot owned devices for count-based resolution
    let owned_snapshot: Vec<Uuid> = {
        let dm = state.device_manager.read().await;
        dm.owned_devices().iter().copied().collect()
    };

    let device_ids = match resolve_device_ids(&params, &owned_snapshot) {
        Ok(ids) => ids,
        Err(e) => return JsonRpcResponse::error(id, &e),
    };

    // Verify all requested devices are owned by this node
    {
        let dm = state.device_manager.read().await;
        for device_id in &device_ids {
            if !dm.owns_device(*device_id) {
                return JsonRpcResponse::error(
                    id,
                    &RpcError::DeviceNotFound(format!(
                        "device {device_id} not owned by this node"
                    )),
                );
            }
        }
    }

    // Build DelegatedDevice list from DB
    let mut delegated = Vec::with_capacity(device_ids.len());
    for device_id in &device_ids {
        let device = match state.database.get_device(*device_id).await {
            Ok(d) => d,
            Err(e) => return JsonRpcResponse::error(id, &RpcError::Database(e.to_string())),
        };
        let schedule_time = match state.database.get_scheduled_connection(*device_id).await {
            Ok(conn) => conn.schedule_time,
            Err(_) => chrono::Utc::now().naive_utc() + chrono::Duration::days(1),
        };
        delegated.push(DelegatedDevice::new(device.id, device.ipv4, device.ipv6, schedule_time));
    }

    let delegation_handler = DelegationHandler::new(
        cluster.node_id,
        state.device_manager.clone(),
        cluster.membership.clone(),
        cluster.socket.clone(),
        state.database.clone(),
    );

    match delegation_handler
        .request_delegation(delegated, DelegationReason::Rebalance, target_node_id)
        .await
    {
        Ok(delegated_ids) => JsonRpcResponse::success(
            id,
            json!({
                "status": "delegation_initiated",
                "delegated": delegated_ids.iter().map(|u| u.to_string()).collect::<Vec<_>>(),
                "count": delegated_ids.len(),
                "target_node_id": target_node_id.map(|n| n.to_string()),
            }),
        ),
        Err(e) => JsonRpcResponse::error(id, &RpcError::Internal(e.to_string())),
    }
}
