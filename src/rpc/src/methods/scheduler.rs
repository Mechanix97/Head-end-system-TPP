use serde_json::{Value, json};
use uuid::Uuid;

use common::scheduled_connection::ScheduledStatus;
use crate::error::RpcError;
use crate::protocol::JsonRpcResponse;
use crate::state::RpcState;

/// Returns upcoming scheduled connections (status = Awaiting), sorted by schedule_time ASC.
///
/// Params: limit (default 20, max 200), offset (default 0), device_id (optional UUID filter)
pub async fn upcoming(state: &RpcState, params: Value, id: Value) -> JsonRpcResponse {
    let limit = params
        .get("limit")
        .and_then(Value::as_i64)
        .unwrap_or(20)
        .clamp(1, 200);
    let offset = params
        .get("offset")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        .max(0);
    let device_id = params
        .get("device_id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok());

    match state.database.get_upcoming_connections(limit, offset, device_id).await {
        Ok(connections) => {
            let items: Vec<Value> = connections
                .iter()
                .map(|c| {
                    json!({
                        "device_id": c.fk_device.to_string(),
                        "schedule_time": c.schedule_time.to_string(),
                    })
                })
                .collect();
            let count = items.len();
            JsonRpcResponse::success(id, json!({ "connections": items, "count": count }))
        }
        Err(e) => JsonRpcResponse::error(id, &RpcError::Internal(e.to_string())),
    }
}

/// Returns past connections (status = Done or Lost), sorted by schedule_time DESC.
///
/// Params: limit (default 20, max 200), offset (default 0), device_id (optional UUID filter)
pub async fn history(state: &RpcState, params: Value, id: Value) -> JsonRpcResponse {
    let limit = params
        .get("limit")
        .and_then(Value::as_i64)
        .unwrap_or(20)
        .clamp(1, 200);
    let offset = params
        .get("offset")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        .max(0);
    let device_id = params
        .get("device_id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok());

    match state
        .database
        .get_connection_history(limit, offset, device_id)
        .await
    {
        Ok(connections) => {
            let items: Vec<Value> = connections
                .iter()
                .map(|c| {
                    let status = match c.status {
                        ScheduledStatus::Done => "done",
                        ScheduledStatus::Lost => "lost",
                        ScheduledStatus::Awaiting => "awaiting",
                    };
                    json!({
                        "device_id": c.fk_device.to_string(),
                        "schedule_time": c.schedule_time.to_string(),
                        "connection_time": c.connection_time.map(|t| t.to_string()),
                        "status": status,
                    })
                })
                .collect();
            let count = items.len();
            JsonRpcResponse::success(id, json!({ "connections": items, "count": count }))
        }
        Err(e) => JsonRpcResponse::error(id, &RpcError::Internal(e.to_string())),
    }
}
