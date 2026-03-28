use serde_json::{Value, json};

use crate::protocol::JsonRpcResponse;
use crate::state::RpcState;

pub async fn version(state: &RpcState, _params: Value, id: Value) -> JsonRpcResponse {
    let node_name = state
        .config_store
        .get_config_value("node_name")
        .await
        .ok()
        .flatten()
        .unwrap_or_default();

    let database_type = state
        .config_store
        .get_config_value("database_type")
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| "unknown".to_string());

    let uptime = state.started_at.elapsed();
    let uptime_secs = uptime.as_secs();
    let uptime_human = format!(
        "{}d {}h {}m {}s",
        uptime_secs / 86400,
        (uptime_secs % 86400) / 3600,
        (uptime_secs % 3600) / 60,
        uptime_secs % 60,
    );

    let cluster_info = state.cluster_handle.as_ref().map(|h| {
        json!({
            "enabled": true,
            "addr": h.local_addr.to_string(),
        })
    });

    JsonRpcResponse::success(
        id,
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            "node_id": state.node_id.to_string(),
            "node_name": node_name,
            "database": database_type,
            "uptime": uptime_human,
            "uptime_secs": uptime_secs,
            "cluster": cluster_info.unwrap_or(json!({ "enabled": false })),
        }),
    )
}
