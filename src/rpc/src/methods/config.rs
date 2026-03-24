use std::sync::atomic::Ordering;

use serde_json::{Value, json};

use common::config_store::ConfigStore;

use crate::error::RpcError;
use crate::protocol::JsonRpcResponse;
use crate::state::RpcState;

/// Lists all configurable keys and their current values.
pub async fn list(config_store: &dyn ConfigStore, _params: Value, id: Value) -> JsonRpcResponse {
    match config_store.get_all_config().await {
        Ok(map) => JsonRpcResponse::success(id, json!(map)),
        Err(e) => JsonRpcResponse::error(id, &RpcError::Config(e.to_string())),
    }
}

/// Gets the value of a single config key.
pub async fn get(config_store: &dyn ConfigStore, params: Value, id: Value) -> JsonRpcResponse {
    let key = match params.get("key").and_then(Value::as_str) {
        Some(k) => k.to_string(),
        None => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams("missing 'key'".to_string()),
            );
        }
    };

    match config_store.get_config_value(&key).await {
        Ok(Some(val)) => JsonRpcResponse::success(id, json!({ "key": key, "value": val })),
        Ok(None) => JsonRpcResponse::error(
            id,
            &RpcError::InvalidParams(format!("unknown config key: {key}")),
        ),
        Err(e) => JsonRpcResponse::error(id, &RpcError::Config(e.to_string())),
    }
}

/// Sets a config key to a new value, persists to disk, and applies hot-reload side effects.
pub async fn set(state: &RpcState, params: Value, id: Value) -> JsonRpcResponse {
    let key = match params.get("key").and_then(Value::as_str) {
        Some(k) => k.to_string(),
        None => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams("missing 'key'".to_string()),
            );
        }
    };
    let value = match params.get("value").and_then(Value::as_str) {
        Some(v) => v.to_string(),
        None => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams("missing 'value' (must be a string)".to_string()),
            );
        }
    };

    match state.config_store.set_config_value(&key, &value).await {
        Ok(()) => {
            apply_hot_reload(&key, &value, state).await;
            JsonRpcResponse::success(id, json!({ "key": key, "value": value, "status": "ok" }))
        }
        Err(e) => JsonRpcResponse::error(id, &RpcError::Config(e.to_string())),
    }
}

/// Applies in-process side effects for keys that support hot-reload.
///
/// Called after a successful `set_config_value` from both `config.set` and `config.propagate`.
pub async fn apply_hot_reload(key: &str, value: &str, state: &RpcState) {
    match key {
        "metrics_enabled" => {
            let enabled = value == "true";
            state.metrics_enabled.store(enabled, Ordering::Relaxed);
            tracing::info!("Hot-reload: metrics_enabled = {enabled}");
        }
        "backdoor_ip" | "backdoor_port" => {
            // Read the full (ip, port) from config after the save so both are consistent.
            let ip = state
                .config_store
                .get_config_value("backdoor_ip")
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            let port = state
                .config_store
                .get_config_value("backdoor_port")
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            let _ = state.backdoor_rebind.send((ip.clone(), port.clone()));
            tracing::info!("Hot-reload: backdoor rebind requested → {ip}:{port}");
        }
        "node_name" => {
            if let Some(cluster) = &state.cluster_handle {
                if !value.is_empty() {
                    let mut m = cluster.membership.write().await;
                    m.local_node_mut().node_name = value.to_string();
                    tracing::info!("Hot-reload: node_name = {value}");
                }
            }
        }
        _ => {}
    }
}
