use serde_json::{Value, json};

use common::config_store::ConfigStore;

use crate::error::RpcError;
use crate::protocol::JsonRpcResponse;

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

/// Sets a config key to a new value and persists to disk.
pub async fn set(config_store: &dyn ConfigStore, params: Value, id: Value) -> JsonRpcResponse {
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

    match config_store.set_config_value(&key, &value).await {
        Ok(()) => JsonRpcResponse::success(id, json!({ "key": key, "value": value, "status": "ok" })),
        Err(e) => JsonRpcResponse::error(id, &RpcError::Config(e.to_string())),
    }
}
