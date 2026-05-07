use metrics::metrics_connections::METRICS_CONNECTIONS;

use crate::methods;
use crate::protocol::{JsonRpcRequest, JsonRpcResponse};
use crate::state::RpcState;

/// Dispatches a JSON-RPC request to the appropriate handler.
pub async fn dispatch(req: JsonRpcRequest, state: &RpcState) -> JsonRpcResponse {
    let start = std::time::Instant::now();
    let method = req.method.clone();

    let response = dispatch_inner(req, state).await;

    let elapsed_ms = start.elapsed().as_millis() as f64;
    let result_label = if response.error.is_some() { "error" } else { "success" };
    METRICS_CONNECTIONS
        .hes_rpc_request_total
        .with_label_values(&[method.as_str(), result_label])
        .inc();
    METRICS_CONNECTIONS
        .hes_rpc_request_duration_ms
        .with_label_values(&[method.as_str()])
        .observe(elapsed_ms);

    response
}

async fn dispatch_inner(req: JsonRpcRequest, state: &RpcState) -> JsonRpcResponse {
    match req.method.as_str() {
        // system.*
        "system.version" => methods::system::version(state, req.params, req.id).await,

        // config.*
        "config.list" => {
            methods::config::list(state.config_store.as_ref(), req.params, req.id).await
        }
        "config.get" => {
            methods::config::get(state.config_store.as_ref(), req.params, req.id).await
        }
        "config.set" => methods::config::set(state, req.params, req.id).await,
        "config.propagate" => config_propagate(state, req.params, req.id).await,

        // device.*
        "device.list" => methods::device::list(state, req.params, req.id).await,
        "device.info" => methods::device::info(state, req.params, req.id).await,
        "device.delegate" => methods::device::delegate(state, req.params, req.id).await,

        // cluster.*
        "cluster.peers" => methods::cluster::peers(state, req.params, req.id).await,
        "cluster.status" => methods::cluster::status(state, req.params, req.id).await,
        "cluster.join" => methods::cluster::join(state, req.params, req.id).await,

        // scheduler.*
        "scheduler.upcoming" => methods::scheduler::upcoming(state, req.params, req.id).await,
        "scheduler.history" => methods::scheduler::history(state, req.params, req.id).await,

        other => JsonRpcResponse::method_not_found(req.id, other),
    }
}

/// Handles config.propagate: set locally + broadcast ConfigUpdate to cluster.
async fn config_propagate(
    state: &RpcState,
    params: serde_json::Value,
    id: serde_json::Value,
) -> JsonRpcResponse {
    use crate::error::RpcError;
    use serde_json::json;

    let key = match params.get("key").and_then(serde_json::Value::as_str) {
        Some(k) => k.to_string(),
        None => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams("missing 'key'".to_string()),
            );
        }
    };
    let value = match params.get("value").and_then(serde_json::Value::as_str) {
        Some(v) => v.to_string(),
        None => {
            return JsonRpcResponse::error(
                id,
                &RpcError::InvalidParams("missing 'value'".to_string()),
            );
        }
    };

    // Apply locally first
    if let Err(e) = state.config_store.set_config_value(&key, &value).await {
        return JsonRpcResponse::error(id, &RpcError::Config(e.to_string()));
    }
    crate::methods::config::apply_hot_reload(&key, &value, state).await;

    // Propagate to cluster peers if cluster is enabled
    let propagated_to = if let Some(cluster) = &state.cluster_handle {
        match cluster.broadcast_config_update(&key, &value).await {
            Ok(count) => count,
            Err(e) => {
                tracing::warn!("Config propagation to cluster failed: {}", e);
                0
            }
        }
    } else {
        0
    };

    JsonRpcResponse::success(
        id,
        json!({
            "key": key,
            "value": value,
            "status": "ok",
            "propagated_to": propagated_to,
        }),
    )
}
