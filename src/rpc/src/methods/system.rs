use serde_json::{Value, json};

use crate::protocol::JsonRpcResponse;
use crate::state::RpcState;

pub async fn version(state: &RpcState, _params: Value, id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(
        id,
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            "node_id": state.node_id.to_string(),
        }),
    )
}
