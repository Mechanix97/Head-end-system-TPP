use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::handler::dispatch;
use crate::protocol::JsonRpcRequest;
use crate::state::RpcState;

/// Starts the JSON-RPC TCP server.
///
/// Listens on `rpc_ip:rpc_port`. Each connection is handled in its own tokio task.
/// Messages are newline-delimited JSON-RPC 2.0.
///
/// Returns a `JoinHandle` that can be aborted for graceful shutdown.
pub async fn start_rpc_server(
    rpc_ip: String,
    rpc_port: u16,
    state: RpcState,
) -> Result<JoinHandle<()>, std::io::Error> {
    let bind_addr = format!("{rpc_ip}:{rpc_port}");
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("JSON-RPC server listening on {bind_addr}");

    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    let state = state.clone();
                    tokio::spawn(handle_connection(stream, peer_addr, state));
                }
                Err(e) => {
                    warn!("RPC accept error: {e}");
                }
            }
        }
    });

    Ok(handle)
}

/// Handles a single TCP client connection.
async fn handle_connection(stream: TcpStream, peer_addr: SocketAddr, state: RpcState) {
    debug!("RPC client connected: {peer_addr}");
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                debug!("RPC client disconnected: {peer_addr}");
                break;
            }
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let response = match serde_json::from_str::<JsonRpcRequest>(trimmed) {
                    Ok(req) => dispatch(req, &state).await,
                    Err(e) => {
                        use crate::error::RpcError;
                        use crate::protocol::JsonRpcResponse;
                        JsonRpcResponse::error(
                            serde_json::Value::Null,
                            &RpcError::Json(e),
                        )
                    }
                };

                let mut json_bytes = match serde_json::to_vec(&response) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!("Failed to serialize RPC response: {e}");
                        continue;
                    }
                };
                json_bytes.push(b'\n');

                if let Err(e) = write_half.write_all(&json_bytes).await {
                    debug!("RPC write error to {peer_addr}: {e}");
                    break;
                }
            }
            Err(e) => {
                debug!("RPC read error from {peer_addr}: {e}");
                break;
            }
        }
    }
}
