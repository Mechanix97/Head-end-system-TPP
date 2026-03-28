use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::handler::dispatch;
use crate::protocol::JsonRpcRequest;
use crate::state::RpcState;

/// Maximum length of a single JSON-RPC line (64 KB). Lines exceeding this
/// cause the client to be disconnected, preventing memory-exhaustion attacks.
const MAX_LINE_LENGTH: usize = 64 * 1024;

/// Maximum number of concurrent RPC client connections.
const MAX_RPC_CONNECTIONS: usize = 16;

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

    let semaphore = Arc::new(Semaphore::new(MAX_RPC_CONNECTIONS));

    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    let permit = match semaphore.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => {
                            warn!("RPC semaphore closed, stopping server");
                            break;
                        }
                    };
                    let state = state.clone();
                    tokio::spawn(handle_connection(stream, peer_addr, state, permit));
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
///
/// The `_permit` is held for the lifetime of the connection, limiting concurrency
/// via the semaphore in `start_rpc_server`. It is released automatically on return.
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    state: RpcState,
    _permit: OwnedSemaphorePermit,
) {
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
            Ok(n) if n > MAX_LINE_LENGTH => {
                warn!("RPC client {peer_addr} sent oversized message ({n} bytes), disconnecting");
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
