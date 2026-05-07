use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::{Router, extract::State, http::StatusCode, routing::get};
use tracing::{error, info};

use crate::MetricsError;
use crate::metrics_cluster::METRICS_CLUSTER;
use crate::metrics_connections::METRICS_CONNECTIONS;

/// Starts the Prometheus metrics HTTP server.
///
/// `enabled` is an `Arc<AtomicBool>` shared with the RPC layer so that
/// `config set metrics_enabled false` takes effect without a restart.
pub async fn start_prometheus_metrics_api(
    address: String,
    port: String,
    enabled: Arc<AtomicBool>,
) -> Result<tokio::task::JoinHandle<()>, MetricsError> {
    info!("Starting prometheus api at {}:{}", address, port);
    let join_handle: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(get_metrics))
            .route("/health", get(|| async { "Service Up" }))
            .with_state(enabled);

        let listener = match tokio::net::TcpListener::bind(&format!("{address}:{port}")).await {
            Ok(l) => l,
            Err(e) => {
                error!("Unable to bind metrics port {}:{}: {}", address, port, e);
                return;
            }
        };
        if let Err(e) = axum::serve(listener, app).await {
            error!("Metrics server error: {}", e);
        }
    });
    Ok(join_handle)
}

/// HTTP handler for the `/metrics` endpoint.
///
/// Returns 503 with an empty body when metrics are disabled at runtime.
pub(crate) async fn get_metrics(
    State(enabled): State<Arc<AtomicBool>>,
) -> (StatusCode, String) {
    if !enabled.load(Ordering::Relaxed) {
        return (StatusCode::SERVICE_UNAVAILABLE, String::new());
    }
    let mut output = String::new();

    match METRICS_CONNECTIONS.gather_metrics() {
        Ok(s) => output.push_str(&s),
        Err(_) => error!("Failed to gather METRICS_CONNECTIONS"),
    }

    match METRICS_CLUSTER.gather_metrics() {
        Ok(s) => output.push_str(&s),
        Err(_) => error!("Failed to gather METRICS_CLUSTER"),
    }

    (StatusCode::OK, output)
}
