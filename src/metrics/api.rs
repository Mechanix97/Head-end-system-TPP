use crate::MetricsError;
use axum::{Router, routing::get};
use tracing::{error, info};

use crate::metrics_connections::METRICS_CONNECTIONS;

/// Starts the Prometheus metrics HTTP server.
///
/// This launches an Axum web server on the specified address and port that exposes
/// two endpoints:
/// - `GET /metrics` - Returns Prometheus metrics in OpenMetrics format
/// - `GET /health` - Simple health check endpoint that returns "Service Up"
///
/// The server runs in a background tokio task and Prometheus can scrape the
/// `/metrics` endpoint at regular intervals (configured in prometheus.yml).
///
/// # Example
/// ```ignore
/// let handle = start_prometheus_metrics_api("0.0.0.0".to_string(), "6464".to_string()).await?;
/// // Metrics available at http://0.0.0.0:6464/metrics
/// ```
///
/// Returns a JoinHandle for the spawned server task.
pub async fn start_prometheus_metrics_api(
    address: String,
    port: String,
) -> Result<tokio::task::JoinHandle<()>, MetricsError> {
    info!("Starting prometheus api at {}:{}", address, port);
    let join_handle: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(get_metrics))
            .route("/health", get(|| async { "Service Up" }));

        let listener = tokio::net::TcpListener::bind(&format!("{address}:{port}"))
            .await
            .expect("Unable to bind port");
        axum::serve(listener, app)
            .await
            .expect("Unable to serve app");
    });
    Ok(join_handle)
}

/// HTTP handler for the `/metrics` endpoint.
///
/// Gathers all registered metrics and returns them in Prometheus text format.
/// If gathering fails, returns an empty string and logs an error.
pub(crate) async fn get_metrics() -> String {
    match METRICS_CONNECTIONS.gather_metrics() {
        Ok(string) => string,
        Err(_) => {
            error!("Failed to register METRICS_CONNECTIONS");
            String::new()
        }
    }
}
