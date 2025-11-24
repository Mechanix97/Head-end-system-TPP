use crate::MetricsError;
use axum::{Json, Router, routing::get};
use serde::Serialize;
use tracing::{error, info};

use crate::metrics_connections::METRICS_CONNECTIONS;

/// JSON response structure for raw metrics endpoint
#[derive(Serialize)]
pub(crate) struct RawMetricsResponse {
    /// Individual ACK duration measurements in seconds
    measurements: Vec<f64>,
    /// Number of measurements in the buffer
    count: usize,
}

/// Starts the Prometheus metrics HTTP server.
///
/// This launches an Axum web server on the specified address and port that exposes
/// three endpoints:
/// - `GET /metrics` - Returns Prometheus metrics in OpenMetrics format
/// - `GET /metrics/raw` - Returns raw measurement data as JSON for Grafana histogram
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
            .route("/metrics/raw", get(get_raw_metrics))
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

/// HTTP handler for the `/metrics/raw` endpoint.
///
/// Returns raw ACK duration measurements as JSON for Grafana histogram visualization.
/// Returns the last N measurements (up to 1000) stored in the ring buffer.
///
/// Response format:
/// ```json
/// {
///   "measurements": [0.123, 0.456, 0.789, ...],
///   "count": 1000
/// }
/// ```
pub(crate) async fn get_raw_metrics() -> Json<RawMetricsResponse> {
    let measurements = METRICS_CONNECTIONS.get_raw_ack_durations();
    let count = measurements.len();

    Json(RawMetricsResponse {
        measurements,
        count,
    })
}
