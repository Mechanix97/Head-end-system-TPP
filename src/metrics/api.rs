use crate::MetricsError;
use axum::{Router, routing::get};
use tracing::{error, info};

use crate::metrics_connections::METRICS_CONNECTIONS;

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

pub(crate) async fn get_metrics() -> String {
    match METRICS_CONNECTIONS.gather_metrics() {
        Ok(string) => string,
        Err(_) => {
            error!("Failed to register METRICS_CONNECTIONS");
            String::new()
        }
    }
}
