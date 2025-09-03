use crate::MetricsError;
use axum::{Router, routing::get};
use tracing::error;

use crate::metrics_connections::METRICS_CONNECTIONS;

pub async fn start_prometheus_metrics_api(
    address: String,
    port: String,
) -> Result<(), MetricsError> {
    let app = Router::new()
        .route("/metrics", get(get_metrics))
        .route("/health", get(|| async { "Service Up" }));

    let listener = tokio::net::TcpListener::bind(&format!("{address}:{port}")).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

pub(crate) async fn get_metrics() -> String {
    let ret_string = match METRICS_CONNECTIONS.gather_metrics() {
        Ok(string) => string,
        Err(_) => {
            error!("Failed to register METRICS_CONNECTIONS");
            String::new()
        }
    };

    ret_string
}
