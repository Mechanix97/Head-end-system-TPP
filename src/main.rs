use std::{error::Error, sync::Arc};
use tokio::sync::Mutex;
use tracing::info;

use backdoor::backdoor::init_backdoor;
use metrics::api::start_prometheus_metrics_api;
use scheduler::scheduler::Scheduler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt().init();

    info!("Head-End System starting");

    let scheduler = Arc::new(Mutex::new(Scheduler::new().await?));
    scheduler.lock().await.start().await?;

    init_backdoor(scheduler.clone()).await?;

    start_prometheus_metrics_api("0.0.0.0".to_string(), "8000".to_string()).await?;
    Ok(())
}
