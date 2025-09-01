use std::error::Error;
use tokio::time::{Duration, sleep};
use tracing::info;

use backdoor::backdoor::init_backdoor;
use common::connection::Conection;
use metrics::api::start_prometheus_metrics_api;
use scheduler::scheduler::Scheduler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt().init();

    info!("Head-End System starting");

    let mut scheduler = Scheduler::new().await?;
    scheduler.start().await?;

    scheduler
        .add_connection(Conection {
            id: 0,

            ip: "192.168.0.1".into(),
        })
        .await?;

    init_backdoor();

    start_prometheus_metrics_api("127.0.0.1".to_string(), "8000".to_string()).await?;
    loop {
        sleep(Duration::from_millis(100)).await;
    }
}
