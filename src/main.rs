use std::{error::Error, sync::Arc};
use tokio::sync::Mutex;
use tracing::info;

use backdoor::backdoor::init_backdoor;
use metrics::api::start_prometheus_metrics_api;
use scheduler::scheduler::Scheduler;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Backdoor IP
    #[arg(
        long = "backdoor-ip",
        default_value = "0.0.0.0",
        help = "Prometheus metrics api IP"
    )]
    backdoor_ip: String,

    /// Backdoor port
    #[arg(long = "backdoor-port", default_value = "6565", help = "Backdoor port")]
    backdoor_port: String,

    /// Prometheus metrics api IP
    #[arg(
        long = "metrics-ip",
        default_value = "0.0.0.0",
        help = "Prometheus metrics api IP"
    )]
    metrics_ip: String,

    /// Prometheus metrics api port
    #[arg(
        long = "metrics-port",
        default_value = "6464",
        help = "Prometheus metrics api port"
    )]
    metrics_port: String,

    /// no metrics bool
    #[arg(
        long = "no-metrics",
        default_value = "false",
        help = "No metrics indicator"
    )]
    no_metrics: bool,

    /// number of buckets
    #[arg(
        long = "buckets-number",
        default_value = "48",
        help = "No metrics indicator"
    )]
    buckets_number: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    tracing_subscriber::fmt().init();

    info!("Head-End System starting");

    let scheduler = Arc::new(Mutex::new(Scheduler::new(args.buckets_number).await?));
    scheduler.lock().await.start().await?;

    let _bdjh = init_backdoor(scheduler.clone(), args.backdoor_ip, args.backdoor_port).await?;

    if !args.no_metrics {
        start_prometheus_metrics_api(args.metrics_ip, args.metrics_port).await?;
    } else {
        #[allow(clippy::empty_loop)]
        loop {}
    }
    Ok(())
}
