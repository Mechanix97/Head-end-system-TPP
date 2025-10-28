use clap::Parser;
use std::{error::Error, sync::Arc};
use tokio::{
    io::{self, AsyncReadExt},
    sync::Mutex,
};
use tracing::info;

use backdoor::backdoor::init_backdoor;
use common::database::{DatabaseType, api::Database};
use metrics::api::start_prometheus_metrics_api;
use scheduler::scheduler::Scheduler;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Backdoor IP
    #[arg(long = "backdoor-ip", default_value = "0.0.0.0", help = "Backdoor IP")]
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

    /// Database type
    #[arg(
        long = "database",
        default_value = "postgres",
        help = "Database type to use (in-memory or postgres)"
    )]
    database_type: DatabaseType,

    /// Postgres user
    #[arg(
        long = "postgres-user",
        default_value = "postgres",
        help = "Postgres user"
    )]
    postgres_user: Option<String>,

    /// Postgres password
    #[arg(
        long = "postgres-password",
        default_value = "HeadEndSystem",
        help = "Postgres password"
    )]
    postgres_password: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    tracing_subscriber::fmt().init();

    info!("Head-End System starting");

    let db = Database::new(
        args.database_type,
        args.postgres_user,
        args.postgres_password,
    )
    .await?;

    let scheduler = Arc::new(Mutex::new(
        Scheduler::new(args.buckets_number, db.clone()).await?,
    ));
    scheduler.lock().await.start().await?;

    let backdoor_joinhandle = init_backdoor(
        scheduler.clone(),
        args.backdoor_ip,
        args.backdoor_port,
        None,
    )
    .await?;

    let metrics_join_handle = if !args.no_metrics {
        let mjh = start_prometheus_metrics_api(args.metrics_ip, args.metrics_port).await?;
        Some(mjh)
    } else {
        None
    };

    loop {
        let mut buffer: [u8; 1] = [0; 1];
        let mut reader = io::BufReader::new(io::stdin());
        if let Ok(1) = reader.read(&mut buffer).await {
            let c = buffer[0] as char;
            if c == 'q' || c == 'Q' {
                info!("Shutting down.");
                break;
            }
        }
    }

    backdoor_joinhandle.abort_handle().abort();
    if let Some(metrics_join_handle) = metrics_join_handle {
        metrics_join_handle.abort_handle().abort();
    }

    Ok(())
}
