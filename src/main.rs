use clap::Parser;
use std::{error::Error, sync::Arc};
use tokio::{
    io::{self, AsyncReadExt},
    sync::RwLock,
};
use tracing::info;
use uuid::Uuid;

use backdoor::backdoor::init_backdoor;
use cluster::{ClusterConfig, ClusterManager};
use common::database::{DatabaseType, api::Database, postgres::PostgresConnectionArgs};
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
    postgres_user: String,

    /// Postgres password
    #[arg(
        long = "postgres-password",
        default_value = "HeadEndSystem",
        help = "Postgres password"
    )]
    postgres_password: String,

    /// Postgres url
    #[arg(
        long = "postgres-url",
        default_value = "127.0.0.1",
        help = "Postgres url"
    )]
    postgres_url: String,

    /// Postgres port
    #[arg(long = "postgres-port", default_value = "5432", help = "Postgres port")]
    postgres_port: String,

    /// Disable cluster mode (runs in single-node mode)
    #[arg(
        long = "disable-cluster",
        default_value = "false",
        help = "Disable cluster mode"
    )]
    disable_cluster: bool,

    /// Cluster node name
    #[arg(long = "node-name", help = "Cluster node name (defaults to hostname)")]
    node_name: Option<String>,

    /// Cluster communication port
    #[arg(
        long = "cluster-port",
        default_value = "6570",
        help = "Cluster communication port"
    )]
    cluster_port: u16,

    /// Cluster bind IP
    #[arg(
        long = "cluster-ip",
        default_value = "0.0.0.0",
        help = "Cluster bind IP"
    )]
    cluster_ip: String,

    /// Cluster seed nodes
    #[arg(
        long = "cluster-seeds",
        help = "Seed nodes for cluster join (comma-separated, e.g., '127.0.0.1:6570,127.0.0.1:6571')"
    )]
    cluster_seeds: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    // Validate: cluster mode requires a shared database
    if !args.disable_cluster && args.database_type == DatabaseType::InMemory {
        return Err("Cluster mode requires a shared database. Use --database postgres or run with --disable-cluster for single-node mode.".into());
    }

    tracing_subscriber::fmt().init();

    // TODO: Persist configuration to file (e.g., ~/.hes/config.toml or /etc/hes/config.toml)
    // - node_id should be persistent across restarts (currently regenerates on each startup)
    // - Store CLI args as defaults (backdoor_port, cluster_port, metrics_port, etc.)
    // - Load config from file first, then override with CLI args if provided
    // - In cluster mode: changing node_id would orphan all owned devices in database

    // Generate node ID (always, even for single-node)
    let node_id = Uuid::new_v4();
    info!("Head-End System starting with node_id: {}", node_id);

    let db_params = if args.database_type == DatabaseType::Postgres {
        Some(PostgresConnectionArgs {
            user: args.postgres_user.clone(),
            password: args.postgres_password.clone(),
            url: args.postgres_url.clone(),
            port: args.postgres_port.clone(),
        })
    } else {
        None
    };

    let db = Database::new(args.database_type, db_params).await?;

    let scheduler = Arc::new(RwLock::new(
        Scheduler::new(args.buckets_number, db.clone(), node_id).await?,
    ));

    // Initialize cluster unless disabled
    let cluster_manager = if !args.disable_cluster {
        // Create cluster configuration from CLI args
        let config = ClusterConfig::from_cli_args(
            node_id,
            args.node_name.clone(),
            args.cluster_ip.clone(),
            args.cluster_port,
            args.backdoor_port.parse().unwrap_or(6565),
            args.buckets_number as i32,
            args.cluster_seeds.clone(),
        )?;

        // Initialize cluster manager
        let mut manager = ClusterManager::new(config, db.clone(), scheduler.clone()).await?;

        // Start cluster manager
        manager.start().await?;

        // Sync scheduler with cluster-owned devices
        let owned_devices = manager.get_owned_devices().await;
        scheduler.write().await.enable_cluster_mode(owned_devices);

        info!("Cluster mode enabled");
        Some(manager)
    } else {
        info!("Running in single-node mode");
        None
    };

    let backdoor_joinhandle = init_backdoor(
        scheduler.clone(),
        args.backdoor_ip,
        args.backdoor_port,
        None,
        db.clone(),
        node_id,
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

    // Graceful shutdown
    if let Some(cluster_manager) = cluster_manager {
        info!("Initiating cluster graceful shutdown...");
        if let Err(e) = cluster_manager.shutdown().await {
            tracing::warn!("Error during cluster shutdown: {}", e);
        }
    }

    backdoor_joinhandle.abort_handle().abort();
    if let Some(metrics_join_handle) = metrics_join_handle {
        metrics_join_handle.abort_handle().abort();
    }

    Ok(())
}
