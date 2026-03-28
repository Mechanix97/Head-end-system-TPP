mod config;

use clap::Parser;
use std::{error::Error, sync::{Arc, atomic::AtomicBool}};
use tokio::{
    io::{self, AsyncReadExt},
    sync::{RwLock, watch},
};
use tracing::info;

use backdoor::backdoor::init_backdoor;
use cluster::{ClusterConfig, ClusterManager};
use common::config_store::ConfigStore;
use common::database::{DatabaseConfig, DatabaseType, api::Database};
use config::{CliOverrides, resolve_config};
use device_manager::DeviceManager;
use metrics::api::start_prometheus_metrics_api;
use rpc::{ClusterManagerHandle, RpcState, start_rpc_server};
use scheduler::scheduler::Scheduler;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to YAML config file
    #[arg(
        long = "config",
        help = "Path to YAML config file (default: ~/.hes/config.yaml)"
    )]
    config: Option<String>,

    /// Backdoor IP
    #[arg(long = "backdoor-ip", help = "Backdoor IP")]
    backdoor_ip: Option<String>,

    /// Backdoor port
    #[arg(long = "backdoor-port", help = "Backdoor port")]
    backdoor_port: Option<String>,

    /// Prometheus metrics api IP
    #[arg(long = "metrics-ip", help = "Prometheus metrics api IP")]
    metrics_ip: Option<String>,

    /// Prometheus metrics api port
    #[arg(long = "metrics-port", help = "Prometheus metrics api port")]
    metrics_port: Option<String>,

    /// Disable metrics
    #[arg(long = "disble-metrics", help = "Disable Prometheus metrics API")]
    no_metrics: bool,

    /// Number of time buckets
    #[arg(long = "buckets-number", help = "Number of scheduler time buckets")]
    buckets_number: Option<usize>,

    /// Database type
    #[arg(
        long = "database",
        help = "Database type to use (in-memory or postgres)"
    )]
    database_type: Option<DatabaseType>,

    /// Postgres user
    #[arg(long = "postgres-user", help = "Postgres user")]
    postgres_user: Option<String>,

    /// Postgres password
    #[arg(long = "postgres-password", help = "Postgres password")]
    postgres_password: Option<String>,

    /// Postgres url
    #[arg(long = "postgres-url", help = "Postgres url")]
    postgres_url: Option<String>,

    /// Postgres port
    #[arg(long = "postgres-port", help = "Postgres port")]
    postgres_port: Option<String>,

    /// Disable cluster mode (runs in single-node mode)
    #[arg(long = "disable-cluster", help = "Disable cluster mode")]
    disable_cluster: bool,

    /// Cluster node name
    #[arg(long = "node-name", help = "Cluster node name (defaults to hostname)")]
    node_name: Option<String>,

    /// Cluster communication port
    #[arg(long = "cluster-port", help = "Cluster communication port")]
    cluster_port: Option<u16>,

    /// Cluster bind IP
    #[arg(long = "cluster-ip", help = "Cluster bind IP")]
    cluster_ip: Option<String>,

    /// Cluster seed nodes
    #[arg(
        long = "cluster-seeds",
        help = "Seed nodes for cluster join (comma-separated, e.g., '127.0.0.1:6570,127.0.0.1:6571')"
    )]
    cluster_seeds: Option<String>,

    /// Disable the JSON-RPC admin console
    #[arg(long = "disable-rpc", help = "Disable the JSON-RPC admin console")]
    disable_rpc: bool,

    /// JSON-RPC server bind IP
    #[arg(long = "rpc-ip", help = "JSON-RPC server bind IP (default: 127.0.0.1)")]
    rpc_ip: Option<String>,

    /// JSON-RPC server port
    #[arg(long = "rpc-port", help = "JSON-RPC server port (default: 6600)")]
    rpc_port: Option<u16>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    tracing_subscriber::fmt().init();

    // Resolve config: defaults → config file → CLI args
    let config_manager = resolve_config(
        args.config,
        CliOverrides {
            backdoor_ip: args.backdoor_ip,
            backdoor_port: args.backdoor_port,
            no_metrics: args.no_metrics,
            metrics_ip: args.metrics_ip,
            metrics_port: args.metrics_port,
            buckets_number: args.buckets_number,
            database_type: args.database_type,
            postgres_user: args.postgres_user,
            postgres_password: args.postgres_password,
            postgres_url: args.postgres_url,
            postgres_port: args.postgres_port,
            disable_cluster: args.disable_cluster,
            node_name: args.node_name,
            cluster_ip: args.cluster_ip,
            cluster_port: args.cluster_port,
            cluster_seeds: args.cluster_seeds,
            disable_rpc: args.disable_rpc,
            rpc_ip: args.rpc_ip,
            rpc_port: args.rpc_port,
        },
    )?;
    let config = config_manager.get().await;

    // Validate: cluster mode requires a shared database
    if config.cluster_enabled && config.database_type == DatabaseType::InMemory {
        return Err("Cluster mode requires a shared database. Use --database postgres or run with --disable-cluster for single-node mode.".into());
    }

    info!("Head-End System starting with node_id: {}", config.node_id);

    let db = Database::new(DatabaseConfig {
        db_type: config.database_type,
        user: config.postgres_user.clone(),
        password: config.postgres_password.clone(),
        url: config.postgres_url.clone(),
        port: config.postgres_port.clone(),
    })
    .await?;

    let scheduler = Scheduler::new(config.buckets_number, db.clone(), config.node_id).await?;

    let device_manager = Arc::new(RwLock::new(DeviceManager::new(
        config.node_id,
        config.buckets_number as i32,
        db.clone(),
        scheduler,
    )));

    // Initialize cluster unless disabled
    let cluster_manager = if config.cluster_enabled {
        let cluster_config = ClusterConfig::from_cli_args(
            config.node_id,
            config.node_name.clone(),
            config.cluster_ip.clone(),
            config.cluster_port,
            config.backdoor_port.parse().unwrap_or(6565),
            config.buckets_number as i32,
            config.cluster_seeds.clone(),
        )?;

        let config_store: Arc<dyn ConfigStore> = Arc::new(config_manager.clone());
        let mut manager =
            ClusterManager::new(cluster_config, db.clone(), device_manager.clone(), config_store)
                .await?;

        manager.start().await?;

        let owned_devices = manager.get_owned_devices().await;
        device_manager
            .write()
            .await
            .enable_cluster_mode(owned_devices);

        info!("Cluster mode enabled");
        Some(manager)
    } else {
        info!("Running in single-node mode");
        None
    };

    // Hot-reload primitives shared between the RPC layer and the running services.
    let (backdoor_rebind_tx, backdoor_rebind_rx) =
        watch::channel((config.backdoor_ip.clone(), config.backdoor_port.clone()));
    let backdoor_rebind_tx = Arc::new(backdoor_rebind_tx);

    let metrics_enabled_flag = Arc::new(AtomicBool::new(config.metrics_enabled));    
  
    // Reload scheduled connections now that cluster ownership is known.
    // In cluster mode this runs after enable_cluster_mode(), so only this
    // node's own devices are picked up. In single-node mode it loads all.
    device_manager
        .write()
        .await
        .reload_active_connections()
        .await?;

   let backdoor_joinhandle = init_backdoor(backdoor::backdoor::BackdoorConfig {
        ip: config.backdoor_ip,
        port: config.backdoor_port,
        ack_timeout_duration: None,
        database: db.clone(),
        node_id: config.node_id,
        device_manager: device_manager.clone(),
        max_concurrent_handlers: None,
        rebind_rx: backdoor_rebind_rx,
    }).await?;

    let metrics_join_handle = if config.metrics_enabled {
        let mjh = start_prometheus_metrics_api(
            config.metrics_ip,
            config.metrics_port,
            Arc::clone(&metrics_enabled_flag),
        )
        .await?;
        Some(mjh)
    } else {
        None
    };

    // Start JSON-RPC server if enabled
    let rpc_join_handle = if config.rpc_enabled {
        let rpc_state = RpcState {
            node_id: config.node_id,
            config_store: Arc::new(config_manager.clone()),
            device_manager: device_manager.clone(),
            cluster_handle: cluster_manager.as_ref().map(ClusterManagerHandle::from),
            database: db.clone(),
            metrics_enabled: Arc::clone(&metrics_enabled_flag),
            backdoor_rebind: Arc::clone(&backdoor_rebind_tx),
            started_at: std::time::Instant::now(),
        };

        let rpc_addr = format!("{}:{}", config.rpc_ip, config.rpc_port);
        let handle = start_rpc_server(config.rpc_ip, config.rpc_port, rpc_state).await?;
        info!("JSON-RPC admin console started on {rpc_addr}");
        Some(handle)
    } else {
        info!("JSON-RPC admin console disabled");
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
    if let Some(rpc_join_handle) = rpc_join_handle {
        rpc_join_handle.abort_handle().abort();
    }

    Ok(())
}
