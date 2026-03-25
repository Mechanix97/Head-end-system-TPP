use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::info;
use tokio::sync::RwLock;
use uuid::Uuid;

use common::database::DatabaseType;

// ── YAML deserialization structs ─────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct FileConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backdoor: Option<BackdoorFileConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<MetricsFileConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduler: Option<SchedulerFileConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<DatabaseFileConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster: Option<ClusterFileConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc: Option<RpcFileConfig>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct RpcFileConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct BackdoorFileConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct MetricsFileConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct SchedulerFileConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buckets_number: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct DatabaseFileConfig {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub db_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres: Option<PostgresFileConfig>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct PostgresFileConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct ClusterFileConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seeds: Option<String>,
}

// ── Resolved config ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct HesConfig {
    pub node_id: Uuid,
    pub backdoor_ip: String,
    pub backdoor_port: String,
    pub metrics_enabled: bool,
    pub metrics_ip: String,
    pub metrics_port: String,
    pub buckets_number: usize,
    pub database_type: DatabaseType,
    pub postgres_user: String,
    pub postgres_password: String,
    pub postgres_url: String,
    pub postgres_port: String,
    pub cluster_enabled: bool,
    pub node_name: Option<String>,
    pub cluster_ip: String,
    pub cluster_port: u16,
    pub cluster_seeds: Option<String>,
    pub rpc_enabled: bool,
    pub rpc_ip: String,
    pub rpc_port: u16,
}

impl Default for HesConfig {
    fn default() -> Self {
        Self {
            node_id: Uuid::new_v4(),
            backdoor_ip: "0.0.0.0".to_string(),
            backdoor_port: "6565".to_string(),
            metrics_enabled: true,
            metrics_ip: "0.0.0.0".to_string(),
            metrics_port: "6464".to_string(),
            buckets_number: 48,
            database_type: DatabaseType::Postgres,
            postgres_user: "postgres".to_string(),
            postgres_password: "HeadEndSystem".to_string(),
            postgres_url: "127.0.0.1".to_string(),
            postgres_port: "5432".to_string(),
            cluster_enabled: true,
            node_name: None,
            cluster_ip: "0.0.0.0".to_string(),
            cluster_port: 6570,
            cluster_seeds: None,
            rpc_enabled: true,
            rpc_ip: "127.0.0.1".to_string(),
            rpc_port: 6600,
        }
    }
}

impl HesConfig {
    /// Converts the resolved config back to the YAML file format.
    pub fn to_file_config(&self) -> FileConfig {
        FileConfig {
            node_id: Some(self.node_id.to_string()),
            backdoor: Some(BackdoorFileConfig {
                ip: Some(self.backdoor_ip.clone()),
                port: Some(self.backdoor_port.clone()),
            }),
            metrics: Some(MetricsFileConfig {
                enabled: Some(self.metrics_enabled),
                ip: Some(self.metrics_ip.clone()),
                port: Some(self.metrics_port.clone()),
            }),
            scheduler: Some(SchedulerFileConfig {
                buckets_number: Some(self.buckets_number),
            }),
            database: Some(DatabaseFileConfig {
                db_type: Some(database_type_to_string(self.database_type)),
                postgres: Some(PostgresFileConfig {
                    user: Some(self.postgres_user.clone()),
                    password: Some(self.postgres_password.clone()),
                    url: Some(self.postgres_url.clone()),
                    port: Some(self.postgres_port.clone()),
                }),
            }),
            cluster: Some(ClusterFileConfig {
                enabled: Some(self.cluster_enabled),
                node_name: self.node_name.clone(),
                ip: Some(self.cluster_ip.clone()),
                port: Some(self.cluster_port),
                seeds: self.cluster_seeds.clone(),
            }),
            rpc: Some(RpcFileConfig {
                enabled: Some(self.rpc_enabled),
                ip: Some(self.rpc_ip.clone()),
                port: Some(self.rpc_port),
            }),
        }
    }
}

// ── ConfigManager ─────────────────────────────────────────────────────────────

#[derive(Debug)]
struct ConfigManagerInner {
    config: HesConfig,
    config_path: PathBuf,
}

/// Thread-safe wrapper around `HesConfig` that auto-saves changes to disk.
///
/// Cheap to clone (clones only the `Arc`). Pass by value to components that need
/// to mutate config at runtime.
#[derive(Debug, Clone)]
pub struct ConfigManager {
    inner: Arc<RwLock<ConfigManagerInner>>,
}

impl ConfigManager {
    fn new(config: HesConfig, config_path: PathBuf) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ConfigManagerInner {
                config,
                config_path,
            })),
        }
    }

    /// Returns a snapshot of the current configuration.
    pub async fn get(&self) -> HesConfig {
        self.inner.read().await.config.clone()
    }

    /// Applies `f` to the config, then persists the result to disk.
    pub async fn update<F>(&self, f: F) -> Result<(), ConfigError>
    where
        F: FnOnce(&mut HesConfig),
    {
        let mut guard = self.inner.write().await;
        f(&mut guard.config);
        let file_config = guard.config.to_file_config();
        let path = guard.config_path.clone();
        // Release the lock before doing I/O
        drop(guard);
        save_config_to_file(&path, &file_config)
    }
}

#[async_trait::async_trait]
impl common::config_store::ConfigStore for ConfigManager {
    async fn set_cluster_seeds(
        &self,
        seeds: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.update(|c| c.cluster_seeds = seeds).await.map_err(Into::into)
    }

    async fn get_all_config(
        &self,
    ) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>> {
        let c = self.get().await;
        let mut map = HashMap::new();
        map.insert("backdoor_ip".to_string(), c.backdoor_ip.clone());
        map.insert("backdoor_port".to_string(), c.backdoor_port.clone());
        map.insert("metrics_enabled".to_string(), c.metrics_enabled.to_string());
        map.insert("metrics_ip".to_string(), c.metrics_ip.clone());
        map.insert("metrics_port".to_string(), c.metrics_port.clone());
        map.insert("buckets_number".to_string(), c.buckets_number.to_string());
        map.insert("cluster_enabled".to_string(), c.cluster_enabled.to_string());
        map.insert("cluster_ip".to_string(), c.cluster_ip.clone());
        map.insert("cluster_port".to_string(), c.cluster_port.to_string());
        if let Some(seeds) = &c.cluster_seeds {
            map.insert("cluster_seeds".to_string(), seeds.clone());
        }
        if let Some(name) = &c.node_name {
            map.insert("node_name".to_string(), name.clone());
        }
        map.insert("rpc_enabled".to_string(), c.rpc_enabled.to_string());
        map.insert("rpc_ip".to_string(), c.rpc_ip.clone());
        map.insert("rpc_port".to_string(), c.rpc_port.to_string());
        // Read-only keys (informational)
        map.insert("node_id".to_string(), c.node_id.to_string());
        map.insert("database_type".to_string(), database_type_to_string(c.database_type));
        Ok(map)
    }

    async fn get_config_value(
        &self,
        key: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        let c = self.get().await;
        let val = match key {
            "backdoor_ip" => Some(c.backdoor_ip.clone()),
            "backdoor_port" => Some(c.backdoor_port.clone()),
            "metrics_enabled" => Some(c.metrics_enabled.to_string()),
            "metrics_ip" => Some(c.metrics_ip.clone()),
            "metrics_port" => Some(c.metrics_port.clone()),
            "buckets_number" => Some(c.buckets_number.to_string()),
            "cluster_enabled" => Some(c.cluster_enabled.to_string()),
            "cluster_ip" => Some(c.cluster_ip.clone()),
            "cluster_port" => Some(c.cluster_port.to_string()),
            "cluster_seeds" => c.cluster_seeds.clone(),
            "node_name" => c.node_name.clone(),
            "rpc_enabled" => Some(c.rpc_enabled.to_string()),
            "rpc_ip" => Some(c.rpc_ip.clone()),
            "rpc_port" => Some(c.rpc_port.to_string()),
            "node_id" => Some(c.node_id.to_string()),
            "database_type" => Some(database_type_to_string(c.database_type)),
            _ => None,
        };
        Ok(val)
    }

    async fn set_config_value(
        &self,
        key: &str,
        value: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Read-only keys
        match key {
            "node_id" | "database_type" => {
                return Err(format!("'{key}' is read-only").into());
            }
            _ => {}
        }

        // Validate and parse the value before mutating — closures can't return errors.
        enum Validated {
            Str(String),
            Bool(bool),
            Port(String), // stored as String in HesConfig
            U16(u16),
            Usize(usize),
            OptStr(Option<String>),
        }

        let validated: Validated = match key {
            "backdoor_ip" | "metrics_ip" | "cluster_ip" => {
                if value.is_empty() {
                    return Err(format!("'{key}' cannot be empty").into());
                }
                Validated::Str(value.to_string())
            }
            "backdoor_port" | "metrics_port" => {
                let p: u16 = value
                    .parse()
                    .map_err(|_| format!("'{value}' is not a valid port number (1-65535)"))?;
                if p == 0 {
                    return Err("port must be between 1 and 65535".into());
                }
                Validated::Port(p.to_string())
            }
            "metrics_enabled" | "cluster_enabled" => match value {
                "true" => Validated::Bool(true),
                "false" => Validated::Bool(false),
                _ => return Err(format!("'{value}' is not valid — use 'true' or 'false'").into()),
            },
            "buckets_number" => {
                let n: usize = value
                    .parse()
                    .map_err(|_| format!("'{value}' is not a valid positive integer"))?;
                if n == 0 {
                    return Err("buckets_number must be greater than 0".into());
                }
                Validated::Usize(n)
            }
            "cluster_port" => {
                let p: u16 = value
                    .parse()
                    .map_err(|_| format!("'{value}' is not a valid port number (1-65535)"))?;
                if p == 0 {
                    return Err("port must be between 1 and 65535".into());
                }
                Validated::U16(p)
            }
            "rpc_enabled" => match value {
                "true" => Validated::Bool(true),
                "false" => Validated::Bool(false),
                _ => return Err(format!("'{value}' is not valid — use 'true' or 'false'").into()),
            },
            "rpc_ip" => {
                if value.is_empty() {
                    return Err(format!("'{key}' cannot be empty").into());
                }
                Validated::Str(value.to_string())
            }
            "rpc_port" => {
                let p: u16 = value
                    .parse()
                    .map_err(|_| format!("'{value}' is not a valid port number (1-65535)"))?;
                if p == 0 {
                    return Err("port must be between 1 and 65535".into());
                }
                Validated::U16(p)
            }
            "cluster_seeds" => Validated::OptStr(if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            }),
            "node_name" => Validated::OptStr(if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            }),
            _ => return Err(format!("unknown config key '{key}'").into()),
        };

        self.update(|c| match (&validated, key) {
            (Validated::Str(v), "backdoor_ip") => c.backdoor_ip = v.clone(),
            (Validated::Str(v), "metrics_ip") => c.metrics_ip = v.clone(),
            (Validated::Str(v), "cluster_ip") => c.cluster_ip = v.clone(),
            (Validated::Port(v), "backdoor_port") => c.backdoor_port = v.clone(),
            (Validated::Port(v), "metrics_port") => c.metrics_port = v.clone(),
            (Validated::Bool(v), "metrics_enabled") => c.metrics_enabled = *v,
            (Validated::Bool(v), "cluster_enabled") => c.cluster_enabled = *v,
            (Validated::Usize(v), "buckets_number") => c.buckets_number = *v,
            (Validated::U16(v), "cluster_port") => c.cluster_port = *v,
            (Validated::Bool(v), "rpc_enabled") => c.rpc_enabled = *v,
            (Validated::Str(v), "rpc_ip") => c.rpc_ip = v.clone(),
            (Validated::U16(v), "rpc_port") => c.rpc_port = *v,
            (Validated::OptStr(v), "cluster_seeds") => c.cluster_seeds = v.clone(),
            (Validated::OptStr(v), "node_name") => c.node_name = v.clone(),
            _ => {}
        })
        .await
        .map_err(Into::into)
    }
}

// ── Error type ───────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("YAML parse error: {0}")]
    YamlParseError(#[from] serde_yaml::Error),
    #[error("Environment variable not found: ${{{0}}}")]
    EnvVarNotFound(String),
    #[error("Invalid node_id in config: {0}")]
    InvalidNodeId(String),
    #[error("Invalid database type '{0}'. Valid values: postgres, in-memory")]
    InvalidDatabaseType(String),
    #[error("Could not determine home directory")]
    NoHomeDirectory,
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Resolves the final config by merging: defaults → config file → CLI args.
///
/// If `cli_config_path` is `Some`, uses that path. Otherwise checks `~/.hes/config.yaml`.
/// If no config file exists, creates one with all resolved values.
/// Returns a `ConfigManager` that auto-saves any future mutations to disk.
pub fn resolve_config(
    cli_config_path: Option<String>,
    cli: CliOverrides,
) -> Result<ConfigManager, ConfigError> {
    let mut config = HesConfig::default();

    // Determine config file path
    let config_path = match cli_config_path {
        Some(ref p) => PathBuf::from(p),
        None => default_config_path()?,
    };

    // Load and apply file config if it exists
    let is_new_file = !config_path.exists();
    if !is_new_file {
        let file_config = load_file_config(&config_path)?;
        apply_file_config(&mut config, file_config)?;
        info!("Loaded config from {}", config_path.display());
    }

    // Apply CLI overrides (CLI wins over file)
    apply_cli_overrides(&mut config, cli)?;

    // Always persist the full resolved config (creates or updates the file)
    let file_config = config.to_file_config();
    save_config_to_file(&config_path, &file_config)?;
    if is_new_file {
        info!("Created config file at {}", config_path.display());
    }

    Ok(ConfigManager::new(config, config_path))
}

/// CLI values that can override config file. All optional — only `Some` values override.
pub struct CliOverrides {
    pub backdoor_ip: Option<String>,
    pub backdoor_port: Option<String>,
    pub no_metrics: bool,
    pub metrics_ip: Option<String>,
    pub metrics_port: Option<String>,
    pub buckets_number: Option<usize>,
    pub database_type: Option<DatabaseType>,
    pub postgres_user: Option<String>,
    pub postgres_password: Option<String>,
    pub postgres_url: Option<String>,
    pub postgres_port: Option<String>,
    pub disable_cluster: bool,
    pub node_name: Option<String>,
    pub cluster_ip: Option<String>,
    pub cluster_port: Option<u16>,
    pub cluster_seeds: Option<String>,
    pub disable_rpc: bool,
    pub rpc_ip: Option<String>,
    pub rpc_port: Option<u16>,
}

// ── Internal helpers ─────────────────────────────────────────────────────────

fn default_config_path() -> Result<PathBuf, ConfigError> {
    let home = dirs::home_dir().ok_or(ConfigError::NoHomeDirectory)?;
    Ok(home.join(".hes").join("config.yaml"))
}

/// Replaces all `${VAR}` patterns in `input` with values from the environment.
fn substitute_env_vars(input: &str) -> Result<String, ConfigError> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"\$\{([^}]+)\}").expect("valid regex")
    });

    let mut result = input.to_string();
    for cap in re.captures_iter(input) {
        let var_name = &cap[1];
        let var_value = env::var(var_name)
            .map_err(|_| ConfigError::EnvVarNotFound(var_name.to_string()))?;
        result = result.replace(&cap[0], &var_value);
    }
    Ok(result)
}

fn load_file_config(path: &Path) -> Result<FileConfig, ConfigError> {
    let raw = std::fs::read_to_string(path)?;
    let substituted = substitute_env_vars(&raw)?;
    let config: FileConfig = serde_yaml::from_str(&substituted)?;
    Ok(config)
}

fn apply_file_config(config: &mut HesConfig, fc: FileConfig) -> Result<(), ConfigError> {
    if let Some(ref id) = fc.node_id {
        config.node_id = Uuid::parse_str(id)
            .map_err(|e| ConfigError::InvalidNodeId(e.to_string()))?;
    }

    if let Some(b) = fc.backdoor {
        if let Some(v) = b.ip { config.backdoor_ip = v; }
        if let Some(v) = b.port { config.backdoor_port = v; }
    }

    if let Some(m) = fc.metrics {
        if let Some(v) = m.enabled { config.metrics_enabled = v; }
        if let Some(v) = m.ip { config.metrics_ip = v; }
        if let Some(v) = m.port { config.metrics_port = v; }
    }

    if let Some(s) = fc.scheduler {
        if let Some(v) = s.buckets_number { config.buckets_number = v; }
    }

    if let Some(db) = fc.database {
        if let Some(ref t) = db.db_type {
            config.database_type = parse_database_type(t)?;
        }
        if let Some(pg) = db.postgres {
            if let Some(v) = pg.user { config.postgres_user = v; }
            if let Some(v) = pg.password { config.postgres_password = v; }
            if let Some(v) = pg.url { config.postgres_url = v; }
            if let Some(v) = pg.port { config.postgres_port = v; }
        }
    }

    if let Some(cl) = fc.cluster {
        if let Some(v) = cl.enabled { config.cluster_enabled = v; }
        if let Some(v) = cl.node_name { config.node_name = Some(v); }
        if let Some(v) = cl.ip { config.cluster_ip = v; }
        if let Some(v) = cl.port { config.cluster_port = v; }
        if let Some(v) = cl.seeds { config.cluster_seeds = Some(v); }
    }

    if let Some(rpc) = fc.rpc {
        if let Some(v) = rpc.enabled { config.rpc_enabled = v; }
        if let Some(v) = rpc.ip { config.rpc_ip = v; }
        if let Some(v) = rpc.port { config.rpc_port = v; }
    }

    Ok(())
}

fn apply_cli_overrides(config: &mut HesConfig, cli: CliOverrides) -> Result<(), ConfigError> {
    if let Some(v) = cli.backdoor_ip { config.backdoor_ip = v; }
    if let Some(v) = cli.backdoor_port { config.backdoor_port = v; }
    if cli.no_metrics { config.metrics_enabled = false; }
    if let Some(v) = cli.metrics_ip { config.metrics_ip = v; }
    if let Some(v) = cli.metrics_port { config.metrics_port = v; }
    if let Some(v) = cli.buckets_number { config.buckets_number = v; }
    if let Some(v) = cli.database_type { config.database_type = v; }
    if let Some(v) = cli.postgres_user { config.postgres_user = v; }
    if let Some(v) = cli.postgres_password { config.postgres_password = v; }
    if let Some(v) = cli.postgres_url { config.postgres_url = v; }
    if let Some(v) = cli.postgres_port { config.postgres_port = v; }
    if cli.disable_cluster { config.cluster_enabled = false; }
    if cli.node_name.is_some() { config.node_name = cli.node_name; }
    if let Some(v) = cli.cluster_ip { config.cluster_ip = v; }
    if let Some(v) = cli.cluster_port { config.cluster_port = v; }
    if cli.cluster_seeds.is_some() { config.cluster_seeds = cli.cluster_seeds; }
    if cli.disable_rpc { config.rpc_enabled = false; }
    if let Some(v) = cli.rpc_ip { config.rpc_ip = v; }
    if let Some(v) = cli.rpc_port { config.rpc_port = v; }
    Ok(())
}

fn database_type_to_string(db_type: DatabaseType) -> String {
    match db_type {
        DatabaseType::Postgres => "postgres".to_string(),
        DatabaseType::InMemory => "in-memory".to_string(),
    }
}

/// Serializes `file_config` to YAML and writes it to `path`, creating parent dirs as needed.
fn save_config_to_file(path: &Path, file_config: &FileConfig) -> Result<(), ConfigError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let yaml = serde_yaml::to_string(file_config)?;
    std::fs::write(path, yaml)?;
    Ok(())
}

fn parse_database_type(s: &str) -> Result<DatabaseType, ConfigError> {
    match s {
        "postgres" => Ok(DatabaseType::Postgres),
        "in-memory" | "in_memory" | "inmemory" => Ok(DatabaseType::InMemory),
        other => Err(ConfigError::InvalidDatabaseType(other.to_string())),
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;
    use std::error::Error;
    use std::fs;

    fn no_cli() -> CliOverrides {
        CliOverrides {
            backdoor_ip: None,
            backdoor_port: None,
            no_metrics: false,
            metrics_ip: None,
            metrics_port: None,
            buckets_number: None,
            database_type: None,
            postgres_user: None,
            postgres_password: None,
            postgres_url: None,
            postgres_port: None,
            disable_cluster: false,
            node_name: None,
            cluster_ip: None,
            cluster_port: None,
            cluster_seeds: None,
            disable_rpc: false,
            rpc_ip: None,
            rpc_port: None,
        }
    }

    fn tmp_path(name: &str) -> String {
        temp_dir().join(name).to_string_lossy().into_owned()
    }

    #[test]
    fn test_default_config_values() {
        let config = HesConfig::default();
        assert_eq!(config.backdoor_ip, "0.0.0.0");
        assert_eq!(config.backdoor_port, "6565");
        assert!(config.metrics_enabled);
        assert_eq!(config.metrics_port, "6464");
        assert_eq!(config.buckets_number, 48);
        assert_eq!(config.cluster_port, 6570);
        assert!(config.cluster_enabled);
    }

    #[test]
    fn test_env_var_substitution() -> Result<(), Box<dyn Error>> {
        // SAFETY: test is single-threaded with respect to this variable
        unsafe { env::set_var("TEST_HES_PASSWORD", "secret123") };
        let result = substitute_env_vars("password: ${TEST_HES_PASSWORD}")?;
        assert_eq!(result, "password: secret123");
        unsafe { env::remove_var("TEST_HES_PASSWORD") };
        Ok(())
    }

    #[test]
    fn test_env_var_missing_returns_error() {
        let result = substitute_env_vars("password: ${TEST_HES_VAR_THAT_DOES_NOT_EXIST_XYZ}");
        assert!(matches!(result, Err(ConfigError::EnvVarNotFound(_))));
    }

    #[test]
    fn test_yaml_parsing() -> Result<(), Box<dyn Error>> {
        let yaml = r#"
node_id: "550e8400-e29b-41d4-a716-446655440000"
backdoor:
  ip: "192.168.1.1"
  port: "7777"
metrics:
  enabled: false
  port: "9090"
scheduler:
  buckets_number: 24
database:
  type: "in-memory"
cluster:
  enabled: false
  seeds: "10.0.0.1:6570"
"#;
        let fc: FileConfig = serde_yaml::from_str(yaml)?;
        assert_eq!(fc.node_id.as_deref(), Some("550e8400-e29b-41d4-a716-446655440000"));
        assert_eq!(fc.backdoor.as_ref().and_then(|b| b.ip.as_deref()), Some("192.168.1.1"));
        assert_eq!(fc.scheduler.as_ref().and_then(|s| s.buckets_number), Some(24));
        assert_eq!(fc.metrics.as_ref().and_then(|m| m.enabled), Some(false));
        assert_eq!(fc.cluster.as_ref().and_then(|c| c.enabled), Some(false));
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_file_over_defaults() -> Result<(), Box<dyn Error>> {
        let yaml = "backdoor:\n  port: \"9999\"\nscheduler:\n  buckets_number: 12\n";
        let path = tmp_path("hes_test_merge_file.yaml");
        fs::write(&path, yaml)?;

        let manager = resolve_config(Some(path.clone()), no_cli())?;
        let config = manager.get().await;
        assert_eq!(config.backdoor_port, "9999");
        assert_eq!(config.buckets_number, 12);
        assert_eq!(config.backdoor_ip, "0.0.0.0");
        assert_eq!(config.metrics_port, "6464");

        fs::remove_file(&path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_cli_over_file() -> Result<(), Box<dyn Error>> {
        let path = tmp_path("hes_test_merge_cli.yaml");
        fs::write(&path, "backdoor:\n  port: \"9999\"\n")?;

        let cli = CliOverrides { backdoor_port: Some("1234".to_string()), ..no_cli() };
        let manager = resolve_config(Some(path.clone()), cli)?;
        let config = manager.get().await;
        assert_eq!(config.backdoor_port, "1234");

        fs::remove_file(&path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn test_node_id_preserved_from_file() -> Result<(), Box<dyn Error>> {
        let fixed_id = "550e8400-e29b-41d4-a716-446655440000";
        let path = tmp_path("hes_test_node_id.yaml");
        fs::write(&path, format!("node_id: \"{fixed_id}\"\n"))?;

        let manager = resolve_config(Some(path.clone()), no_cli())?;
        let config = manager.get().await;
        assert_eq!(config.node_id.to_string(), fixed_id);

        fs::remove_file(&path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn test_node_id_generated_and_persisted_when_missing() -> Result<(), Box<dyn Error>> {
        let path = tmp_path("hes_test_node_id_generated.yaml");
        fs::remove_file(&path).ok();

        let manager = resolve_config(Some(path.clone()), no_cli())?;
        let config = manager.get().await;
        assert!(PathBuf::from(&path).exists(), "config file should have been created");

        let written = fs::read_to_string(&path)?;
        assert!(written.contains(&config.node_id.to_string()));

        fs::remove_file(&path).ok();
        Ok(())
    }

    #[test]
    fn test_database_type_parsing() {
        assert!(matches!(parse_database_type("postgres"), Ok(DatabaseType::Postgres)));
        assert!(matches!(parse_database_type("in-memory"), Ok(DatabaseType::InMemory)));
        assert!(matches!(parse_database_type("in_memory"), Ok(DatabaseType::InMemory)));
        assert!(matches!(parse_database_type("bad"), Err(ConfigError::InvalidDatabaseType(_))));
    }

    #[test]
    fn test_partial_yaml_no_error() -> Result<(), Box<dyn Error>> {
        let fc: FileConfig = serde_yaml::from_str("metrics:\n  port: \"9090\"\n")?;
        assert!(fc.node_id.is_none());
        assert!(fc.backdoor.is_none());
        assert_eq!(fc.metrics.and_then(|m| m.port).as_deref(), Some("9090"));
        Ok(())
    }

    #[test]
    fn test_empty_yaml_no_error() -> Result<(), Box<dyn Error>> {
        let fc: FileConfig = serde_yaml::from_str("")?;
        assert!(fc.node_id.is_none());
        assert!(fc.backdoor.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_no_metrics_cli_flag_disables_metrics() -> Result<(), Box<dyn Error>> {
        let path = tmp_path("hes_test_no_metrics.yaml");
        fs::remove_file(&path).ok();

        let cli = CliOverrides { no_metrics: true, ..no_cli() };
        let manager = resolve_config(Some(path.clone()), cli)?;
        let config = manager.get().await;
        assert!(!config.metrics_enabled);

        fs::remove_file(&path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn test_disable_cluster_cli_flag() -> Result<(), Box<dyn Error>> {
        let path = tmp_path("hes_test_disable_cluster.yaml");
        fs::remove_file(&path).ok();

        let cli = CliOverrides { disable_cluster: true, ..no_cli() };
        let manager = resolve_config(Some(path.clone()), cli)?;
        let config = manager.get().await;
        assert!(!config.cluster_enabled);

        fs::remove_file(&path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn test_auto_save_persists_change() -> Result<(), Box<dyn Error>> {
        let path = tmp_path("hes_test_auto_save.yaml");
        fs::remove_file(&path).ok();

        let manager = resolve_config(Some(path.clone()), no_cli())?;
        manager.update(|c| c.buckets_number = 99).await?;

        let written = fs::read_to_string(&path)?;
        assert!(written.contains("99"), "YAML should contain updated buckets_number");

        // Re-load and verify the value round-trips
        let manager2 = resolve_config(Some(path.clone()), no_cli())?;
        let config2 = manager2.get().await;
        assert_eq!(config2.buckets_number, 99);

        fs::remove_file(&path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn test_auto_save_preserves_all_fields() -> Result<(), Box<dyn Error>> {
        let path = tmp_path("hes_test_auto_save_full.yaml");
        fs::remove_file(&path).ok();

        let cli = CliOverrides {
            backdoor_port: Some("7777".to_string()),
            buckets_number: Some(24),
            ..no_cli()
        };
        let manager = resolve_config(Some(path.clone()), cli)?;

        // Mutate one field
        manager.update(|c| c.node_name = Some("test-node".to_string())).await?;

        // Re-load: both CLI-overridden fields and the new mutation should be present
        let manager2 = resolve_config(Some(path.clone()), no_cli())?;
        let config2 = manager2.get().await;
        assert_eq!(config2.backdoor_port, "7777");
        assert_eq!(config2.buckets_number, 24);
        assert_eq!(config2.node_name.as_deref(), Some("test-node"));

        fs::remove_file(&path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_update() -> Result<(), Box<dyn Error>> {
        let path = tmp_path("hes_test_batch_update.yaml");
        fs::remove_file(&path).ok();

        let manager = resolve_config(Some(path.clone()), no_cli())?;

        // Apply multiple mutations in one update call
        manager.update(|c| {
            c.buckets_number = 12;
            c.cluster_seeds = Some("10.0.0.1:6570".to_string());
            c.node_name = Some("batch-node".to_string());
        }).await?;

        let written = fs::read_to_string(&path)?;
        assert!(written.contains("12"));
        assert!(written.contains("10.0.0.1:6570"));
        assert!(written.contains("batch-node"));

        fs::remove_file(&path).ok();
        Ok(())
    }
}
