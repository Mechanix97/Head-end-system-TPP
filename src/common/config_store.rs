use std::collections::HashMap;

/// Trait for persisting runtime config mutations to durable storage.
///
/// Components accept `Arc<dyn ConfigStore>` to stay decoupled from the
/// concrete `ConfigManager` defined in the binary.
#[async_trait::async_trait]
pub trait ConfigStore: Send + Sync {
    /// Persists the cluster seed addresses (comma-separated `ip:port` list).
    async fn set_cluster_seeds(
        &self,
        seeds: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Returns all configurable keys and their current string values.
    async fn get_all_config(
        &self,
    ) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>>;

    /// Returns the current value of a single config key, or `None` if the key is unknown.
    async fn get_config_value(
        &self,
        key: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>>;

    /// Updates a single config key and persists the change.
    ///
    /// Returns an error if the key is unknown or the value is invalid.
    async fn set_config_value(
        &self,
        key: &str,
        value: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// No-op implementation for use in tests.
pub struct NoOpConfigStore;

#[async_trait::async_trait]
impl ConfigStore for NoOpConfigStore {
    async fn set_cluster_seeds(
        &self,
        _seeds: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn get_all_config(
        &self,
    ) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(HashMap::new())
    }

    async fn get_config_value(
        &self,
        _key: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(None)
    }

    async fn set_config_value(
        &self,
        _key: &str,
        _value: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}
