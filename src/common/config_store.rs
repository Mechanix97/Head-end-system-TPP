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
}
