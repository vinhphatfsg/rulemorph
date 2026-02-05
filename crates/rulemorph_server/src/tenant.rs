use anyhow::Result;
use async_trait::async_trait;

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TenantContext {
    pub tenant_id: String,
}

impl TenantContext {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
        }
    }
}

#[async_trait]
pub trait TenantResolver: Send + Sync {
    async fn resolve(&self, api_key: &str) -> Result<Option<TenantContext>>;
}
