use std::path::PathBuf;

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

pub fn validate_tenant_id(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        anyhow::bail!("tenant_id is empty");
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        anyhow::bail!("tenant_id contains invalid characters");
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct TenantLayout {
    base_dir: PathBuf,
    tenant_id: String,
}

impl TenantLayout {
    pub fn new(base_dir: PathBuf, tenant_id: &str) -> Result<Self> {
        validate_tenant_id(tenant_id)?;
        Ok(Self {
            base_dir,
            tenant_id: tenant_id.to_string(),
        })
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn tenant_root(&self) -> PathBuf {
        self.base_dir.join("tenants").join(&self.tenant_id)
    }

    pub fn data_dir(&self) -> PathBuf {
        self.tenant_root()
    }

    pub fn rules_dir(&self) -> PathBuf {
        self.tenant_root().join("rules")
    }

    pub fn api_rules_dir(&self) -> PathBuf {
        self.tenant_root().join("api_rules")
    }

    pub fn auth_dir(&self) -> PathBuf {
        self.tenant_root().join("auth")
    }

    pub fn api_keys_path(&self) -> PathBuf {
        self.auth_dir().join("api_keys.json")
    }
}

#[async_trait]
pub trait TenantResolver: Send + Sync {
    async fn resolve(&self, api_key: &str) -> Result<Option<TenantContext>>;
}
