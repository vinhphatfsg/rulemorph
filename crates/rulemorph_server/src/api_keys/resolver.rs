use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;

use super::{crypto::API_KEY_PREFIX, store::ApiKeyStore};
use crate::{TenantContext, TenantLayout, TenantResolver, validate_tenant_id};

#[derive(Clone, Debug)]
pub struct ParsedApiKey {
    pub tenant_id: String,
    pub secret: String,
    pub full_key: String,
}

#[derive(Clone)]
pub struct ApiKeyResolver {
    base_dir: PathBuf,
}

impl ApiKeyResolver {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }
}

#[async_trait]
impl TenantResolver for ApiKeyResolver {
    async fn resolve(&self, api_key: &str) -> Result<Option<TenantContext>> {
        let parsed = match parse_api_key(api_key) {
            Some(parsed) => parsed,
            None => return Ok(None),
        };
        if validate_tenant_id(&parsed.tenant_id).is_err() {
            return Ok(None);
        }
        let layout = TenantLayout::new(self.base_dir.clone(), &parsed.tenant_id)?;
        let store = ApiKeyStore::load(layout.api_keys_path(), &parsed.tenant_id)?;
        let Some(store) = store else {
            return Ok(None);
        };
        if store.verify(&parsed.full_key)? {
            Ok(Some(TenantContext::new(parsed.tenant_id)))
        } else {
            Ok(None)
        }
    }
}

pub fn parse_api_key(value: &str) -> Option<ParsedApiKey> {
    let trimmed = value.trim();
    if !trimmed.starts_with(API_KEY_PREFIX) {
        return None;
    }
    let rest = trimmed.strip_prefix(API_KEY_PREFIX)?;
    let mut parts = rest.splitn(2, '.');
    let tenant_id = parts.next()?.trim();
    let secret = parts.next()?.trim();
    if tenant_id.is_empty() || secret.is_empty() {
        return None;
    }
    Some(ParsedApiKey {
        tenant_id: tenant_id.to_string(),
        secret: secret.to_string(),
        full_key: trimmed.to_string(),
    })
}
