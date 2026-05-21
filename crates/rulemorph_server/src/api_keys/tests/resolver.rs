use anyhow::Result;
use tempfile::tempdir;

use crate::{TenantLayout, TenantResolver};

use super::super::{ApiKeyResolver, ApiKeyStore, crypto::API_KEY_PREFIX};

#[tokio::test]
async fn api_key_resolver_resolves_tenant_bound_store() -> Result<()> {
    let temp = tempdir()?;
    let base_dir = temp.path().join("data");
    let tenant_id = "tenant-a";
    let layout = TenantLayout::new(base_dir.clone(), tenant_id)?;
    let mut store = ApiKeyStore::load_or_init(layout.api_keys_path(), tenant_id)?;
    let issued = store.issue(Some("tenant key".to_string()))?;
    let other_tenant_id = "tenant-b";
    let other_layout = TenantLayout::new(base_dir.clone(), other_tenant_id)?;
    let mut other_store = ApiKeyStore::load_or_init(other_layout.api_keys_path(), other_tenant_id)?;
    let _other_issued = other_store.issue(Some("other tenant key".to_string()))?;
    let resolver = ApiKeyResolver::new(base_dir);

    let resolved = resolver
        .resolve(&issued.key)
        .await?
        .expect("issued key resolves tenant");
    assert_eq!(resolved.tenant_id, tenant_id);

    let original_key_prefix = format!("{}{}.", API_KEY_PREFIX, tenant_id);
    let tampered_key_prefix = format!("{}{}.", API_KEY_PREFIX, other_tenant_id);
    let tampered_tenant_key = issued
        .key
        .replacen(&original_key_prefix, &tampered_key_prefix, 1);
    assert!(resolver.resolve(&tampered_tenant_key).await?.is_none());
    assert!(resolver.resolve("rmk_bad/tenant.secret").await?.is_none());
    Ok(())
}
