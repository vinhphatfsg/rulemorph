mod crypto;
mod file_lock;
mod resolver;
mod store;

pub use self::resolver::{ApiKeyResolver, ParsedApiKey, parse_api_key};
pub use self::store::{ApiKeyInfo, ApiKeyIssueResult, ApiKeyRecord, ApiKeyStore};

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;
    use tempfile::tempdir;

    use crate::{TenantLayout, TenantResolver};

    use super::{ApiKeyResolver, ApiKeyStore, file_lock::lock_path};

    #[test]
    fn api_key_store_persists_multiple_updates() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("api_keys.json");
        let tenant_id = "tenant-a";

        let mut store = ApiKeyStore::load_or_init(path.clone(), tenant_id)?;
        let first = store.issue(Some("first".to_string()))?;
        let second = store.issue(Some("second".to_string()))?;

        let mut reloaded =
            ApiKeyStore::load(path.clone(), tenant_id)?.expect("api key store should exist");
        assert_eq!(reloaded.list().len(), 2);

        assert!(reloaded.revoke(&first.id)?);
        let rotated = reloaded
            .rotate(&second.id, Some("rotated".to_string()))?
            .expect("rotate should issue a new key");

        let final_store = ApiKeyStore::load(path, tenant_id)?.expect("api key store should exist");
        assert_eq!(final_store.list().len(), 3);
        assert!(!final_store.verify(&first.key)?);
        assert!(!final_store.verify(&second.key)?);
        assert!(final_store.verify(&rotated.key)?);
        Ok(())
    }

    #[test]
    fn stale_store_save_does_not_resurrect_revoked_key() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("api_keys.json");
        let tenant_id = "tenant-a";

        let mut first_writer = ApiKeyStore::load_or_init(path.clone(), tenant_id)?;
        let first = first_writer.issue(Some("first".to_string()))?;

        let mut stale_writer =
            ApiKeyStore::load(path.clone(), tenant_id)?.expect("api key store should exist");
        let mut revoker =
            ApiKeyStore::load(path.clone(), tenant_id)?.expect("api key store should exist");
        assert!(revoker.revoke(&first.id)?);

        let second = stale_writer.issue(Some("second".to_string()))?;
        let final_store = ApiKeyStore::load(path, tenant_id)?.expect("api key store should exist");
        assert!(!final_store.verify(&first.key)?);
        assert!(final_store.verify(&second.key)?);
        Ok(())
    }

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
        let mut other_store =
            ApiKeyStore::load_or_init(other_layout.api_keys_path(), other_tenant_id)?;
        let _other_issued = other_store.issue(Some("other tenant key".to_string()))?;
        let resolver = ApiKeyResolver::new(base_dir);

        let resolved = resolver
            .resolve(&issued.key)
            .await?
            .expect("issued key resolves tenant");
        assert_eq!(resolved.tenant_id, tenant_id);

        let original_key_prefix = format!("{}{}.", super::crypto::API_KEY_PREFIX, tenant_id);
        let tampered_key_prefix = format!("{}{}.", super::crypto::API_KEY_PREFIX, other_tenant_id);
        let tampered_tenant_key =
            issued
                .key
                .replacen(&original_key_prefix, &tampered_key_prefix, 1);
        assert!(resolver.resolve(&tampered_tenant_key).await?.is_none());
        assert!(resolver.resolve("rmk_bad/tenant.secret").await?.is_none());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn dead_process_lock_file_is_recovered() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("api_keys.json");
        let tenant_id = "tenant-a";
        fs::write(lock_path(&path), "999999\n")?;

        let mut store = ApiKeyStore::load_or_init(path.clone(), tenant_id)?;
        let issued = store.issue(Some("first".to_string()))?;

        let final_store = ApiKeyStore::load(path, tenant_id)?.expect("api key store should exist");
        assert!(final_store.verify(&issued.key)?);
        Ok(())
    }
}
