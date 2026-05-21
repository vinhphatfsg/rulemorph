use anyhow::Result;
use tempfile::tempdir;

use super::super::ApiKeyStore;

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
