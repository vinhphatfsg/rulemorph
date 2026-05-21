use std::fs;

use anyhow::Result;
use tempfile::tempdir;

use super::super::{ApiKeyStore, file_lock::lock_path};

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
