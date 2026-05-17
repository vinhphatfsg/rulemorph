use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

const API_KEY_VERSION: u8 = 1;

mod crypto;
mod file_lock;
mod resolver;

use self::crypto::{
    API_KEY_PREFIX, ID_BYTES, PREFIX_VISIBLE_CHARS, SALT_BYTES, SECRET_BYTES, hash_key,
    random_base64,
};
use self::file_lock::ApiKeyFileLock;
pub use self::resolver::{ApiKeyResolver, ParsedApiKey, parse_api_key};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    pub id: String,
    pub prefix: String,
    pub hash: String,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ApiKeyInfo {
    pub id: String,
    pub prefix: String,
    pub created_at: String,
    pub revoked_at: Option<String>,
    pub label: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ApiKeyIssueResult {
    pub id: String,
    pub key: String,
    pub prefix: String,
    pub created_at: String,
    pub label: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ApiKeyFile {
    version: u8,
    salt: String,
    keys: Vec<ApiKeyRecord>,
}

#[derive(Clone, Debug)]
pub struct ApiKeyStore {
    tenant_id: String,
    path: PathBuf,
    salt: String,
    keys: Vec<ApiKeyRecord>,
}

impl ApiKeyStore {
    pub fn load(path: PathBuf, tenant_id: &str) -> Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read api key store: {}", path.display()))?;
        let file: ApiKeyFile = serde_json::from_str(&raw)
            .with_context(|| format!("invalid api key store: {}", path.display()))?;
        if file.version != API_KEY_VERSION {
            anyhow::bail!("unsupported api key store version: {}", file.version);
        }
        Ok(Some(Self {
            tenant_id: tenant_id.to_string(),
            path,
            salt: file.salt,
            keys: file.keys,
        }))
    }

    pub fn load_or_init(path: PathBuf, tenant_id: &str) -> Result<Self> {
        if let Some(existing) = Self::load(path.clone(), tenant_id)? {
            return Ok(existing);
        }
        let salt = random_base64(SALT_BYTES);
        Ok(Self {
            tenant_id: tenant_id.to_string(),
            path,
            salt,
            keys: Vec::new(),
        })
    }

    pub fn issue(&mut self, label: Option<String>) -> Result<ApiKeyIssueResult> {
        let _lock = ApiKeyFileLock::acquire(&self.path)?;
        self.reload_or_keep()?;
        self.issue_unlocked(label)
    }

    fn issue_unlocked(&mut self, label: Option<String>) -> Result<ApiKeyIssueResult> {
        let secret = random_base64(SECRET_BYTES);
        let key = format!("{}{}.{}", API_KEY_PREFIX, self.tenant_id, secret);
        let created_at = now_rfc3339();
        let id = random_base64(ID_BYTES);
        let prefix = format!(
            "{}{}.{}",
            API_KEY_PREFIX,
            self.tenant_id,
            secret
                .chars()
                .take(PREFIX_VISIBLE_CHARS)
                .collect::<String>()
        );
        let hash = hash_key(&self.salt, &key)?;
        self.keys.push(ApiKeyRecord {
            id: id.clone(),
            prefix: prefix.clone(),
            hash,
            created_at: created_at.clone(),
            revoked_at: None,
            label: label.clone(),
        });
        self.save_unlocked()?;
        Ok(ApiKeyIssueResult {
            id,
            key,
            prefix,
            created_at,
            label,
        })
    }

    pub fn list(&self) -> Vec<ApiKeyInfo> {
        self.keys
            .iter()
            .map(|record| ApiKeyInfo {
                id: record.id.clone(),
                prefix: record.prefix.clone(),
                created_at: record.created_at.clone(),
                revoked_at: record.revoked_at.clone(),
                label: record.label.clone(),
            })
            .collect()
    }

    pub fn revoke(&mut self, id: &str) -> Result<bool> {
        let _lock = ApiKeyFileLock::acquire(&self.path)?;
        self.reload_or_keep()?;
        self.revoke_unlocked(id)
    }

    fn revoke_unlocked(&mut self, id: &str) -> Result<bool> {
        let mut updated = false;
        let now = now_rfc3339();
        for record in &mut self.keys {
            if record.id == id && record.revoked_at.is_none() {
                record.revoked_at = Some(now.clone());
                updated = true;
            }
        }
        if updated {
            self.save_unlocked()?;
        }
        Ok(updated)
    }

    pub fn rotate(&mut self, id: &str, label: Option<String>) -> Result<Option<ApiKeyIssueResult>> {
        let _lock = ApiKeyFileLock::acquire(&self.path)?;
        self.reload_or_keep()?;
        let revoked = self.revoke_unlocked(id)?;
        if !revoked {
            return Ok(None);
        }
        let issued = self.issue_unlocked(label)?;
        Ok(Some(issued))
    }

    pub fn verify(&self, api_key: &str) -> Result<bool> {
        let parsed = match parse_api_key(api_key) {
            Some(parsed) => parsed,
            None => return Ok(false),
        };
        if parsed.tenant_id != self.tenant_id {
            return Ok(false);
        }
        let hash = hash_key(&self.salt, &parsed.full_key)?;
        Ok(self
            .keys
            .iter()
            .any(|record| record.hash == hash && record.revoked_at.is_none()))
    }

    fn reload_or_keep(&mut self) -> Result<()> {
        let Some(existing) = Self::load(self.path.clone(), &self.tenant_id)? else {
            return Ok(());
        };
        self.salt = existing.salt;
        self.keys = existing.keys;
        Ok(())
    }

    fn save_unlocked(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create api key dir: {}", parent.display()))?;
        }
        let file = ApiKeyFile {
            version: API_KEY_VERSION,
            salt: self.salt.clone(),
            keys: self.keys.clone(),
        };
        let payload = serde_json::to_vec_pretty(&file)?;
        let temp_path = tmp_path(&self.path);
        let mut handle = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .with_context(|| format!("failed to open temp file: {}", temp_path.display()))?;
        handle.write_all(&payload)?;
        handle.sync_all()?;
        replace_file(&temp_path, &self.path)?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn tmp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("api_keys.json");
    let suffix = random_base64(6);
    let tmp_name = format!("{}.tmp-{}", file_name, suffix);
    path.with_file_name(tmp_name)
}

fn replace_file(temp_path: &Path, target_path: &Path) -> Result<()> {
    match fs::rename(temp_path, target_path) {
        Ok(()) => Ok(()),
        Err(rename_err) => {
            #[cfg(windows)]
            {
                if target_path.exists() {
                    fs::remove_file(target_path).with_context(|| {
                        format!(
                            "failed to remove existing api key store: {}",
                            target_path.display()
                        )
                    })?;
                    fs::rename(temp_path, target_path).with_context(|| {
                        format!(
                            "failed to replace api key store after remove: {}",
                            target_path.display()
                        )
                    })?;
                    return Ok(());
                }
            }
            Err(rename_err).with_context(|| {
                format!("failed to replace api key store: {}", target_path.display())
            })
        }
    }
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

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
