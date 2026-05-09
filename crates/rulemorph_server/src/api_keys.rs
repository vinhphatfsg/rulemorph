use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use async_trait::async_trait;

use crate::{TenantContext, TenantLayout, TenantResolver, validate_tenant_id};

const API_KEY_VERSION: u8 = 1;
const API_KEY_PREFIX: &str = "rmk_";
const SECRET_BYTES: usize = 32;
const SALT_BYTES: usize = 16;
const ID_BYTES: usize = 12;
const PREFIX_VISIBLE_CHARS: usize = 8;
const API_KEY_LOCK_STALE_AFTER: Duration = Duration::from_secs(300);

#[cfg(unix)]
unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

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

struct ApiKeyFileLock {
    path: PathBuf,
}

impl ApiKeyFileLock {
    fn acquire(path: &Path) -> Result<Self> {
        let lock_path = lock_path(path);
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create api key lock dir: {}", parent.display())
            })?;
        }
        let mut last_err = None;
        for _ in 0..250 {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(mut handle) => {
                    writeln!(handle, "{}", std::process::id()).ok();
                    return Ok(Self { path: lock_path });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    if remove_stale_lock_if_needed(&lock_path)? {
                        continue;
                    }
                    last_err = Some(err);
                    thread::sleep(Duration::from_millis(20));
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to create api key lock: {}", lock_path.display())
                    });
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "api key lock timeout")
        }))
        .with_context(|| {
            format!(
                "timed out waiting for api key lock: {}",
                lock_path.display()
            )
        })
    }
}

fn remove_stale_lock_if_needed(path: &Path) -> Result<bool> {
    match fs::read_to_string(path) {
        Ok(contents) => {
            if let Ok(pid) = contents.trim().parse::<u32>() {
                if pid != std::process::id() && !process_is_running(pid) {
                    remove_lock_file(path)?;
                    return Ok(true);
                }
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(_) => {}
    }

    let is_old = fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|age| age >= API_KEY_LOCK_STALE_AFTER);
    if is_old {
        remove_lock_file(path)?;
        return Ok(true);
    }
    Ok(false)
}

fn remove_lock_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => {
            Err(err).with_context(|| format!("failed to remove stale lock: {}", path.display()))
        }
    }
}

#[cfg(unix)]
fn process_is_running(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    let rc = unsafe { kill(pid, 0) };
    if rc == 0 {
        return true;
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(1) => true,
        Some(3) => false,
        _ => true,
    }
}

#[cfg(not(unix))]
fn process_is_running(_pid: u32) -> bool {
    true
}

impl Drop for ApiKeyFileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
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

fn tmp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("api_keys.json");
    let suffix = random_base64(6);
    let tmp_name = format!("{}.tmp-{}", file_name, suffix);
    path.with_file_name(tmp_name)
}

fn lock_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("api_keys.json");
    path.with_file_name(format!("{}.lock", file_name))
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

fn random_base64(size: usize) -> String {
    let mut buf = vec![0u8; size];
    rand::thread_rng().fill_bytes(&mut buf);
    URL_SAFE_NO_PAD.encode(buf)
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

fn hash_key(salt_b64: &str, key: &str) -> Result<String> {
    let salt = URL_SAFE_NO_PAD
        .decode(salt_b64)
        .context("invalid api key salt")?;
    let mut hasher = Sha256::new();
    hasher.update(&salt);
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    Ok(hex_encode(&digest))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;
    use tempfile::tempdir;

    use super::{ApiKeyStore, lock_path};

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
