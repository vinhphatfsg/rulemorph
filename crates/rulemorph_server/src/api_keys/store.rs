mod file;

use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::crypto::{
    API_KEY_PREFIX, ID_BYTES, PREFIX_VISIBLE_CHARS, SALT_BYTES, SECRET_BYTES, hash_key,
    random_base64,
};
use super::file_lock::ApiKeyFileLock;
use super::resolver::parse_api_key;
use file::{load_store_file, save_store_file};

const API_KEY_VERSION: u8 = 1;

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
        let file = load_store_file(&path, API_KEY_VERSION)?;
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
        save_store_file(&self.path, API_KEY_VERSION, &self.salt, &self.keys)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}
