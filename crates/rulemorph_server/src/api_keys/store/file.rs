use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::ApiKeyRecord;
use crate::api_keys::crypto::random_base64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ApiKeyFile {
    pub(super) version: u8,
    pub(super) salt: String,
    pub(super) keys: Vec<ApiKeyRecord>,
}

pub(super) fn load_store_file(path: &Path, expected_version: u8) -> Result<ApiKeyFile> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read api key store: {}", path.display()))?;
    let file: ApiKeyFile = serde_json::from_str(&raw)
        .with_context(|| format!("invalid api key store: {}", path.display()))?;
    if file.version != expected_version {
        anyhow::bail!("unsupported api key store version: {}", file.version);
    }
    Ok(file)
}

pub(super) fn save_store_file(
    path: &Path,
    version: u8,
    salt: &str,
    keys: &[ApiKeyRecord],
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create api key dir: {}", parent.display()))?;
    }
    let file = ApiKeyFile {
        version,
        salt: salt.to_string(),
        keys: keys.to_vec(),
    };
    let payload = serde_json::to_vec_pretty(&file)?;
    let temp_path = tmp_path(path);
    let mut handle = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .with_context(|| format!("failed to open temp file: {}", temp_path.display()))?;
    handle.write_all(&payload)?;
    handle.sync_all()?;
    replace_file(&temp_path, path)?;
    Ok(())
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
