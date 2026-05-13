use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use sha2::{Digest, Sha256};

pub(super) const API_KEY_PREFIX: &str = "rmk_";
pub(super) const SECRET_BYTES: usize = 32;
pub(super) const SALT_BYTES: usize = 16;
pub(super) const ID_BYTES: usize = 12;
pub(super) const PREFIX_VISIBLE_CHARS: usize = 8;

pub(super) fn random_base64(size: usize) -> String {
    let mut buf = vec![0u8; size];
    rand::thread_rng().fill_bytes(&mut buf);
    URL_SAFE_NO_PAD.encode(buf)
}

pub(super) fn hash_key(salt_b64: &str, key: &str) -> Result<String> {
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
