use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rulemorph_trace::TraceManifest;

pub fn unique_temp_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("rulemorph-trace-test-{pid}-{nanos}-{counter}"))
}

pub fn create_temp_dir() -> anyhow::Result<PathBuf> {
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir)?;
    Ok(temp_dir)
}

pub fn sampling_bucket(value: &str) -> f64 {
    fn fnv1a64(bytes: &[u8]) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        let mut hash = FNV_OFFSET;
        for byte in bytes {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let hash = fnv1a64(value.as_bytes());
    (hash as f64) / (u64::MAX as f64)
}

pub fn read_manifest_payload(manifest_path: impl AsRef<Path>) -> anyhow::Result<String> {
    Ok(fs::read_to_string(manifest_path)?)
}

pub fn read_manifest(manifest_path: impl AsRef<Path>) -> anyhow::Result<TraceManifest> {
    let payload = read_manifest_payload(manifest_path)?;
    Ok(serde_json::from_str(&payload)?)
}

pub fn read_manifest_value(manifest_path: impl AsRef<Path>) -> anyhow::Result<serde_json::Value> {
    let payload = read_manifest_payload(manifest_path)?;
    Ok(serde_json::from_str(&payload)?)
}

pub fn trace_dir(manifest_path: &Path) -> &Path {
    manifest_path.parent().expect("trace dir should exist")
}
