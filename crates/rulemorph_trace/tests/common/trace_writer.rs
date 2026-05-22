use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rulemorph_trace::{TraceManifest, TraceStore};
use serde_json::{Map, Value};

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

pub fn assert_no_detail_artifacts(manifest_path: &Path) -> anyhow::Result<()> {
    for entry in fs::read_dir(trace_dir(manifest_path))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        assert!(
            !name.starts_with("records-")
                && !name.starts_with("nodes-")
                && !name.starts_with("finalize.json")
                && !name.contains(".tmp-"),
            "unexpected detail file: {name}"
        );
    }

    Ok(())
}

pub fn read_ndjson_lines(path: impl AsRef<Path>) -> anyhow::Result<Vec<String>> {
    let payload = fs::read_to_string(path)?;
    Ok(payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect())
}

pub fn read_ndjson_values(path: impl AsRef<Path>) -> anyhow::Result<Vec<serde_json::Value>> {
    read_ndjson_lines(path)?
        .into_iter()
        .map(|line| Ok(serde_json::from_str(&line)?))
        .collect()
}

pub fn write_ndjson_lines(path: impl AsRef<Path>, lines: &[String]) -> anyhow::Result<()> {
    fs::write(path, format!("{}\n", lines.join("\n")))?;
    Ok(())
}

pub async fn load_trace(data_dir: impl AsRef<Path>, trace_id: &str) -> anyhow::Result<Value> {
    let store = TraceStore::new(data_dir.as_ref().to_path_buf()).await?;
    Ok(store.get(trace_id).await?.expect("trace should load"))
}

pub fn object_field<'a>(value: &'a Value, field: &str) -> &'a Map<String, Value> {
    value
        .get(field)
        .and_then(|value| value.as_object())
        .unwrap_or_else(|| panic!("{field} object"))
}

pub fn object_member<'a>(object: &'a Map<String, Value>, field: &str) -> &'a Map<String, Value> {
    object
        .get(field)
        .and_then(|value| value.as_object())
        .unwrap_or_else(|| panic!("{field} object"))
}

pub fn array_field<'a>(value: &'a Value, field: &str) -> &'a Vec<Value> {
    value
        .get(field)
        .and_then(|value| value.as_array())
        .unwrap_or_else(|| panic!("{field} array"))
}

pub fn array_member<'a>(object: &'a Map<String, Value>, field: &str) -> &'a Vec<Value> {
    object
        .get(field)
        .and_then(|value| value.as_array())
        .unwrap_or_else(|| panic!("{field} array"))
}

pub fn first_record_object(value: &Value) -> &Map<String, Value> {
    array_field(value, "records")
        .first()
        .and_then(|value| value.as_object())
        .expect("record object")
}
