use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use chrono::{Datelike, Utc};
use serde_json::Value as JsonValue;
use tracing::warn;

pub(super) fn resolve_trace_timestamp(trace: &JsonValue) -> String {
    trace
        .get("timestamp")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
        .unwrap_or_else(|| Utc::now().to_rfc3339())
}

pub(super) fn trace_dir_base_for_timestamp(data_dir: &Path, timestamp: &str) -> PathBuf {
    let (year, month, day) = parse_date_parts(timestamp).unwrap_or_else(|| {
        let now = Utc::now();
        (now.year(), now.month(), now.day())
    });

    data_dir
        .join("traces")
        .join(format!("{year:04}"))
        .join(format!("{month:02}"))
        .join(format!("{day:02}"))
}

fn parse_date_parts(timestamp: &str) -> Option<(i32, u32, u32)> {
    let parsed = chrono::DateTime::parse_from_rfc3339(timestamp).ok()?;
    Some((parsed.year(), parsed.month(), parsed.day()))
}

pub(super) fn ensure_unique_trace_dir(
    base_dir: &Path,
    trace_id: String,
    raw_trace_id: &str,
) -> Result<(String, PathBuf)> {
    fs::create_dir_all(base_dir)?;
    let base_id = trace_id;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut counter = 0usize;
    loop {
        let candidate = if counter == 0 {
            base_id.clone()
        } else {
            let suffix = if counter == 1 {
                format!("dup-{nanos}")
            } else {
                format!("dup-{nanos}-{}", counter - 1)
            };
            format!("{base_id}-{suffix}")
        };
        let trace_dir = base_dir.join(&candidate);
        match fs::create_dir(&trace_dir) {
            Ok(()) => {
                if counter > 0 {
                    warn!(
                        "trace_id collision for {}; using {} instead",
                        raw_trace_id, candidate
                    );
                }
                return Ok((candidate, trace_dir));
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                counter = counter.saturating_add(1);
                continue;
            }
            Err(err) => return Err(err.into()),
        }
    }
}
