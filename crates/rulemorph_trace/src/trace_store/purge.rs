use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::warn;

use super::{PurgeFailure, PurgeReport, TraceMeta};

const MAX_TRACE_FUTURE_SKEW: Duration = Duration::from_secs(300);

pub(super) async fn purge_trace_metas(
    traces: Vec<TraceMeta>,
    traces_dir: &Path,
    retention: Duration,
    dry_run: bool,
) -> PurgeReport {
    let cutoff = SystemTime::now()
        .checked_sub(retention)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let traces_root = if dry_run {
        None
    } else {
        Some(
            traces_dir
                .canonicalize()
                .unwrap_or_else(|_| traces_dir.to_path_buf()),
        )
    };
    let mut purged = Vec::new();
    let mut failed = Vec::new();

    for meta in traces {
        let path = PathBuf::from(&meta.path);
        let Some(timestamp) = resolve_trace_timestamp(&meta, &path).await else {
            continue;
        };
        if timestamp > cutoff {
            continue;
        }
        if dry_run {
            purged.push(meta);
            continue;
        }
        if let Err(err) = delete_trace_path(
            &path,
            traces_root
                .as_deref()
                .expect("traces root is available when not dry-run"),
        )
        .await
        {
            failed.push(PurgeFailure {
                trace_id: meta.trace_id.clone(),
                path: meta.path.clone(),
                error: err.to_string(),
            });
            warn!(
                "failed to purge trace {} at {}: {}",
                meta.trace_id, meta.path, err
            );
            continue;
        }
        purged.push(meta);
    }

    PurgeReport { purged, failed }
}

pub(super) async fn resolve_trace_timestamp(meta: &TraceMeta, path: &Path) -> Option<SystemTime> {
    if let Some(timestamp) = meta.timestamp.as_deref() {
        if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(timestamp) {
            let utc = parsed.with_timezone(&Utc);
            let parsed_time = SystemTime::from(utc);
            let now = SystemTime::now();
            if parsed_time <= now {
                return Some(parsed_time);
            }
            if let Ok(delta) = parsed_time.duration_since(now) {
                if delta <= MAX_TRACE_FUTURE_SKEW {
                    return Some(parsed_time);
                }
            }
        }
    }
    let metadata = tokio::fs::metadata(path).await.ok()?;
    metadata.modified().ok()
}

async fn delete_trace_path(path: &Path, traces_root: &Path) -> Result<()> {
    let candidate = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    if !candidate.starts_with(traces_root) {
        return Err(anyhow::anyhow!(
            "trace path escapes traces dir: {}",
            candidate.display()
        ));
    }
    let file_name = candidate.file_name().and_then(|name| name.to_str());
    let parent = candidate.parent().unwrap_or(traces_root);
    if file_name == Some("trace.json") && parent != traces_root {
        tokio::fs::remove_dir_all(parent)
            .await
            .with_context(|| format!("failed to remove trace dir {}", parent.display()))?;
    } else {
        tokio::fs::remove_file(&candidate)
            .await
            .with_context(|| format!("failed to remove trace file {}", candidate.display()))?;
    }
    Ok(())
}
