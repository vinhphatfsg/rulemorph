use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::Value as JsonValue;

use super::cleanup::TraceDirGuard;
use super::detail::{detail_status_for_level, initial_detail_reasons, strip_trace_detail};
use super::externalize::externalize_trace_payloads;
use super::masking::{apply_masking, normalize_masking_rules};
use super::options::{TraceDetailLevel, TraceWriteOptions, clamp_max_chunk_bytes_uncompressed};
use super::sampling::should_keep_full_detail;
use super::trace_dir::{
    ensure_unique_trace_dir, resolve_trace_timestamp, trace_dir_base_for_timestamp,
};
use super::trace_identity::resolve_trace_id;
use crate::trace_schema::{TRACE_CHUNK_COUNT_HARD_MAX, TraceMasking};

mod detail_chunks;
mod detail_cleanup;
mod detail_files;
mod manifest_build;
mod manifest_payload;
use self::detail_chunks::write_full_detail_chunks;
use self::detail_cleanup::{
    DetailReset, add_detail_reason, reset_detail_to_basic, total_detail_bytes,
};
use self::detail_files::ensure_detail_files_exist;
use self::manifest_build::{TraceManifestInput, build_trace_manifest};
use self::manifest_payload::write_manifest_payload;

const TRACE_SCHEMA_VERSION: u8 = 1;

pub async fn write_trace_bundle(
    data_dir: &Path,
    trace: &JsonValue,
    options: Option<TraceWriteOptions>,
) -> Result<PathBuf> {
    let data_dir = data_dir.to_path_buf();
    let trace = trace.clone();
    let options = options.unwrap_or_default();
    tokio::task::spawn_blocking(move || write_trace_bundle_sync(&data_dir, &trace, &options))
        .await?
}

pub(crate) fn write_trace_bundle_sync(
    data_dir: &Path,
    trace: &JsonValue,
    options: &TraceWriteOptions,
) -> Result<PathBuf> {
    let mut trace = trace.clone();
    let mut options = options.clone();
    options.max_chunk_bytes_uncompressed =
        clamp_max_chunk_bytes_uncompressed(options.max_chunk_bytes_uncompressed);
    let resolved_trace_id = resolve_trace_id(&trace);
    let timestamp = resolve_trace_timestamp(&trace);
    let trace_dir_base = trace_dir_base_for_timestamp(data_dir, &timestamp);
    let (trace_id, trace_dir) = ensure_unique_trace_dir(
        &trace_dir_base,
        resolved_trace_id.trace_id,
        &resolved_trace_id.raw_trace_id,
    )?;
    if let Some(obj) = trace.as_object_mut() {
        obj.insert("trace_id".to_string(), JsonValue::String(trace_id.clone()));
    }
    let mut trace_dir_guard = TraceDirGuard::new(trace_dir.clone());

    let records_raw = trace
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    let mut record_chunks = Vec::new();
    let mut record_files = Vec::new();
    let mut node_chunks = Vec::new();
    let mut node_files = Vec::new();
    let mut finalize_chunk = None;
    let mut finalize_file = None;
    let mut detail_layout = "records_inline".to_string();

    let mut detail_level = options.detail_level;
    let mut detail_reason = initial_detail_reasons(options.detail_reason.as_deref());

    if detail_level == TraceDetailLevel::Full
        && !should_keep_full_detail(&trace, &records_raw, &options)
    {
        detail_level = TraceDetailLevel::Basic;
        detail_reason.push("sampled_out".to_string());
    }

    let mut detail_status = detail_status_for_level(detail_level);

    let masking_rules = if options.masking_enabled {
        normalize_masking_rules(&options.masking_rules)
    } else {
        Vec::new()
    };
    let masking = if options.masking_enabled {
        Some(TraceMasking {
            enabled: true,
            rules: masking_rules.clone(),
        })
    } else {
        None
    };
    if detail_level != TraceDetailLevel::Full {
        strip_trace_detail(&mut trace);
    }
    if options.masking_enabled {
        apply_masking(&mut trace, &masking_rules);
    }

    let mut blob_files: Vec<PathBuf> = Vec::new();
    let mut budget_remaining = options.max_bytes_per_trace as u64;
    let mut chunk_budget_remaining = TRACE_CHUNK_COUNT_HARD_MAX;
    let mut budget_exceeded = false;
    let mut chunk_too_large = false;
    if detail_level == TraceDetailLevel::Full {
        budget_exceeded = externalize_trace_payloads(
            &mut trace,
            &trace_dir,
            &options,
            &mut budget_remaining,
            &mut blob_files,
        )?;
    }

    match detail_level {
        TraceDetailLevel::Full => {
            if !budget_exceeded {
                let records = trace
                    .get("records")
                    .and_then(|value| value.as_array())
                    .cloned()
                    .unwrap_or_default();
                let detail_result = write_full_detail_chunks(
                    &trace_dir,
                    &trace,
                    &records,
                    &options,
                    &mut budget_remaining,
                    &mut chunk_budget_remaining,
                )?;
                record_chunks = detail_result.record_chunks;
                record_files = detail_result.record_files;
                node_chunks = detail_result.node_chunks;
                node_files = detail_result.node_files;
                finalize_chunk = detail_result.finalize_chunk;
                finalize_file = detail_result.finalize_file;
                detail_layout = detail_result.detail_layout;
                budget_exceeded = detail_result.budget_exceeded;
                chunk_too_large = detail_result.chunk_too_large;
                if chunk_too_large {
                    detail_status = "basic".to_string();
                    add_detail_reason(&mut detail_reason, "chunk_too_large");
                }
            }
        }
        TraceDetailLevel::Basic => {
            if detail_reason.is_empty() {
                detail_reason.push("trace_level_basic".to_string());
            }
        }
        TraceDetailLevel::Off => {
            detail_reason.push("trace_level_off".to_string());
        }
    }

    if budget_exceeded || chunk_too_large {
        reset_detail_to_basic(DetailReset {
            record_files: &mut record_files,
            node_files: &mut node_files,
            finalize_file: &mut finalize_file,
            blob_files: &mut blob_files,
            record_chunks: &mut record_chunks,
            node_chunks: &mut node_chunks,
            finalize_chunk: &mut finalize_chunk,
            detail_status: &mut detail_status,
            detail_layout: &mut detail_layout,
        });
        if budget_exceeded {
            add_detail_reason(&mut detail_reason, "budget_exceeded");
        }
        if chunk_too_large {
            add_detail_reason(&mut detail_reason, "chunk_too_large");
        }
    }

    let detail_bytes =
        total_detail_bytes(&record_chunks, &node_chunks, &finalize_chunk, &blob_files);
    if detail_status == "full" && detail_bytes > options.max_bytes_per_trace as u64 {
        reset_detail_to_basic(DetailReset {
            record_files: &mut record_files,
            node_files: &mut node_files,
            finalize_file: &mut finalize_file,
            blob_files: &mut blob_files,
            record_chunks: &mut record_chunks,
            node_chunks: &mut node_chunks,
            finalize_chunk: &mut finalize_chunk,
            detail_status: &mut detail_status,
            detail_layout: &mut detail_layout,
        });
        detail_reason.push("budget_exceeded".to_string());
    }

    let (mut manifest, mut detail) = build_trace_manifest(
        &trace,
        TraceManifestInput {
            trace_id,
            timestamp,
            detail_layout,
            detail_status,
            detail_reason,
            record_chunks,
            node_chunks,
            finalize_chunk,
            masking,
        },
        &options,
    );

    ensure_detail_files_exist(&detail.status, record_files, node_files, finalize_file)?;

    let manifest_path = trace_dir.join("trace.json");
    write_manifest_payload(&manifest_path, &mut manifest, &mut detail)?;
    trace_dir_guard.commit();

    Ok(manifest_path)
}
