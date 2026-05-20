use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::Value as JsonValue;

use super::chunk_write::{
    max_ndjson_line_bytes, write_finalize_chunk, write_node_chunks, write_record_chunks,
};
use super::cleanup::TraceDirGuard;
use super::detail::{detail_status_for_level, initial_detail_reasons, strip_trace_detail};
use super::externalize::externalize_trace_payloads;
use super::manifest::count_inline_nodes;
use super::masking::{apply_masking, normalize_masking_rules};
use super::options::{TraceDetailLevel, TraceWriteOptions, clamp_max_chunk_bytes_uncompressed};
use super::record_nodes::{normalize_inline_records, split_records_and_nodes};
use super::sampling::should_keep_full_detail;
use super::trace_dir::{
    ensure_unique_trace_dir, resolve_trace_timestamp, trace_dir_base_for_timestamp,
};
use super::trace_identity::resolve_trace_id;
use crate::trace_schema::{
    TRACE_CHUNK_COUNT_HARD_MAX, TRACE_NODE_COUNT_HARD_MAX, TRACE_RECORD_COUNT_HARD_MAX,
    TraceMasking,
};

mod detail_cleanup;
mod detail_files;
mod manifest_build;
mod manifest_payload;
use self::detail_cleanup::{add_detail_reason, reset_detail_to_basic, total_detail_bytes};
use self::detail_files::ensure_detail_files_exist;
use self::manifest_build::build_trace_manifest;
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
                let (records_for_chunks, nodes_for_chunks) = if options.split_nodes {
                    detail_layout = "records_nodes_split".to_string();
                    split_records_and_nodes(&records)
                } else {
                    (normalize_inline_records(&records), Vec::new())
                };

                let total_records = records_for_chunks.len();
                let total_nodes = if options.split_nodes {
                    nodes_for_chunks.len()
                } else {
                    count_inline_nodes(&records_for_chunks, TRACE_NODE_COUNT_HARD_MAX)
                };
                if total_records > TRACE_RECORD_COUNT_HARD_MAX
                    || total_nodes > TRACE_NODE_COUNT_HARD_MAX
                {
                    budget_exceeded = true;
                }

                if !budget_exceeded {
                    let max_record_line = max_ndjson_line_bytes(&records_for_chunks, "record")?;
                    let max_node_line = max_ndjson_line_bytes(&nodes_for_chunks, "node")?;
                    let max_line = max_record_line.max(max_node_line);
                    if max_line > options.max_chunk_bytes_uncompressed {
                        detail_status = "basic".to_string();
                        detail_layout = "records_inline".to_string();
                        if !detail_reason
                            .iter()
                            .any(|reason| reason == "chunk_too_large")
                        {
                            detail_reason.push("chunk_too_large".to_string());
                        }
                        chunk_too_large = true;
                    } else {
                        let record_result = write_record_chunks(
                            &trace_dir,
                            &records_for_chunks,
                            &options,
                            &mut budget_remaining,
                            &mut chunk_budget_remaining,
                        )?;
                        record_chunks = record_result.chunks;
                        record_files = record_result.files;
                        budget_exceeded = record_result.budget_exceeded;

                        if !budget_exceeded && options.split_nodes {
                            let node_result = write_node_chunks(
                                &trace_dir,
                                &nodes_for_chunks,
                                &options,
                                &mut budget_remaining,
                                &mut chunk_budget_remaining,
                            )?;
                            node_chunks = node_result.chunks;
                            node_files = node_result.files;
                            budget_exceeded = node_result.budget_exceeded;
                        }

                        if !budget_exceeded {
                            let finalize_result = write_finalize_chunk(
                                &trace_dir,
                                &trace,
                                &options,
                                &mut budget_remaining,
                                &mut chunk_budget_remaining,
                            )?;
                            finalize_chunk = finalize_result.chunk;
                            finalize_file = finalize_result.file;
                            budget_exceeded = finalize_result.budget_exceeded;
                            if finalize_result.size_exceeded {
                                chunk_too_large = true;
                            }
                        }
                    }
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
        reset_detail_to_basic(
            &mut record_files,
            &mut node_files,
            &mut finalize_file,
            &mut blob_files,
            &mut record_chunks,
            &mut node_chunks,
            &mut finalize_chunk,
            &mut detail_status,
            &mut detail_layout,
        );
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
        reset_detail_to_basic(
            &mut record_files,
            &mut node_files,
            &mut finalize_file,
            &mut blob_files,
            &mut record_chunks,
            &mut node_chunks,
            &mut finalize_chunk,
            &mut detail_status,
            &mut detail_layout,
        );
        detail_reason.push("budget_exceeded".to_string());
    }

    let (mut manifest, mut detail) = build_trace_manifest(
        &trace,
        trace_id,
        timestamp,
        detail_layout,
        detail_status,
        detail_reason,
        record_chunks,
        node_chunks,
        finalize_chunk,
        masking,
        &options,
    );

    ensure_detail_files_exist(&detail.status, record_files, node_files, finalize_file)?;

    let manifest_path = trace_dir.join("trace.json");
    write_manifest_payload(&manifest_path, &mut manifest, &mut detail)?;
    trace_dir_guard.commit();

    Ok(manifest_path)
}
