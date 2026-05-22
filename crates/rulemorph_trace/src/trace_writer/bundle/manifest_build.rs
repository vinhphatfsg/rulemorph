use serde_json::Value as JsonValue;

use super::super::manifest::{parse_rule_meta, parse_summary};
use super::super::options::TraceWriteOptions;
use super::TRACE_SCHEMA_VERSION;
use crate::trace_schema::{TraceChunkRef, TraceDetailRef, TraceManifest, TraceMasking};

pub(super) fn build_trace_manifest(
    trace: &JsonValue,
    trace_id: String,
    timestamp: String,
    detail_layout: String,
    detail_status: String,
    mut detail_reason: Vec<String>,
    record_chunks: Vec<TraceChunkRef>,
    node_chunks: Vec<TraceChunkRef>,
    finalize_chunk: Option<TraceChunkRef>,
    masking: Option<TraceMasking>,
    options: &TraceWriteOptions,
) -> (TraceManifest, TraceDetailRef) {
    let summary = trace.get("summary").map(parse_summary);
    let rule = trace.get("rule").map(parse_rule_meta);
    let status = trace
        .get("status")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());
    let input_format = trace
        .get("input_format")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());
    let mut rule_source = trace.get("rule_source").cloned();
    if let Some(rule_source_value) = rule_source.as_ref() {
        let rule_source_bytes = serde_json::to_vec(rule_source_value)
            .map(|payload| payload.len() as u64)
            .unwrap_or(0);
        if rule_source_bytes > options.max_bytes_per_trace as u64 {
            rule_source = None;
            if !detail_reason
                .iter()
                .any(|reason| reason == "rule_source_dropped")
            {
                detail_reason.push("rule_source_dropped".to_string());
            }
        }
    }

    let detail = TraceDetailRef {
        layout: detail_layout,
        status: detail_status.clone(),
        reason: detail_reason,
        records: record_chunks,
        nodes: node_chunks,
        finalize: finalize_chunk,
    };

    let manifest = TraceManifest {
        trace_schema_version: TRACE_SCHEMA_VERSION,
        trace_id,
        timestamp: Some(timestamp),
        status,
        rule,
        input_format,
        summary,
        max_chunk_bytes_uncompressed: Some(options.max_chunk_bytes_uncompressed as u64),
        detail: Some(detail.clone()),
        masking,
        rule_source,
    };

    (manifest, detail)
}
