use crate::trace_schema::{
    TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TRACE_CHUNK_COUNT_HARD_MAX, TRACE_NODE_COUNT_HARD_MAX,
    TRACE_RECORD_COUNT_HARD_MAX, TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TraceChunkRef,
    TraceDetailRef, TraceManifest,
};

pub(super) fn apply_record_total_budget_for_get(manifest: &mut TraceManifest) {
    let record_total = manifest
        .summary
        .as_ref()
        .and_then(|summary| summary.record_total);
    let Some(record_total) = record_total else {
        return;
    };
    if record_total as usize <= TRACE_RECORD_COUNT_HARD_MAX {
        return;
    }
    let Some(detail) = manifest.detail.as_mut() else {
        return;
    };
    if detail.status != "full" {
        return;
    }

    detail.status = "basic".to_string();
    if !detail
        .reason
        .iter()
        .any(|reason| reason == "budget_exceeded")
    {
        detail.reason.push("budget_exceeded".to_string());
    }
    detail.records.clear();
    detail.nodes.clear();
    detail.finalize = None;
}

pub(super) fn resolve_max_chunk_bytes(manifest: &TraceManifest) -> usize {
    let requested = manifest
        .max_chunk_bytes_uncompressed
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX);
    requested.clamp(1, TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX)
}

fn estimate_chunk_item_count(
    chunks: &[TraceChunkRef],
    start: impl Fn(&TraceChunkRef) -> Option<u64>,
    end: impl Fn(&TraceChunkRef) -> Option<u64>,
) -> Option<u64> {
    let mut total = 0u64;
    for chunk in chunks {
        let start = start(chunk)?;
        let end = end(chunk)?;
        if end < start {
            return None;
        }
        total = total.checked_add(end - start + 1)?;
    }
    Some(total)
}

fn estimate_uncompressed_bytes(
    detail: &TraceDetailRef,
    max_chunk_bytes: Option<u64>,
) -> Option<u64> {
    let mut total = 0u64;
    let mut add_chunk = |chunk: &TraceChunkRef| -> Option<()> {
        let bytes = match chunk.bytes_uncompressed {
            Some(bytes) => bytes,
            None if chunk.compression == "none" => chunk
                .bytes
                .or(max_chunk_bytes)
                .unwrap_or(TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX as u64),
            None => {
                if let Some(max_chunk_bytes) = max_chunk_bytes {
                    max_chunk_bytes
                } else {
                    chunk.bytes?
                }
            }
        };
        total = total.checked_add(bytes)?;
        Some(())
    };
    for chunk in detail.records.iter().chain(detail.nodes.iter()) {
        add_chunk(chunk)?;
    }
    if let Some(chunk) = detail.finalize.as_ref() {
        add_chunk(chunk)?;
    }
    Some(total)
}

pub(super) fn apply_manifest_budget(manifest: &mut TraceManifest) {
    let summary_record_total = manifest
        .summary
        .as_ref()
        .and_then(|summary| summary.record_total);
    let Some(detail) = manifest.detail.as_mut() else {
        return;
    };
    if detail.status != "full" {
        return;
    }

    let mut budget_exceeded = false;

    let chunk_count =
        detail.records.len() + detail.nodes.len() + usize::from(detail.finalize.is_some());
    if chunk_count > TRACE_CHUNK_COUNT_HARD_MAX {
        budget_exceeded = true;
    }

    let max_chunk_bytes = manifest
        .max_chunk_bytes_uncompressed
        .and_then(|value| usize::try_from(value).ok())
        .map(|value| value.clamp(1, TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX) as u64);

    match estimate_uncompressed_bytes(detail, max_chunk_bytes) {
        Some(estimated_bytes) => {
            if estimated_bytes > TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX as u64 {
                budget_exceeded = true;
            }
        }
        None => {
            budget_exceeded = true;
        }
    }

    let record_total = if detail.records.is_empty() {
        summary_record_total
    } else {
        match estimate_chunk_item_count(
            &detail.records,
            |chunk| chunk.record_start,
            |chunk| chunk.record_end,
        ) {
            Some(record_total) => Some(record_total),
            None => summary_record_total,
        }
    };
    if let Some(record_total) = record_total {
        if record_total as usize > TRACE_RECORD_COUNT_HARD_MAX {
            budget_exceeded = true;
        }
    } else if !detail.records.is_empty() {
        budget_exceeded = true;
    }

    let node_total = if detail.nodes.is_empty() {
        if detail.layout == "records_inline" {
            record_total
        } else {
            None
        }
    } else {
        estimate_chunk_item_count(
            &detail.nodes,
            |chunk| chunk.node_start,
            |chunk| chunk.node_end,
        )
    };
    if let Some(node_total) = node_total {
        if node_total as usize > TRACE_NODE_COUNT_HARD_MAX {
            budget_exceeded = true;
        }
    } else if !detail.nodes.is_empty() {
        budget_exceeded = true;
    }

    if budget_exceeded {
        detail.status = "basic".to_string();
        if !detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
        {
            detail.reason.push("budget_exceeded".to_string());
        }
        detail.records.clear();
        detail.nodes.clear();
        detail.finalize = None;
    }
}
