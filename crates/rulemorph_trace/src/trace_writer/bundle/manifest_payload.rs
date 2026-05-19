use std::path::Path;

use anyhow::Result;

use super::super::atomic::write_atomic;
use crate::trace_schema::{TRACE_JSON_MAX_BYTES, TraceDetailRef, TraceManifest};

pub(super) fn write_manifest_payload(
    manifest_path: &Path,
    manifest: &mut TraceManifest,
    detail: &mut TraceDetailRef,
) -> Result<()> {
    let mut manifest_payload = serde_json::to_string_pretty(manifest)?;
    if manifest_payload.len() as u64 > TRACE_JSON_MAX_BYTES {
        if manifest.rule_source.is_some() {
            manifest.rule_source = None;
            if !detail
                .reason
                .iter()
                .any(|reason| reason == "rule_source_dropped")
            {
                detail.reason.push("rule_source_dropped".to_string());
            }
            manifest.detail = Some(detail.clone());
            manifest_payload = serde_json::to_string_pretty(manifest)?;
        }
        if manifest_payload.len() as u64 > TRACE_JSON_MAX_BYTES {
            return Err(anyhow::anyhow!(
                "trace json exceeds max bytes: {} > {}",
                manifest_payload.len(),
                TRACE_JSON_MAX_BYTES
            ));
        }
    }
    write_atomic(manifest_path, manifest_payload.as_bytes())
}
