#[tokio::test]
async fn write_trace_bundle_downgrades_on_budget_exceeded() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-test",
        "records": [
            { "id": 1, "payload": "x".repeat(1024) }
        ],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        max_bytes_per_trace: 1,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );
    assert!(detail.records.is_empty());
    assert!(detail.finalize.is_none());

    assert_no_detail_artifacts(&manifest_path)?;

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_clamps_max_chunk_bytes_uncompressed() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-clamp",
        "records": [
            { "index": 0, "status": "ok", "value": 1 }
        ]
    });

    let options = TraceWriteOptions {
        max_chunk_bytes_uncompressed: 64 * 1024 * 1024,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    assert_eq!(
        manifest.max_chunk_bytes_uncompressed,
        Some(16 * 1024 * 1024)
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_drops_rule_source_when_manifest_too_large() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-large-manifest",
        "records": [],
        "rule_source": {
            "raw": "x".repeat(TRACE_JSON_MAX_BYTES as usize)
        }
    });

    let options = TraceWriteOptions {
        max_bytes_per_trace: TRACE_JSON_MAX_BYTES as usize + 1024,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest_payload = read_manifest_payload(&manifest_path)?;
    assert!(manifest_payload.len() as u64 <= TRACE_JSON_MAX_BYTES);
    let manifest: TraceManifest = serde_json::from_str(&manifest_payload)?;
    let detail = manifest.detail.expect("detail should exist");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "rule_source_dropped")
    );
    assert!(manifest.rule_source.is_none());

    Ok(())
}

include!("budget_sampling/chunk_limits.rs");
include!("budget_sampling/sampling.rs");
