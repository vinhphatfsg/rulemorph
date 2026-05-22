#[test]
fn legacy_trace_downgrades_on_record_limit() -> Result<()> {
    let mut legacy = json!({
        "trace_id": "legacy-over-limit",
        "records": [
            { "index": 0, "status": "ok" },
            { "index": 1, "status": "ok" }
        ]
    });

    apply_legacy_limits_with_thresholds(&mut legacy, 1, 10);

    let detail = legacy
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("budget_exceeded"))
    );
    let records = legacy
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(records.is_empty());

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_detail_on_oversized_chunk() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/07/trace-oversized");
    fs::create_dir_all(&trace_dir)?;

    let oversized = TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX + 1024;
    let payload = format!(
        "{{\"index\":0,\"payload\":\"{}\"}}\n",
        "x".repeat(oversized)
    );
    let compressed = zstd::stream::encode_all(payload.as_bytes(), 3)?;
    fs::write(trace_dir.join("records-0001.ndjson.zst"), compressed)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-oversized",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "records": [
                    {
                        "path": "records-0001.ndjson.zst",
                        "format": "ndjson",
                        "compression": "zstd",
                        "bytes": 1024,
                        "record_start": 0,
                        "record_end": 0
                    }
                ],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-oversized")
        .await?
        .expect("trace should exist");
    let detail = trace
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("chunk_error"))
    );
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("chunk_too_large"))
    );
    let detail_records = detail
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_records.is_empty());
    let detail_nodes = detail
        .get("nodes")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_nodes.is_empty());
    assert!(detail.get("finalize").is_none());
    let records = trace
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(records.is_empty());
    assert!(trace.get("finalize").is_none());

    Ok(())
}

#[test]
fn trace_store_downgrades_on_total_bytes_budget() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("trace-budget");
    fs::create_dir_all(&trace_dir)?;

    let payload = format!("{{\"index\":0,\"payload\":\"{}\"}}\n", "x".repeat(64));
    let payload_len = payload.as_bytes().len();
    fs::write(trace_dir.join("records-0001.ndjson"), payload.as_bytes())?;
    fs::write(trace_dir.join("records-0002.ndjson"), payload.as_bytes())?;

    let max_chunk_bytes = payload_len + 8;
    let manifest = TraceManifest {
        trace_schema_version: 1,
        trace_id: "trace-budget".to_string(),
        timestamp: None,
        status: Some("ok".to_string()),
        rule: None,
        input_format: None,
        summary: None,
        max_chunk_bytes_uncompressed: Some(max_chunk_bytes as u64),
        detail: Some(TraceDetailRef {
            layout: "records_inline".to_string(),
            status: "full".to_string(),
            reason: Vec::new(),
            records: vec![
                TraceChunkRef {
                    path: "records-0001.ndjson".to_string(),
                    format: "ndjson".to_string(),
                    compression: "none".to_string(),
                    record_start: None,
                    record_end: None,
                    node_start: None,
                    node_end: None,
                    bytes: None,
                    bytes_uncompressed: None,
                },
                TraceChunkRef {
                    path: "records-0002.ndjson".to_string(),
                    format: "ndjson".to_string(),
                    compression: "none".to_string(),
                    record_start: None,
                    record_end: None,
                    node_start: None,
                    node_end: None,
                    bytes: None,
                    bytes_uncompressed: None,
                },
            ],
            nodes: Vec::new(),
            finalize: None,
        }),
        masking: None,
        rule_source: None,
    };

    let budget = ChunkBudget {
        remaining_bytes: payload_len + 1,
        remaining_chunks: 10,
    };
    let trace = build_trace_from_manifest_with_budget(&manifest, &trace_dir, budget)?;
    let detail = trace
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("budget_exceeded"))
    );
    let detail_records = detail
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_records.is_empty());

    Ok(())
}
