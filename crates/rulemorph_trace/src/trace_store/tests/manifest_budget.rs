#[tokio::test]
async fn manifest_downgrades_when_chunk_budget_exceeded() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("traces/2026/02/03/trace-over-budget");
    fs::create_dir_all(&trace_dir)?;

    let mut records = Vec::new();
    for index in 0..(TRACE_CHUNK_COUNT_HARD_MAX + 1) {
        records.push(json!({
            "path": format!("records-{index:04}.ndjson"),
            "format": "ndjson",
            "compression": "none",
            "record_start": 0,
            "record_end": 0
        }));
    }

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-over-budget",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "reason": [],
                "records": records,
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(temp.path().to_path_buf()).await?;
    let manifest = store
        .get_manifest("trace-over-budget")
        .await?
        .expect("manifest");
    let detail = manifest.detail.expect("detail");
    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );
    assert!(detail.records.is_empty());

    Ok(())
}
#[tokio::test]
async fn manifest_downgrades_when_total_bytes_exceeded() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("traces/2026/02/03/trace-bytes-budget");
    fs::create_dir_all(&trace_dir)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-bytes-budget",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "reason": [],
                "records": [
                    {
                        "path": "records-0001.ndjson",
                        "format": "ndjson",
                        "compression": "none",
                        "record_start": 0,
                        "record_end": 0,
                        "bytes": TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX as u64 + 1
                    }
                ],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(temp.path().to_path_buf()).await?;
    let manifest = store
        .get_manifest("trace-bytes-budget")
        .await?
        .expect("manifest");
    let detail = manifest.detail.expect("detail");
    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );

    Ok(())
}

#[tokio::test]
async fn manifest_downgrades_when_unknown_uncompressed_bytes_exceed_budget() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("traces/2026/02/03/trace-unknown-bytes");
    fs::create_dir_all(&trace_dir)?;

    let unknown_chunks =
        TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX / TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX + 1;
    let records: Vec<Value> = (0..unknown_chunks)
        .map(|index| {
            json!({
                "path": format!("records-{index:04}.ndjson.zst"),
                "format": "ndjson",
                "compression": "zstd",
                "record_start": index as u64,
                "record_end": index as u64,
                "bytes": 1024
            })
        })
        .collect();

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-unknown-bytes",
            "status": "ok",
            "max_chunk_bytes_uncompressed": TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX as u64,
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "reason": [],
                "records": records,
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(temp.path().to_path_buf()).await?;
    let manifest = store
        .get_manifest("trace-unknown-bytes")
        .await?
        .expect("manifest");
    let detail = manifest.detail.expect("detail");
    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );

    Ok(())
}

#[tokio::test]
async fn manifest_downgrades_when_record_total_exceeded() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("traces/2026/02/03/trace-record-budget");
    fs::create_dir_all(&trace_dir)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-record-budget",
            "status": "ok",
            "summary": {
                "record_total": TRACE_RECORD_COUNT_HARD_MAX as u64 + 1
            },
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "reason": [],
                "records": [],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(temp.path().to_path_buf()).await?;
    let manifest = store
        .get_manifest("trace-record-budget")
        .await?
        .expect("manifest");
    let detail = manifest.detail.expect("detail");
    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );

    let trace = store.get("trace-record-budget").await?.expect("trace");
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

    Ok(())
}

#[tokio::test]
async fn manifest_downgrades_when_node_total_exceeded() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("traces/2026/02/03/trace-node-budget");
    fs::create_dir_all(&trace_dir)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-node-budget",
            "status": "ok",
            "detail": {
                "layout": "records_nodes_split",
                "status": "full",
                "reason": [],
                "records": [],
                "nodes": [
                    {
                        "path": "nodes-0001.ndjson",
                        "format": "ndjson",
                        "compression": "none",
                        "node_start": 0,
                        "node_end": TRACE_NODE_COUNT_HARD_MAX as u64
                    }
                ]
            }
        }))?,
    )?;

    let store = TraceStore::new(temp.path().to_path_buf()).await?;
    let manifest = store
        .get_manifest("trace-node-budget")
        .await?
        .expect("manifest");
    let detail = manifest.detail.expect("detail");
    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );

    Ok(())
}
