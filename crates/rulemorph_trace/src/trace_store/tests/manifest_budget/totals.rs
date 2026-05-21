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
