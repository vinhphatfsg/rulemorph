#[tokio::test]
async fn trace_store_downgrades_on_node_count_limit() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();

    let trace_dir = data_dir.join("traces/2026/01/04/trace-node-limit");
    fs::create_dir_all(&trace_dir)?;

    let node_count = TRACE_NODE_COUNT_HARD_MAX + 1;
    let mut lines = String::new();
    for _ in 0..node_count {
        lines.push_str("{\"record_index\":0,\"node\":0}\n");
    }
    fs::write(trace_dir.join("nodes-0001.ndjson"), lines)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-node-limit",
            "status": "ok",
            "max_chunk_bytes_uncompressed": 16 * 1024 * 1024,
            "detail": {
                "layout": "records_nodes_split",
                "status": "full",
                "records": [],
                "nodes": [
                    {
                        "path": "nodes-0001.ndjson",
                        "format": "ndjson",
                        "compression": "none"
                    }
                ]
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-node-limit")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "budget_exceeded");

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_inline_node_count_limit() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();

    let trace_dir = data_dir.join("traces/2026/01/04/trace-inline-node-limit");
    fs::create_dir_all(&trace_dir)?;

    let node_count = TRACE_NODE_COUNT_HARD_MAX + 1;
    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        nodes.push(json!({}));
    }
    let record = json!({
        "index": 0,
        "status": "ok",
        "nodes": nodes
    });
    let line = serde_json::to_string(&record)?;
    fs::write(trace_dir.join("records-0001.ndjson"), format!("{line}\n"))?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-inline-node-limit",
            "status": "ok",
            "max_chunk_bytes_uncompressed": 16 * 1024 * 1024,
            "detail": {
                "layout": "records_nodes_split",
                "status": "full",
                "records": [
                    {
                        "path": "records-0001.ndjson",
                        "format": "ndjson",
                        "compression": "none"
                    }
                ],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-inline-node-limit")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "budget_exceeded");

    Ok(())
}
