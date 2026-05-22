#[tokio::test]
async fn trace_store_downgrades_on_record_count_limit() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();

    let trace_dir = data_dir.join("traces/2026/01/04/trace-record-limit");
    fs::create_dir_all(&trace_dir)?;

    let record_count = TRACE_RECORD_COUNT_HARD_MAX + 1;
    let mut lines = String::new();
    for index in 0..record_count {
        lines.push_str(&format!("{{\"index\":{index}}}\n"));
    }
    fs::write(trace_dir.join("records-0001.ndjson"), lines)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-record-limit",
            "status": "ok",
            "max_chunk_bytes_uncompressed": 4 * 1024 * 1024,
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
        .get("trace-record-limit")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "budget_exceeded");
    assert_detail_array_empty(&trace, "records");

    Ok(())
}

include!("count_limits/nodes.rs");
