#[tokio::test]
async fn trace_store_downgrades_detail_on_missing_chunk() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/02/trace-missing");
    fs::create_dir_all(&trace_dir)?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-missing",
        "records-0001.ndjson",
        "ndjson",
        "none",
        None,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-missing")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");
    assert_detail_array_empty(&trace, "records");
    assert_detail_array_empty(&trace, "nodes");
    assert_top_level_array_empty(&trace, "records");
    assert_finalize_absent(&trace);

    Ok(())
}

include!("chunk_downgrade/chunk_size.rs");

include!("chunk_downgrade/errors.rs");

#[tokio::test]
async fn trace_store_downgrades_detail_on_chunk_budget_exceeded() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/02/trace-chunk-budget");
    fs::create_dir_all(&trace_dir)?;

    let max_chunks = 128usize;
    let mut chunks = Vec::new();
    for index in 0..=max_chunks {
        let filename = format!("records-{index:04}.ndjson");
        fs::write(trace_dir.join(&filename), "{\"index\":0}\n")?;
        chunks.push(json!({
            "path": filename,
            "format": "ndjson",
            "compression": "none"
        }));
    }

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-chunk-budget",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "records": chunks,
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-chunk-budget")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");
    assert_detail_reason(&trace, "budget_exceeded");
    assert_detail_array_empty(&trace, "records");
    assert_detail_array_empty(&trace, "nodes");
    assert_top_level_array_empty(&trace, "records");

    Ok(())
}
