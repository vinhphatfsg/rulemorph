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

#[tokio::test]
async fn trace_store_accepts_compressed_chunk_overhead() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/02/trace-compressed-overhead");
    fs::create_dir_all(&trace_dir)?;

    let payload = "0\n";
    let compressed = zstd::stream::encode_all(payload.as_bytes(), 3)?;
    assert!(compressed.len() > payload.len());
    let max_chunk_bytes_uncompressed = compressed.len().saturating_sub(1);
    assert!(max_chunk_bytes_uncompressed >= payload.len());
    fs::write(trace_dir.join("records-0001.ndjson.zst"), compressed)?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-compressed-overhead",
        "records-0001.ndjson.zst",
        "ndjson",
        "zstd",
        Some(max_chunk_bytes_uncompressed),
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-compressed-overhead")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "full");
    let records = trace
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(records.len(), 1);

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_detail_on_oversized_uncompressed_chunk() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/02/trace-oversized-none");
    fs::create_dir_all(&trace_dir)?;

    let payload = format!("{{\"index\":0,\"payload\":\"{}\"}}\n", "x".repeat(64));
    fs::write(trace_dir.join("records-0001.ndjson"), payload)?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-oversized-none",
        "records-0001.ndjson",
        "ndjson",
        "none",
        Some(32),
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-oversized-none")
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

#[tokio::test]
async fn trace_store_downgrades_on_oversized_compressed_chunk() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/03/trace-compressed-oversize");
    fs::create_dir_all(&trace_dir)?;

    let oversized = TRACE_CHUNK_BYTES_COMPRESSED_OVERHEAD_MAX + 2;
    fs::write(
        trace_dir.join("records-0001.ndjson.zst"),
        vec![0u8; oversized],
    )?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-compressed-oversize",
        "records-0001.ndjson.zst",
        "ndjson",
        "zstd",
        Some(1),
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-compressed-oversize")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_too_large");

    Ok(())
}

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
