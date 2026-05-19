#[tokio::test]
async fn import_bundle_ignores_blobs_for_metadata() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");
    let bundle_traces = bundle_dir.join("traces/2026/01/03/trace-bundle");
    fs::create_dir_all(&bundle_traces)?;

    fs::write(
        bundle_traces.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-bundle",
            "status": "ok",
            "timestamp": "2026-01-03T00:00:00Z"
        }))?,
    )?;

    let blob_dir = bundle_traces.join("blobs");
    fs::create_dir_all(&blob_dir)?;
    fs::write(
        blob_dir.join("sha256-cafe.json"),
        serde_json::to_vec(&json!({
            "trace_id": "trace-bundle-blob",
            "status": "ok",
            "records": [],
            "summary": { "record_total": 0 }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.clone()).await?;
    let result = store.import_bundle(&bundle_dir).await?;

    assert_eq!(result.imported, 1);
    assert_eq!(result.trace_ids, vec!["trace-bundle".to_string()]);

    Ok(())
}

#[tokio::test]
async fn import_bundle_returns_index_ids_for_sanitized_trace_id() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");
    let bundle_traces = bundle_dir.join("traces/2026/01/05/trace-blank");
    fs::create_dir_all(&bundle_traces)?;

    fs::write(
        bundle_traces.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "",
            "status": "ok",
            "timestamp": "2026-01-05T00:00:00Z"
        }))?,
    )?;

    let store = TraceStore::new(data_dir.clone()).await?;
    let result = store.import_bundle(&bundle_dir).await?;

    assert_eq!(result.imported, 1);
    assert_eq!(result.trace_ids.len(), 1);
    let trace = store.get(&result.trace_ids[0]).await?;
    assert!(trace.is_some());

    Ok(())
}

include!("import_bundle/rejections.rs");
