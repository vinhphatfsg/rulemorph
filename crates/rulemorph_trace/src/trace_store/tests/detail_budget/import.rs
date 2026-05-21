#[tokio::test]
async fn import_bundle_rejects_total_bytes_limit() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");
    let bundle_traces = bundle_dir.join("traces/2026/01/10/trace-total-limit");
    fs::create_dir_all(&bundle_traces)?;

    let trace_payload = serde_json::to_vec(&json!({
        "trace_schema_version": 1,
        "trace_id": "trace-total-limit",
        "status": "ok",
        "timestamp": "2026-01-10T00:00:00Z"
    }))?;
    fs::write(bundle_traces.join("trace.json"), &trace_payload)?;
    let records_payload = vec![b'x'; 64];
    fs::write(bundle_traces.join("records-0001.ndjson"), &records_payload)?;

    let max_total_bytes =
        (trace_payload.len().saturating_add(records_payload.len()) as u64).saturating_sub(1);
    let store = FileTraceBackend::new(data_dir).await?;
    let err = store
        .import_bundle_with_limit(&bundle_dir, max_total_bytes)
        .await
        .expect_err("total bytes should be rejected");
    assert!(err.to_string().contains("max total bytes"));

    Ok(())
}
