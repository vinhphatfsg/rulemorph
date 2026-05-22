#[tokio::test]
async fn import_bundle_rejects_oversized_file() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");
    let bundle_traces = bundle_dir.join("traces/2026/01/09/trace-oversized");
    fs::create_dir_all(&bundle_traces)?;

    fs::write(
        bundle_traces.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-oversized",
            "status": "ok",
            "timestamp": "2026-01-09T00:00:00Z"
        }))?,
    )?;

    let oversized_bytes = (20 * 1024 * 1024) + 1;
    let payload = vec![b'x'; oversized_bytes];
    fs::write(bundle_traces.join("records-0001.ndjson"), payload)?;

    let store = TraceStore::new(data_dir).await?;
    let err = store
        .import_bundle(&bundle_dir)
        .await
        .expect_err("oversized file should be rejected");
    assert!(err.to_string().contains("max bytes"));

    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn import_bundle_rejects_symlinks() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");
    let traces_dir = bundle_dir.join("traces/2026/01/04");
    fs::create_dir_all(&traces_dir)?;

    let target = bundle_dir.join("payload.json");
    fs::write(&target, r#"{"trace_id":"trace-symlink"}"#)?;
    symlink(&target, traces_dir.join("link.json"))?;

    let store = TraceStore::new(data_dir).await?;
    let err = store
        .import_bundle(&bundle_dir)
        .await
        .expect_err("symlink should be rejected");
    assert!(err.to_string().contains("symlink"));

    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn import_bundle_rejects_destination_symlink() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");
    let bundle_traces = bundle_dir.join("traces/2026/01/07/trace-escape");
    fs::create_dir_all(&bundle_traces)?;

    fs::write(
        bundle_traces.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-escape",
            "status": "ok",
            "timestamp": "2026-01-07T00:00:00Z"
        }))?,
    )?;

    let traces_dir = data_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;
    let outside = temp.path().join("outside");
    fs::create_dir_all(&outside)?;
    symlink(&outside, traces_dir.join("2026"))?;

    let store = TraceStore::new(data_dir).await?;
    let err = store
        .import_bundle(&bundle_dir)
        .await
        .expect_err("destination symlink should be rejected");
    assert!(err.to_string().contains("symlink"));

    Ok(())
}
