#[tokio::test]
async fn import_bundle_rejects_overwrite() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let existing_dir = data_dir.join("traces/2026/01/06/trace-existing");
    fs::create_dir_all(&existing_dir)?;
    fs::write(
        existing_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-existing",
            "status": "ok"
        }))?,
    )?;

    let bundle_dir = temp.path().join("bundle");
    let bundle_traces = bundle_dir.join("traces/2026/01/06/trace-existing");
    fs::create_dir_all(&bundle_traces)?;
    fs::write(
        bundle_traces.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-overwrite",
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(data_dir.clone()).await?;
    let err = store
        .import_bundle(&bundle_dir)
        .await
        .expect_err("overwrite should be rejected");
    assert!(err.to_string().contains("overwrite"));

    Ok(())
}

#[tokio::test]
async fn import_bundle_rolls_back_on_rule_conflict() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");

    let conflict_path = data_dir.join("rules/conflict.yaml");
    fs::create_dir_all(conflict_path.parent().expect("rules dir"))?;
    fs::write(&conflict_path, "existing: true")?;

    let bundle_traces = bundle_dir.join("traces/2026/01/08/trace-rollback");
    fs::create_dir_all(&bundle_traces)?;
    fs::write(
        bundle_traces.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-rollback",
            "status": "ok",
            "timestamp": "2026-01-08T00:00:00Z"
        }))?,
    )?;
    let bundle_rules = bundle_dir.join("rules");
    fs::create_dir_all(&bundle_rules)?;
    fs::write(bundle_rules.join("conflict.yaml"), "bundle: true")?;

    let store = TraceStore::new(data_dir.clone()).await?;
    let err = store
        .import_bundle(&bundle_dir)
        .await
        .expect_err("rule conflict should fail");
    assert!(err.to_string().contains("overwrite"));

    let imported_trace = data_dir.join("traces/2026/01/08/trace-rollback/trace.json");
    assert!(
        !imported_trace.exists(),
        "trace should be rolled back on import failure"
    );

    Ok(())
}

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
