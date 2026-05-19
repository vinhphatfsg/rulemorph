#[tokio::test]
async fn write_trace_bundle_sanitizes_legacy_trace_id() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let legacy_path = traces_dir.join("legacy.json");
    fs::write(
        &legacy_path,
        serde_json::to_string(&json!({
            "trace_id": "legacy id/非",
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].trace_id, "legacy_id__");
    let loaded = store
        .get(&items[0].trace_id)
        .await?
        .expect("trace should load");
    assert_eq!(
        loaded.get("trace_id").and_then(|value| value.as_str()),
        Some("legacy_id__")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_legacy_missing_trace_id_trace_filename_falls_back_to_hash()
-> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let legacy_path = traces_dir.join("trace.json");
    fs::write(
        &legacy_path,
        serde_json::to_string(&json!({
            "records": [],
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert!(items[0].trace_id.starts_with("trace-"));
    assert_ne!(items[0].trace_id, "trace");

    let loaded = store
        .get(&items[0].trace_id)
        .await?
        .expect("trace should load");
    assert_eq!(
        loaded.get("trace_id").and_then(|value| value.as_str()),
        Some(items[0].trace_id.as_str())
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_legacy_trace_id_trace_falls_back_to_hash() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let legacy_path = traces_dir.join("legacy-trace.json");
    fs::write(
        &legacy_path,
        serde_json::to_string(&json!({
            "trace_id": "trace",
            "records": [],
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert!(items[0].trace_id.starts_with("trace-"));
    assert_ne!(items[0].trace_id, "trace");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_legacy_empty_trace_id_uses_file_stem() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let legacy_path = traces_dir.join("legacy-custom.json");
    fs::write(
        &legacy_path,
        serde_json::to_string(&json!({
            "trace_id": "",
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].trace_id, "legacy-custom");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_legacy_dot_trace_id_uses_file_stem() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    for raw in [".", ".."] {
        let legacy_path = traces_dir.join(format!("legacy-dot-{raw}.json"));
        fs::write(
            &legacy_path,
            serde_json::to_string(&json!({
                "trace_id": raw,
                "status": "ok"
            }))?,
        )?;
    }

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    let trace_ids: std::collections::HashSet<_> =
        items.iter().map(|item| item.trace_id.as_str()).collect();
    assert!(trace_ids.contains("legacy-dot-."));
    assert!(trace_ids.contains("legacy-dot-.."));

    Ok(())
}
