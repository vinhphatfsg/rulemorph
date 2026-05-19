#[tokio::test]
async fn write_trace_bundle_disambiguates_legacy_trace_id_collisions() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    fs::write(
        traces_dir.join("legacy-a.json"),
        serde_json::to_string(&json!({ "trace_id": "a/b", "status": "ok" }))?,
    )?;
    fs::write(
        traces_dir.join("legacy-b.json"),
        serde_json::to_string(&json!({ "trace_id": "a_b", "status": "ok" }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    let legacy_items: Vec<_> = items
        .into_iter()
        .filter(|item| item.path.ends_with("legacy-a.json") || item.path.ends_with("legacy-b.json"))
        .collect();
    assert_eq!(legacy_items.len(), 2);
    let ids: std::collections::HashSet<_> = legacy_items
        .iter()
        .map(|item| item.trace_id.as_str())
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.iter().all(|id| id.starts_with("a_b-dup-")));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sanitizes_manifest_trace_id_on_list_get() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let manifest_path = traces_dir.join("manifest.json");
    fs::write(
        &manifest_path,
        serde_json::to_string(&json!({
            "trace_schema_version": 1,
            "trace_id": "a/b"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].trace_id, "a_b");

    let loaded = store
        .get(&items[0].trace_id)
        .await?
        .expect("trace should load");
    assert_eq!(
        loaded.get("trace_id").and_then(|value| value.as_str()),
        Some("a_b")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_manifest_empty_trace_id_falls_back_to_hash() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let manifest_path = traces_dir.join("trace.json");
    fs::write(
        &manifest_path,
        serde_json::to_string(&json!({
            "trace_schema_version": 1,
            "trace_id": ""
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert!(items[0].trace_id.starts_with("trace-"));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_manifest_empty_trace_id_uses_file_stem() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let manifest_path = traces_dir.join("custom-id.json");
    fs::write(
        &manifest_path,
        serde_json::to_string(&json!({
            "trace_schema_version": 1,
            "trace_id": ""
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].trace_id, "custom-id");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_manifest_trace_id_trace_falls_back_to_hash() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let manifest_path = traces_dir.join("manifest-trace.json");
    fs::write(
        &manifest_path,
        serde_json::to_string(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace"
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
async fn write_trace_bundle_manifest_dot_trace_id_uses_file_stem() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    for raw in [".", ".."] {
        let manifest_path = traces_dir.join(format!("manifest-dot-{raw}.json"));
        fs::write(
            &manifest_path,
            serde_json::to_string(&json!({
                "trace_schema_version": 1,
                "trace_id": raw
            }))?,
        )?;
    }

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    let trace_ids: std::collections::HashSet<_> =
        items.iter().map(|item| item.trace_id.as_str()).collect();
    assert!(trace_ids.contains("manifest-dot-."));
    assert!(trace_ids.contains("manifest-dot-.."));

    Ok(())
}

include!("store_resolution/legacy.rs");

include!("store_resolution/collisions.rs");
