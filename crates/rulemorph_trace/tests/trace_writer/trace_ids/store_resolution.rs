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

include!("store_resolution_manifest.rs");

include!("store_resolution_legacy.rs");

include!("store_resolution_collisions.rs");
