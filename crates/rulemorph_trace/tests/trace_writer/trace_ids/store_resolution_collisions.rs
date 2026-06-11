#[tokio::test]
async fn write_trace_bundle_disambiguates_manifest_trace_id_collisions() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    fs::write(
        traces_dir.join("manifest-a.json"),
        serde_json::to_string(&json!({
            "trace_schema_version": 1,
            "trace_id": "a/b"
        }))?,
    )?;
    fs::write(
        traces_dir.join("manifest-b.json"),
        serde_json::to_string(&json!({
            "trace_schema_version": 1,
            "trace_id": "a_b"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    let manifest_items: Vec<_> = items
        .into_iter()
        .filter(|item| {
            item.path.ends_with("manifest-a.json") || item.path.ends_with("manifest-b.json")
        })
        .collect();
    assert_eq!(manifest_items.len(), 2);
    let ids: std::collections::HashSet<_> = manifest_items
        .iter()
        .map(|item| item.trace_id.as_str())
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.iter().all(|id| id.starts_with("a_b-dup-")));

    for item in &manifest_items {
        let loaded = store.get(&item.trace_id).await?.expect("trace should load");
        assert_eq!(
            loaded.get("trace_id").and_then(|value| value.as_str()),
            Some(item.trace_id.as_str())
        );
    }

    Ok(())
}

#[tokio::test]
async fn trace_store_collision_resolution_is_stable_across_refresh() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    fs::write(
        traces_dir.join("b.json"),
        serde_json::to_string(&json!({
            "trace_id": "same",
            "records": [],
            "status": "ok"
        }))?,
    )?;
    fs::write(
        traces_dir.join("a.json"),
        serde_json::to_string(&json!({
            "trace_id": "same",
            "records": [],
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let first = store.list().await?;
    let first_map: HashMap<String, String> = first
        .iter()
        .map(|item| (item.path.clone(), item.trace_id.clone()))
        .collect();

    let second = store.list().await?;
    let second_map: HashMap<String, String> = second
        .iter()
        .map(|item| (item.path.clone(), item.trace_id.clone()))
        .collect();

    assert_eq!(first_map, second_map);

    let a_path = traces_dir.join("a.json").display().to_string();
    let b_path = traces_dir.join("b.json").display().to_string();
    let a_id = first_map.get(&a_path).expect("a.json should exist");
    let b_id = first_map.get(&b_path).expect("b.json should exist");
    assert_ne!(a_id, b_id);
    assert!(a_id.starts_with("same-dup-"));
    assert!(b_id.starts_with("same-dup-"));

    Ok(())
}

#[tokio::test]
async fn trace_store_collision_adds_counter_when_candidate_exists() -> anyhow::Result<()> {
    fn fnv1a_hash(bytes: &[u8]) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        let mut hash = FNV_OFFSET_BASIS;
        for byte in bytes {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let base_path = traces_dir.join("a.json");
    fs::write(
        &base_path,
        serde_json::to_string(&json!({
            "trace_id": "same",
            "records": [],
            "status": "ok"
        }))?,
    )?;
    fs::write(
        traces_dir.join("b.json"),
        serde_json::to_string(&json!({
            "trace_id": "same",
            "records": [],
            "status": "ok"
        }))?,
    )?;

    let hash = fnv1a_hash("a.json".as_bytes());
    let conflict_id = format!("same-dup-{hash:x}");
    fs::write(
        traces_dir.join("conflict.json"),
        serde_json::to_string(&json!({
            "trace_id": conflict_id,
            "records": [],
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    let a_item = items
        .iter()
        .find(|item| item.path.ends_with("a.json"))
        .expect("a.json should exist");
    assert_eq!(a_item.trace_id, format!("same-dup-{hash:x}-1"));

    Ok(())
}
