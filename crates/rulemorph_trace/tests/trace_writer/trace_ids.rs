#[tokio::test]
async fn write_trace_bundle_generates_trace_id_when_missing() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(manifest.trace_id.starts_with("trace-"));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_generates_trace_id_when_empty() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(manifest.trace_id.starts_with("trace-"));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sanitizes_trace_id() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "../unsafe/trace",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(!manifest.trace_id.contains('/'));
    assert!(!manifest.trace_id.contains('\\'));
    assert!(manifest.trace_id.contains(".."));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sanitizes_trace_id_general_case() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "abc DEF/ghi あ",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert_eq!(manifest.trace_id, "abc_DEF_ghi__");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_rejects_dot_trace_id() -> anyhow::Result<()> {
    for raw in [".", ".."] {
        let temp_dir = create_temp_dir()?;

        let trace = json!({
            "trace_id": raw,
            "records": []
        });

        let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
        let manifest = read_manifest(&manifest_path)?;

        assert!(manifest.trace_id.starts_with("trace-"));
        assert_ne!(manifest.trace_id, raw);
    }

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_trace_trace_id_falls_back_to_generated() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(manifest.trace_id.starts_with("trace-"));
    assert_ne!(manifest.trace_id, "trace");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_preserves_trailing_underscore_trace_id() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace_",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert_eq!(manifest.trace_id, "trace_");

    Ok(())
}

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
async fn write_trace_bundle_avoids_trace_id_collision_on_sanitize() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace_a = json!({
        "trace_id": "a/b",
        "timestamp": "2026-02-01T00:00:00Z",
        "records": []
    });
    let trace_b = json!({
        "trace_id": "a_b",
        "timestamp": "2026-02-01T00:00:00Z",
        "records": []
    });

    let manifest_a_path = write_trace_bundle(&temp_dir, &trace_a, None).await?;
    let manifest_a = read_manifest(&manifest_a_path)?;

    let manifest_b_path = write_trace_bundle(&temp_dir, &trace_b, None).await?;
    let manifest_b = read_manifest(&manifest_b_path)?;

    assert_ne!(manifest_a.trace_id, manifest_b.trace_id);

    Ok(())
}

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
