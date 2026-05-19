#[tokio::test]
async fn write_trace_bundle_inherits_record_index_for_legacy_wrapper() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-legacy-wrapper-inherit",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let mut lines = read_ndjson_lines(&node_chunk_path)?;
    let first: serde_json::Value = serde_json::from_str(&lines[0])?;
    let wrapper_first = json!({
        "record_index": first.get("record_index").cloned().unwrap_or(json!(0)),
        "node": first
    });
    let mut wrapper_second = json!({
        "node": { "id": "n2", "kind": "branch", "status": "ok" }
    });
    if let Some(obj) = wrapper_second.as_object_mut() {
        obj.remove("record_index");
    }
    lines = vec![
        serde_json::to_string(&wrapper_first)?,
        serde_json::to_string(&wrapper_second)?,
    ];
    write_ndjson_lines(&node_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-legacy-wrapper-inherit")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    assert_eq!(nodes_first.len(), 2);
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );
    assert_eq!(
        nodes_first[1].get("id").and_then(|value| value.as_str()),
        Some("n2")
    );

    Ok(())
}
#[tokio::test]
async fn write_trace_bundle_parses_string_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-string-index",
        "records": [
            {
                "index": 0,
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let record_chunk = &detail.records[0];
    let record_chunk_path = trace_dir.join(&record_chunk.path);

    let mut lines = read_ndjson_lines(&record_chunk_path)?;
    let mut record: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = record.as_object_mut() {
        obj.insert(
            "index".to_string(),
            serde_json::Value::String("0".to_string()),
        );
    }
    lines[0] = serde_json::to_string(&record)?;
    write_ndjson_lines(&record_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-string-index")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );

    Ok(())
}

include!("node_record_indexes/inline_nodes.rs");

#[tokio::test]
async fn write_trace_bundle_skips_invalid_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-invalid-index",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" },
                    { "id": "n2", "kind": "branch", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let mut lines = read_ndjson_lines(&node_chunk_path)?;
    let mut second: serde_json::Value = serde_json::from_str(&lines[1])?;
    if let Some(obj) = second.as_object_mut() {
        obj.insert(
            "record_index".to_string(),
            serde_json::Value::String("invalid".to_string()),
        );
    }
    lines[1] = serde_json::to_string(&second)?;
    write_ndjson_lines(&node_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-invalid-index")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    assert_eq!(nodes_first.len(), 1);
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_null_record_index_keeps_inheritance() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-null-index",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" },
                    { "id": "n2", "kind": "branch", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let mut lines = read_ndjson_lines(&node_chunk_path)?;
    let first: serde_json::Value = serde_json::from_str(&lines[0])?;
    let mut second: serde_json::Value = serde_json::from_str(&lines[1])?;
    if let Some(obj) = second.as_object_mut() {
        obj.insert("record_index".to_string(), serde_json::Value::Null);
        obj.insert(
            "id".to_string(),
            serde_json::Value::String("n2-invalid".to_string()),
        );
    }
    let mut third = first.clone();
    if let Some(obj) = third.as_object_mut() {
        obj.remove("record_index");
        obj.insert(
            "id".to_string(),
            serde_json::Value::String("n3".to_string()),
        );
    }
    lines = vec![
        serde_json::to_string(&first)?,
        serde_json::to_string(&second)?,
        serde_json::to_string(&third)?,
    ];
    write_ndjson_lines(&node_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-null-index")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    assert_eq!(nodes_first.len(), 2);
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );
    assert_eq!(
        nodes_first[1].get("id").and_then(|value| value.as_str()),
        Some("n3")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_dedupes_duplicate_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-duplicate-index",
        "records": [
            {
                "index": 1,
                "status": "ok",
                "nodes": [{ "id": "n1", "kind": "mappings", "status": "ok" }]
            },
            {
                "index": 1,
                "status": "ok",
                "nodes": [{ "id": "n2", "kind": "branch", "status": "ok" }]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);

    let record_chunk = &detail.records[0];
    let mut record_indices: Vec<u64> = read_ndjson_values(trace_dir.join(&record_chunk.path))?
        .iter()
        .filter_map(|record| record.get("index").and_then(|value| value.as_u64()))
        .collect();
    record_indices.sort_unstable();
    assert_eq!(record_indices, vec![1, 2]);

    let node_chunk = &detail.nodes[0];
    let mut node_indices: Vec<u64> = read_ndjson_values(trace_dir.join(&node_chunk.path))?
        .iter()
        .filter_map(|node| node.get("record_index").and_then(|value| value.as_u64()))
        .collect();
    node_indices.sort_unstable();
    assert_eq!(node_indices, vec![1, 2]);

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_missing_first_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-missing-first-index",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" },
                    { "id": "n2", "kind": "branch", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let mut lines = read_ndjson_lines(&node_chunk_path)?;
    let mut first: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = first.as_object_mut() {
        obj.remove("record_index");
    }
    lines[0] = serde_json::to_string(&first)?;
    write_ndjson_lines(&node_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-missing-first-index")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(nodes_first.len(), 1);
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n2")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_accepts_string_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-string-index-input",
        "records": [
            {
                "index": "2",
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);

    let record_chunk = &detail.records[0];
    let record = read_ndjson_values(trace_dir.join(&record_chunk.path))?
        .into_iter()
        .next()
        .expect("record line should exist");
    assert_eq!(
        record.get("index").and_then(|value| value.as_u64()),
        Some(2)
    );

    let node_chunk = &detail.nodes[0];
    let node = read_ndjson_values(trace_dir.join(&node_chunk.path))?
        .into_iter()
        .next()
        .expect("node line should exist");
    assert_eq!(
        node.get("record_index").and_then(|value| value.as_u64()),
        Some(2)
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_first_wins_duplicate_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-dup-index-first-wins",
        "records": [
            {
                "status": "ok",
                "nodes": [{ "id": "n1", "kind": "mappings", "status": "ok" }]
            },
            {
                "status": "ok",
                "nodes": [{ "id": "n2", "kind": "branch", "status": "ok" }]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);

    let record_chunk = &detail.records[0];
    let record_chunk_path = trace_dir.join(&record_chunk.path);
    let mut record_lines = read_ndjson_lines(&record_chunk_path)?;
    for line in &mut record_lines {
        let mut record: serde_json::Value = serde_json::from_str(line)?;
        if let Some(obj) = record.as_object_mut() {
            obj.insert("index".to_string(), serde_json::Value::from(0));
        }
        *line = serde_json::to_string(&record)?;
    }
    write_ndjson_lines(&record_chunk_path, &record_lines)?;

    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);
    let mut node_lines = read_ndjson_lines(&node_chunk_path)?;
    for line in &mut node_lines {
        let mut node: serde_json::Value = serde_json::from_str(line)?;
        if let Some(obj) = node.as_object_mut() {
            obj.insert("record_index".to_string(), serde_json::Value::from(0));
        }
        *line = serde_json::to_string(&node)?;
    }
    write_ndjson_lines(&node_chunk_path, &node_lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-dup-index-first-wins")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    let nodes_second = records[1].get("nodes");
    assert_eq!(nodes_first.len(), 2);
    assert!(nodes_second.is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_missing_record_index_across_chunks() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-missing-index-across-chunks",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" },
                    { "id": "n2", "kind": "branch", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        max_nodes_per_chunk: 1,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    assert!(detail.nodes.len() >= 2);
    let trace_dir = trace_dir(&manifest_path);
    let second_chunk = &detail.nodes[1];
    let second_chunk_path = trace_dir.join(&second_chunk.path);
    let mut lines = read_ndjson_lines(&second_chunk_path)?;
    let mut node: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = node.as_object_mut() {
        obj.remove("record_index");
    }
    lines[0] = serde_json::to_string(&node)?;
    write_ndjson_lines(&second_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-missing-index-across-chunks")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_parses_string_record_index_in_nodes() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-string-index-line",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let mut lines = read_ndjson_lines(&node_chunk_path)?;
    let mut node: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = node.as_object_mut() {
        obj.insert(
            "record_index".to_string(),
            serde_json::Value::String("0".to_string()),
        );
    }
    lines[0] = serde_json::to_string(&node)?;
    write_ndjson_lines(&node_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-string-index-line")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(nodes_first.len(), 1);
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_invalid_record_index_in_record() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-invalid-record-index",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let record_chunk = &detail.records[0];
    let record_chunk_path = trace_dir.join(&record_chunk.path);

    let mut lines = read_ndjson_lines(&record_chunk_path)?;
    let mut record: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = record.as_object_mut() {
        obj.insert(
            "index".to_string(),
            serde_json::Value::String("invalid".to_string()),
        );
    }
    lines[0] = serde_json::to_string(&record)?;
    write_ndjson_lines(&record_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-invalid-record-index")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    assert!(records[0].get("nodes").is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_emits_chunk_metadata_offsets() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-chunk-metadata",
        "records": [
            { "status": "ok", "nodes": [{ "id": "n1", "kind": "mappings", "status": "ok" }] },
            { "status": "ok", "nodes": [{ "id": "n2", "kind": "branch", "status": "ok" }] },
            { "status": "ok", "nodes": [{ "id": "n3", "kind": "branch", "status": "ok" }] }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        max_records_per_chunk: 2,
        max_nodes_per_chunk: 2,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.records.len(), 2);
    assert_eq!(detail.records[0].record_start, Some(0));
    assert_eq!(detail.records[0].record_end, Some(1));
    assert_eq!(detail.records[1].record_start, Some(2));
    assert_eq!(detail.records[1].record_end, Some(2));

    assert_eq!(detail.nodes.len(), 2);
    assert_eq!(detail.nodes[0].node_start, Some(0));
    assert_eq!(detail.nodes[0].node_end, Some(1));
    assert_eq!(detail.nodes[1].node_start, Some(2));
    assert_eq!(detail.nodes[1].node_end, Some(2));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_emits_chunk_metadata_offsets_with_non_sequential_index()
-> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-chunk-metadata-non-seq",
        "records": [
            { "index": 10, "status": "ok", "nodes": [{ "id": "n1", "kind": "mappings", "status": "ok" }] },
            { "index": 20, "status": "ok", "nodes": [{ "id": "n2", "kind": "branch", "status": "ok" }] },
            { "index": 30, "status": "ok", "nodes": [{ "id": "n3", "kind": "branch", "status": "ok" }] }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        max_records_per_chunk: 2,
        max_nodes_per_chunk: 2,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.records.len(), 2);
    assert_eq!(detail.records[0].record_start, Some(0));
    assert_eq!(detail.records[0].record_end, Some(1));
    assert_eq!(detail.records[1].record_start, Some(2));
    assert_eq!(detail.records[1].record_end, Some(2));

    assert_eq!(detail.nodes.len(), 2);
    assert_eq!(detail.nodes[0].node_start, Some(0));
    assert_eq!(detail.nodes[0].node_end, Some(1));
    assert_eq!(detail.nodes[1].node_start, Some(2));
    assert_eq!(detail.nodes[1].node_end, Some(2));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_invalid_record_index_keeps_inheritance() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-invalid-index-keeps",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" },
                    { "id": "n2", "kind": "branch", "status": "ok" },
                    { "id": "n3", "kind": "branch", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let mut lines = read_ndjson_lines(&node_chunk_path)?;
    let mut second: serde_json::Value = serde_json::from_str(&lines[1])?;
    if let Some(obj) = second.as_object_mut() {
        obj.insert(
            "record_index".to_string(),
            serde_json::Value::String("invalid".to_string()),
        );
    }
    lines[1] = serde_json::to_string(&second)?;
    let mut third: serde_json::Value = serde_json::from_str(&lines[2])?;
    if let Some(obj) = third.as_object_mut() {
        obj.remove("record_index");
    }
    lines[2] = serde_json::to_string(&third)?;
    write_ndjson_lines(&node_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-invalid-index-keeps")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    let ids: Vec<&str> = nodes
        .iter()
        .filter_map(|node| node.get("id").and_then(|value| value.as_str()))
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&"n1"));
    assert!(ids.contains(&"n3"));

    Ok(())
}
