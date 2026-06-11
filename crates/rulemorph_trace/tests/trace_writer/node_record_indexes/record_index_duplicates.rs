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
