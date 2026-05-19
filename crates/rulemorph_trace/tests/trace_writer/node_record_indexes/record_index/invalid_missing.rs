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
