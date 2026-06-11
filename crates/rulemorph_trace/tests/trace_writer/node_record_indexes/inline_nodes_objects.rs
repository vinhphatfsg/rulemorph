#[tokio::test]
async fn write_trace_bundle_normalizes_inline_nodes() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-inline-nodes",
        "records": [
            {
                "index": 0,
                "status": "ok",
                "nodes": { "id": "n1", "kind": "mappings", "status": "ok" }
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: false,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-inline-nodes")
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
async fn write_trace_bundle_treats_node_key_with_core_fields_as_normal_node() -> anyhow::Result<()>
{
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-node-key",
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
    let mut first: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = first.as_object_mut() {
        obj.insert(
            "id".to_string(),
            serde_json::Value::String("top".to_string()),
        );
        obj.insert(
            "kind".to_string(),
            serde_json::Value::String("mappings".to_string()),
        );
        obj.insert(
            "status".to_string(),
            serde_json::Value::String("ok".to_string()),
        );
        obj.insert(
            "node".to_string(),
            json!({ "id": "wrapped", "kind": "branch", "status": "ok" }),
        );
    }
    lines[0] = serde_json::to_string(&first)?;
    write_ndjson_lines(&node_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-node-key")
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
        Some("top")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_merges_inline_nodes_with_chunk() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-merge",
        "records": [
            {
                "status": "ok",
                "nodes": [{ "id": "n1", "kind": "mappings", "status": "ok" }]
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
            "nodes".to_string(),
            serde_json::Value::Array(vec![json!({
                "id": "inline",
                "kind": "mappings",
                "status": "ok"
            })]),
        );
    }
    lines[0] = serde_json::to_string(&record)?;
    write_ndjson_lines(&record_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-merge")
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
    assert!(ids.contains(&"inline"));
    assert!(ids.contains(&"n1"));

    Ok(())
}
