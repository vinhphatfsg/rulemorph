#[tokio::test]
async fn write_trace_bundle_reads_legacy_node_wrapper() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-legacy-wrapper",
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
    let original: serde_json::Value = serde_json::from_str(&lines[0])?;
    let record_index = original
        .get("record_index")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let mut node = original.clone();
    if let Some(obj) = node.as_object_mut() {
        obj.remove("record_index");
    }
    let wrapped = json!({
        "record_index": record_index.to_string(),
        "node": node
    });
    lines[0] = serde_json::to_string(&wrapped)?;
    write_ndjson_lines(&node_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-legacy-wrapper")
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

#[tokio::test]
async fn write_trace_bundle_keeps_node_key_with_sibling_fields() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-legacy-wrapper-extra-keys",
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
    let original: serde_json::Value = serde_json::from_str(&lines[0])?;
    let record_index = original
        .get("record_index")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let mut node = original.clone();
    if let Some(obj) = node.as_object_mut() {
        obj.remove("record_index");
    }
    let wrapped = json!({
        "record_index": record_index.to_string(),
        "node": node,
        "extra": "ignored"
    });
    lines[0] = serde_json::to_string(&wrapped)?;
    write_ndjson_lines(&node_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-legacy-wrapper-extra-keys")
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
    assert!(nodes_first[0].get("id").is_none());
    assert_eq!(
        nodes_first[0].get("extra").and_then(|value| value.as_str()),
        Some("ignored")
    );
    assert_eq!(
        nodes_first[0]
            .get("node")
            .and_then(|value| value.get("id"))
            .and_then(|value| value.as_str()),
        Some("n1")
    );
    assert!(nodes_first[0].get("record_index").is_none());

    Ok(())
}
