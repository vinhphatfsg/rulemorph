#[tokio::test]
async fn write_trace_bundle_wraps_non_object_nodes() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-non-object",
        "records": [
            {
                "status": "ok",
                "nodes": ["raw-node"]
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
    let node = read_ndjson_values(trace_dir.join(&node_chunk.path))?
        .into_iter()
        .next()
        .expect("node line should exist");
    assert_eq!(
        node.get("value").and_then(|value| value.as_str()),
        Some("raw-node")
    );
    assert_eq!(
        node.get("record_index").and_then(|value| value.as_u64()),
        Some(0)
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_wraps_inline_non_object_nodes() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-inline-non-object",
        "records": [
            {
                "status": "ok",
                "nodes": ["raw-node"]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: false,
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
    let nodes = record
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(
        nodes[0].get("value").and_then(|value| value.as_str()),
        Some("raw-node")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_wraps_inline_scalar_nodes_value() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-inline-scalar-nodes",
        "records": [
            {
                "status": "ok",
                "nodes": "raw-node"
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: false,
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
    let nodes = record
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(
        nodes[0].get("value").and_then(|value| value.as_str()),
        Some("raw-node")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_wraps_inline_non_object_nodes_on_read() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-inline-non-object-read",
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
        split_nodes: false,
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
            serde_json::Value::Array(vec![serde_json::Value::String("raw".to_string())]),
        );
    }
    lines[0] = serde_json::to_string(&record)?;
    write_ndjson_lines(&record_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-inline-non-object-read")
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
    assert_eq!(
        nodes[0].get("value").and_then(|value| value.as_str()),
        Some("raw")
    );

    Ok(())
}
