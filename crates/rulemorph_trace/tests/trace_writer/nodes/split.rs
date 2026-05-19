#[tokio::test]
async fn write_trace_bundle_splits_nodes() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            },
            {
                "status": "ok",
                "nodes": [
                    { "id": "n2", "kind": "branch", "status": "ok" }
                ]
            }
        ],
        "summary": {
            "record_total": 2,
            "record_success": 2,
            "record_failed": 0
        }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "full");
    assert_eq!(detail.layout, "records_nodes_split");
    assert!(!detail.records.is_empty());
    assert!(!detail.nodes.is_empty());

    let trace_dir = trace_dir(&manifest_path);
    let record_chunk = &detail.records[0];
    for record in read_ndjson_values(trace_dir.join(&record_chunk.path))? {
        assert!(record.get("nodes").is_none());
    }

    let node_chunk = &detail.nodes[0];
    for node in read_ndjson_values(trace_dir.join(&node_chunk.path))? {
        assert!(node.get("record_index").is_some());
        assert!(node.get("id").is_some());
        assert!(node.get("kind").is_some());
        assert!(node.get("node").is_none());
    }

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store.get("trace-nodes").await?.expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    assert!(records[0].get("nodes").is_some());
    let nodes = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(
        nodes[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );
    assert!(nodes[0].get("record_index").is_none());
    let nodes_second = records[1]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("second nodes should be array");
    assert_eq!(
        nodes_second[0].get("id").and_then(|value| value.as_str()),
        Some("n2")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_recovers_nodes_without_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-missing-index",
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
        obj.remove("record_index");
    }
    lines[1] = serde_json::to_string(&second)?;
    write_ndjson_lines(&node_chunk_path, &lines)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-missing-index")
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
    assert_eq!(
        nodes_first[1].get("id").and_then(|value| value.as_str()),
        Some("n2")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_preserves_node_owned_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-node-owned-record-index",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    {
                        "id": "n1",
                        "kind": "mappings",
                        "status": "ok",
                        "record_index": "node-owned"
                    }
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
    let raw_entry = read_ndjson_values(trace_dir.join(&node_chunk.path))?
        .into_iter()
        .next()
        .expect("node line should exist");
    assert_eq!(
        raw_entry
            .get("record_index")
            .and_then(|value| value.as_u64()),
        Some(0)
    );
    assert_eq!(
        raw_entry
            .get("node")
            .and_then(|value| value.get("record_index"))
            .and_then(|value| value.as_str()),
        Some("node-owned")
    );

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-node-owned-record-index")
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
        nodes[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );
    assert_eq!(
        nodes[0]
            .get("record_index")
            .and_then(|value| value.as_str()),
        Some("node-owned")
    );

    Ok(())
}
