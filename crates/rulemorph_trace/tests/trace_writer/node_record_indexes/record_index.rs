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
include!("record_index_string.rs");

include!("record_index_invalid_missing.rs");
include!("record_index_duplicates.rs");
