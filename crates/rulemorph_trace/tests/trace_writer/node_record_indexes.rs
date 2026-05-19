
include!("node_record_indexes/record_index.rs");

include!("node_record_indexes/inline_nodes.rs");

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
