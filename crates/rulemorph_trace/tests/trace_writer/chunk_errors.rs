#[tokio::test]
async fn write_trace_bundle_skips_malformed_ndjson_line() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-malformed-ndjson",
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

    let mut node_payload = fs::read_to_string(&node_chunk_path)?;
    node_payload.push_str("not-json\n");
    fs::write(&node_chunk_path, node_payload)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-malformed-ndjson")
        .await?
        .expect("trace should load");
    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail should exist");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("chunk_error"))
    );
    let detail_records = detail
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_records.is_empty());
    let detail_nodes = detail
        .get("nodes")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_nodes.is_empty());
    assert!(detail.get("finalize").is_none());
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(records.is_empty());
    assert!(loaded.get("finalize").is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_invalid_utf8_chunk() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-invalid-utf8-chunk",
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
    fs::write(&record_chunk_path, vec![0xff, 0xfe, 0xfd])?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-invalid-utf8-chunk")
        .await?
        .expect("trace should load");
    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("chunk_error"))
    );
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    assert!(records.is_empty());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_unsupported_compression_chunk() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-unsupported-compression",
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
    let mut manifest_value = read_manifest_value(&manifest_path)?;
    if let Some(detail) = manifest_value
        .get_mut("detail")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(records) = detail.get_mut("records").and_then(|v| v.as_array_mut()) {
            if let Some(record) = records.get_mut(0).and_then(|v| v.as_object_mut()) {
                record.insert(
                    "compression".to_string(),
                    serde_json::Value::String("gzip".to_string()),
                );
            }
        }
    }
    fs::write(&manifest_path, serde_json::to_string(&manifest_value)?)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-unsupported-compression")
        .await?
        .expect("trace should load");
    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("chunk_error"))
    );
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    assert!(records.is_empty());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_unsupported_format_chunk() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-unsupported-format",
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
    let mut manifest_value = read_manifest_value(&manifest_path)?;
    if let Some(detail) = manifest_value
        .get_mut("detail")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(records) = detail.get_mut("records").and_then(|v| v.as_array_mut()) {
            if let Some(record) = records.get_mut(0).and_then(|v| v.as_object_mut()) {
                record.insert(
                    "format".to_string(),
                    serde_json::Value::String("json".to_string()),
                );
            }
        }
    }
    fs::write(&manifest_path, serde_json::to_string(&manifest_value)?)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-unsupported-format")
        .await?
        .expect("trace should load");
    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("chunk_error"))
    );
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    assert!(records.is_empty());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_zstd_decode_failure() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-zstd-decode-failure",
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
    let mut manifest_value = read_manifest_value(&manifest_path)?;
    if let Some(detail) = manifest_value
        .get_mut("detail")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(records) = detail.get_mut("records").and_then(|v| v.as_array_mut()) {
            if let Some(record) = records.get_mut(0).and_then(|v| v.as_object_mut()) {
                record.insert(
                    "compression".to_string(),
                    serde_json::Value::String("zstd".to_string()),
                );
            }
        }
    }
    fs::write(&manifest_path, serde_json::to_string(&manifest_value)?)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-zstd-decode-failure")
        .await?
        .expect("trace should load");
    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("chunk_error"))
    );
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    assert!(records.is_empty());

    Ok(())
}
