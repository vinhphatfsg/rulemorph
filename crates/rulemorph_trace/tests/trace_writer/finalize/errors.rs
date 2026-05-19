#[tokio::test]
async fn write_trace_bundle_skips_finalize_unsupported_format() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-unsupported-format",
        "records": [],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let mut manifest_value = read_manifest_value(&manifest_path)?;
    if let Some(detail) = manifest_value
        .get_mut("detail")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(finalize) = detail.get_mut("finalize").and_then(|v| v.as_object_mut()) {
            finalize.insert(
                "format".to_string(),
                serde_json::Value::String("ndjson".to_string()),
            );
        }
    }
    fs::write(&manifest_path, serde_json::to_string(&manifest_value)?)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-finalize-unsupported-format")
        .await?
        .expect("trace should load");
    assert!(loaded.get("finalize").is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_finalize_unsupported_compression() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-unsupported-compression",
        "records": [],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let mut manifest_value = read_manifest_value(&manifest_path)?;
    if let Some(detail) = manifest_value
        .get_mut("detail")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(finalize) = detail.get_mut("finalize").and_then(|v| v.as_object_mut()) {
            finalize.insert(
                "compression".to_string(),
                serde_json::Value::String("gzip".to_string()),
            );
        }
    }
    fs::write(&manifest_path, serde_json::to_string(&manifest_value)?)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-finalize-unsupported-compression")
        .await?
        .expect("trace should load");
    assert!(loaded.get("finalize").is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_finalize_invalid_json() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-invalid-json",
        "records": [],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let finalize = detail.finalize.expect("finalize chunk should exist");
    let finalize_path = trace_dir.join(&finalize.path);
    fs::write(&finalize_path, b"{invalid json")?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-finalize-invalid-json")
        .await?
        .expect("trace should load");
    assert!(loaded.get("finalize").is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_finalize_zstd_decode_failure() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-zstd-decode-failure",
        "records": [],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let mut manifest_value = read_manifest_value(&manifest_path)?;
    if let Some(detail) = manifest_value
        .get_mut("detail")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(finalize) = detail.get_mut("finalize").and_then(|v| v.as_object_mut()) {
            finalize.insert(
                "compression".to_string(),
                serde_json::Value::String("zstd".to_string()),
            );
        }
    }
    fs::write(&manifest_path, serde_json::to_string(&manifest_value)?)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-finalize-zstd-decode-failure")
        .await?
        .expect("trace should load");
    assert!(loaded.get("finalize").is_none());

    Ok(())
}
