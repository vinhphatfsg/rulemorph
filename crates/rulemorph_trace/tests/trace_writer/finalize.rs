#[tokio::test]
async fn write_trace_bundle_roundtrips_finalize_none() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-roundtrip-none",
        "records": [],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-finalize-roundtrip-none")
        .await?
        .expect("trace should load");
    assert_eq!(loaded.get("finalize"), Some(&json!({ "output": "done" })));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_roundtrips_finalize_zstd() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-roundtrip-zstd",
        "records": [],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::Zstd,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-finalize-roundtrip-zstd")
        .await?
        .expect("trace should load");
    assert_eq!(loaded.get("finalize"), Some(&json!({ "output": "done" })));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_downgrades_on_oversized_finalize() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-oversized",
        "records": [],
        "finalize": { "output": "x".repeat(200) }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        max_chunk_bytes_uncompressed: 64,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "chunk_too_large")
    );
    assert!(detail.records.is_empty());
    assert!(detail.nodes.is_empty());
    assert!(detail.finalize.is_none());

    let trace_dir = trace_dir(&manifest_path);
    assert!(!trace_dir.join("finalize.json").exists());

    Ok(())
}

include!("finalize/errors.rs");
