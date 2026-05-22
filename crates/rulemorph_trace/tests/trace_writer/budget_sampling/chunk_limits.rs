#[tokio::test]
async fn write_trace_bundle_downgrades_on_oversized_record_line() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-oversize-line",
        "records": [
            { "index": 0, "payload": "x".repeat(1024) }
        ]
    });

    let options = TraceWriteOptions {
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

    assert_no_detail_artifacts(&manifest_path)?;

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_downgrades_on_chunk_count_exceeded() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let record_count = TRACE_CHUNK_COUNT_HARD_MAX + 1;
    let records: Vec<_> = (0..record_count)
        .map(|index| json!({ "index": index, "value": index }))
        .collect();
    let trace = json!({
        "trace_id": "trace-chunk-count",
        "records": records
    });

    let options = TraceWriteOptions {
        max_records_per_chunk: 1,
        compression: TraceCompression::None,
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
            .any(|reason| reason == "budget_exceeded")
    );
    assert!(detail.records.is_empty());
    assert!(detail.nodes.is_empty());
    assert!(detail.finalize.is_none());

    assert_no_detail_artifacts(&manifest_path)?;

    Ok(())
}
