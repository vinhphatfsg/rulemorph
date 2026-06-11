include!("rule_source_detail_levels.rs");

#[tokio::test]
async fn write_trace_bundle_drops_oversized_rule_source() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-rule-source-drop",
        "rule_source": {
            "text": "x".repeat(2048)
        },
        "records": [
            { "index": 0, "status": "ok" }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        max_bytes_per_trace: 64,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(manifest.rule_source.is_none());
    let detail = manifest.detail.expect("detail should exist");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "rule_source_dropped")
    );

    Ok(())
}
