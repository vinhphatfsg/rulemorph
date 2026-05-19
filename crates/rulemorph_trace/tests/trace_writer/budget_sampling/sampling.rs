#[tokio::test]
async fn write_trace_bundle_sampling_skips_success() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-sample-success",
        "status": "ok",
        "records": [
            { "index": 0, "status": "ok", "value": 1 }
        ],
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0,
            "duration_us": 10
        }
    });

    let options = TraceWriteOptions {
        sampling_rate: 0.0,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "basic");
    assert!(detail.reason.iter().any(|reason| reason == "sampled_out"));
    assert!(detail.records.is_empty());
    assert!(detail.nodes.is_empty());
    assert!(detail.finalize.is_none());

    assert_no_detail_artifacts(&manifest_path)?;

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sampling_keeps_error() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-sample-error",
        "status": "error",
        "records": [
            { "index": 0, "status": "error", "value": 1 }
        ],
        "summary": {
            "record_total": 1,
            "record_success": 0,
            "record_failed": 1,
            "duration_us": 20
        }
    });

    let options = TraceWriteOptions {
        sampling_rate: 0.0,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "full");
    assert!(!detail.records.is_empty());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sampling_keeps_slow_trace() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-sample-slow",
        "status": "ok",
        "records": [
            { "index": 0, "status": "ok", "value": 1 }
        ],
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0,
            "duration_us": 250
        }
    });

    let options = TraceWriteOptions {
        sampling_rate: 0.0,
        sampling_slow_threshold_us: Some(100),
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "full");
    assert!(!detail.reason.iter().any(|reason| reason == "sampled_out"));
    assert!(!detail.records.is_empty());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sampling_uses_generated_trace_id_for_placeholder() -> anyhow::Result<()>
{
    let temp_dir = create_temp_dir()?;

    let rate = 0.5;
    let placeholder_keep = sampling_bucket("trace") < rate;

    for _ in 0..10 {
        let trace = json!({
            "trace_id": "trace",
            "status": "ok",
            "records": [
                { "index": 0, "status": "ok", "value": 1 }
            ],
            "summary": {
                "record_total": 1,
                "record_success": 1,
                "record_failed": 0,
                "duration_us": 10
            }
        });

        let options = TraceWriteOptions {
            sampling_rate: rate,
            compression: TraceCompression::None,
            ..Default::default()
        };

        let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
        let manifest = read_manifest(&manifest_path)?;
        let detail = manifest.detail.expect("detail should exist");
        let keep = sampling_bucket(&manifest.trace_id) < rate;
        if keep != placeholder_keep {
            let expected = if keep { "full" } else { "basic" };
            assert_eq!(detail.status, expected);
            return Ok(());
        }
    }

    panic!("failed to find sampling bucket differing from placeholder");
}
