#[tokio::test]
async fn write_trace_bundle_masks_rule_source_when_basic() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-mask-basic-rule-source",
        "status": "ok",
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0
        },
        "rule_source": {
            "token": "secret",
            "headers": {
                "Authorization": "Bearer 123"
            }
        },
        "records": [
            { "index": 0, "input": { "token": "secret" } }
        ]
    });

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Basic,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let loaded = load_trace(&temp_dir, "trace-mask-basic-rule-source").await?;

    let detail = object_field(&loaded, "detail");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    assert!(array_member(detail, "records").is_empty());
    assert!(array_member(detail, "nodes").is_empty());
    assert!(detail.get("finalize").is_none());

    assert!(array_field(&loaded, "records").is_empty());

    let rule_source = object_field(&loaded, "rule_source");
    assert_eq!(
        rule_source.get("token").and_then(|value| value.as_str()),
        Some("[masked]")
    );
    let headers = object_member(rule_source, "headers");
    assert_eq!(
        headers
            .get("Authorization")
            .and_then(|value| value.as_str()),
        Some("[masked]")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_masks_rule_source_when_off() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-mask-off-rule-source",
        "status": "ok",
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0
        },
        "rule_source": {
            "token": "secret"
        },
        "records": [
            { "index": 0, "input": { "token": "secret" } }
        ]
    });

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Off,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let loaded = load_trace(&temp_dir, "trace-mask-off-rule-source").await?;

    let detail = object_field(&loaded, "detail");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("dropped")
    );
    assert!(array_member(detail, "records").is_empty());
    assert!(array_member(detail, "nodes").is_empty());
    assert!(detail.get("finalize").is_none());

    assert!(array_field(&loaded, "records").is_empty());

    let rule_source = object_field(&loaded, "rule_source");
    assert_eq!(
        rule_source.get("token").and_then(|value| value.as_str()),
        Some("[masked]")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_basic_without_masking_strips_detail() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-basic-no-mask",
        "status": "ok",
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0
        },
        "rule_source": {
            "token": "secret"
        },
        "records": [
            { "index": 0, "input": { "token": "secret" } }
        ]
    });

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Basic,
        masking_enabled: false,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let loaded = load_trace(&temp_dir, "trace-basic-no-mask").await?;

    let detail = object_field(&loaded, "detail");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    assert!(array_member(detail, "records").is_empty());
    assert!(array_member(detail, "nodes").is_empty());
    assert!(detail.get("finalize").is_none());

    assert!(array_field(&loaded, "records").is_empty());

    let rule_source = object_field(&loaded, "rule_source");
    assert_eq!(
        rule_source.get("token").and_then(|value| value.as_str()),
        Some("secret")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_off_without_masking_strips_detail() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-off-no-mask",
        "status": "ok",
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0
        },
        "rule_source": {
            "token": "secret"
        },
        "records": [
            { "index": 0, "input": { "token": "secret" } }
        ]
    });

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Off,
        masking_enabled: false,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let loaded = load_trace(&temp_dir, "trace-off-no-mask").await?;

    let detail = object_field(&loaded, "detail");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("dropped")
    );
    assert!(array_member(detail, "records").is_empty());
    assert!(array_member(detail, "nodes").is_empty());
    assert!(detail.get("finalize").is_none());

    assert!(array_field(&loaded, "records").is_empty());

    let rule_source = object_field(&loaded, "rule_source");
    assert_eq!(
        rule_source.get("token").and_then(|value| value.as_str()),
        Some("secret")
    );

    Ok(())
}
