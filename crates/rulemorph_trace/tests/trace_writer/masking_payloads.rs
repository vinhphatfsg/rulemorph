#[tokio::test]
async fn write_trace_bundle_masks_sensitive_fields() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-mask",
        "records": [
            {
                "index": 0,
                "input": {
                    "authorization": "Bearer abc",
                    "password": "secret",
                    "nested": { "token": "abc" },
                    "ok": 1
                },
                "output": { "secret": "value" }
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let loaded = load_trace(&temp_dir, "trace-mask").await?;

    let record = first_record_object(&loaded);
    let input = object_member(record, "input");
    let nested = object_member(input, "nested");
    let output = object_member(record, "output");

    assert_eq!(
        input.get("authorization").and_then(|value| value.as_str()),
        Some("[masked]")
    );
    assert_eq!(
        input.get("password").and_then(|value| value.as_str()),
        Some("[masked]")
    );
    assert_eq!(
        nested.get("token").and_then(|value| value.as_str()),
        Some("[masked]")
    );
    assert_eq!(
        output.get("secret").and_then(|value| value.as_str()),
        Some("[masked]")
    );

    Ok(())
}

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

#[tokio::test]
async fn write_trace_bundle_masks_url_query_params() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-mask-url",
        "records": [
            {
                "index": 0,
                "input": {
                    "url": "https://example.com/path?token=abc&ok=1#frag"
                }
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let loaded = load_trace(&temp_dir, "trace-mask-url").await?;

    let record = first_record_object(&loaded);
    let input = object_member(record, "input");

    assert_eq!(
        input.get("url").and_then(|value| value.as_str()),
        Some("https://example.com/path?token=[masked]&ok=1#frag")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_masks_url_fragment_params() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-mask-url-fragment",
        "records": [
            {
                "index": 0,
                "input": {
                    "fragment_only": "https://example.com/callback#access_token=abc&ok=1",
                    "query_and_fragment": "https://example.com/path?ok=1#token=abc",
                    "hash_route": "https://example.com/#/callback?token=abc&ok=1"
                }
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let loaded = load_trace(&temp_dir, "trace-mask-url-fragment").await?;

    let record = first_record_object(&loaded);
    let input = object_member(record, "input");

    assert_eq!(
        input.get("fragment_only").and_then(|value| value.as_str()),
        Some("https://example.com/callback#access_token=[masked]&ok=1")
    );
    assert_eq!(
        input
            .get("query_and_fragment")
            .and_then(|value| value.as_str()),
        Some("https://example.com/path?ok=1#token=[masked]")
    );
    assert_eq!(
        input.get("hash_route").and_then(|value| value.as_str()),
        Some("https://example.com/#/callback?token=[masked]&ok=1")
    );

    Ok(())
}

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

#[tokio::test]
async fn write_trace_bundle_externalizes_large_payloads() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-blob",
        "records": [
            { "index": 0, "input": { "data": "x".repeat(200) } }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        max_payload_bytes: 32,
        payload_preview_bytes: 8,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let loaded = load_trace(&temp_dir, "trace-blob").await?;
    let record = first_record_object(&loaded);
    let input = object_member(record, "input");

    let blob_ref = input
        .get("blob_ref")
        .and_then(|value| value.as_str())
        .expect("blob_ref");
    let size_bytes = input
        .get("size_bytes")
        .and_then(|value| value.as_u64())
        .expect("size_bytes");
    assert!(size_bytes > 32);

    let trace_dir = trace_dir(&manifest_path);
    let blob_path = trace_dir.join(blob_ref);
    assert!(blob_path.exists(), "blob file should exist");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_counts_blob_bytes_in_budget() -> anyhow::Result<()> {
    let trace = json!({
        "trace_id": "trace-blob-budget",
        "records": [
            { "index": 0, "input": { "data": "x".repeat(5000) } }
        ]
    });

    let options_full = TraceWriteOptions {
        compression: TraceCompression::None,
        max_payload_bytes: 32,
        payload_preview_bytes: 16,
        max_bytes_per_trace: 10_000_000,
        ..Default::default()
    };

    let temp_dir_full = create_temp_dir()?;
    let manifest_path_full = write_trace_bundle(&temp_dir_full, &trace, Some(options_full)).await?;
    let manifest_full = read_manifest(&manifest_path_full)?;
    let detail_full = manifest_full.detail.expect("detail should exist");
    assert_eq!(detail_full.status, "full");

    let chunk_total: u64 = detail_full
        .records
        .iter()
        .filter_map(|chunk| chunk.bytes)
        .sum::<u64>()
        .saturating_add(
            detail_full
                .nodes
                .iter()
                .filter_map(|chunk| chunk.bytes)
                .sum::<u64>(),
        )
        .saturating_add(
            detail_full
                .finalize
                .as_ref()
                .and_then(|c| c.bytes)
                .unwrap_or(0),
        );

    let trace_dir_full = trace_dir(&manifest_path_full);
    let blobs_dir = trace_dir_full.join("blobs");
    let mut blob_total = 0u64;
    if blobs_dir.exists() {
        for entry in fs::read_dir(&blobs_dir)? {
            let entry = entry?;
            blob_total = blob_total.saturating_add(entry.metadata()?.len());
        }
    }
    assert!(blob_total > 0, "expected blob files to be written");

    let options_budget = TraceWriteOptions {
        compression: TraceCompression::None,
        max_payload_bytes: 32,
        payload_preview_bytes: 16,
        max_bytes_per_trace: (chunk_total as usize) + 1,
        ..Default::default()
    };

    let temp_dir_budget = create_temp_dir()?;
    let manifest_path_budget =
        write_trace_bundle(&temp_dir_budget, &trace, Some(options_budget)).await?;
    let manifest_budget = read_manifest(&manifest_path_budget)?;
    let detail_budget = manifest_budget.detail.expect("detail should exist");
    assert_eq!(detail_budget.status, "basic");
    assert!(
        detail_budget
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_masks_preview_for_externalized_payloads() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-blob-mask-preview",
        "records": [
            {
                "index": 0,
                "input": {
                    "token": "secret",
                    "pad": "x".repeat(500)
                }
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        max_payload_bytes: 32,
        payload_preview_bytes: 200,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let loaded = load_trace(&temp_dir, "trace-blob-mask-preview").await?;
    let record = first_record_object(&loaded);
    let input = object_member(record, "input");
    let preview = input
        .get("preview")
        .and_then(|value| value.as_str())
        .expect("preview should exist");
    assert!(!preview.contains("secret"));

    let blob_ref = input
        .get("blob_ref")
        .and_then(|value| value.as_str())
        .expect("blob_ref should exist");
    let blob_path = trace_dir(&manifest_path).join(blob_ref);
    let blob_payload = fs::read_to_string(blob_path)?;
    let blob_json: serde_json::Value = serde_json::from_str(&blob_payload)?;
    let token = blob_json
        .get("token")
        .and_then(|value| value.as_str())
        .expect("token should exist");
    assert_eq!(token, "[masked]");

    Ok(())
}
