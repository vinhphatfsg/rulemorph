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
