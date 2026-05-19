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
