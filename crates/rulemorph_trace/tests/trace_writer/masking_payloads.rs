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

include!("masking_payloads/rule_source.rs");
include!("masking_payloads/url.rs");
include!("masking_payloads/externalized.rs");
