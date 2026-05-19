#[tokio::test]
async fn write_trace_bundle_generates_trace_id_when_missing() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(manifest.trace_id.starts_with("trace-"));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_generates_trace_id_when_empty() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(manifest.trace_id.starts_with("trace-"));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sanitizes_trace_id() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "../unsafe/trace",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(!manifest.trace_id.contains('/'));
    assert!(!manifest.trace_id.contains('\\'));
    assert!(manifest.trace_id.contains(".."));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sanitizes_trace_id_general_case() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "abc DEF/ghi あ",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert_eq!(manifest.trace_id, "abc_DEF_ghi__");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_rejects_dot_trace_id() -> anyhow::Result<()> {
    for raw in [".", ".."] {
        let temp_dir = create_temp_dir()?;

        let trace = json!({
            "trace_id": raw,
            "records": []
        });

        let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
        let manifest = read_manifest(&manifest_path)?;

        assert!(manifest.trace_id.starts_with("trace-"));
        assert_ne!(manifest.trace_id, raw);
    }

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_trace_trace_id_falls_back_to_generated() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(manifest.trace_id.starts_with("trace-"));
    assert_ne!(manifest.trace_id, "trace");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_preserves_trailing_underscore_trace_id() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace_",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert_eq!(manifest.trace_id, "trace_");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_avoids_trace_id_collision_on_sanitize() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace_a = json!({
        "trace_id": "a/b",
        "timestamp": "2026-02-01T00:00:00Z",
        "records": []
    });
    let trace_b = json!({
        "trace_id": "a_b",
        "timestamp": "2026-02-01T00:00:00Z",
        "records": []
    });

    let manifest_a_path = write_trace_bundle(&temp_dir, &trace_a, None).await?;
    let manifest_a = read_manifest(&manifest_a_path)?;

    let manifest_b_path = write_trace_bundle(&temp_dir, &trace_b, None).await?;
    let manifest_b = read_manifest(&manifest_b_path)?;

    assert_ne!(manifest_a.trace_id, manifest_b.trace_id);

    Ok(())
}
