#[tokio::test]
async fn trace_store_downgrades_on_invalid_utf8_chunk() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/04/trace-invalid-utf8");
    fs::create_dir_all(&trace_dir)?;

    fs::write(
        trace_dir.join("records-0001.ndjson"),
        vec![0xff, 0xfe, 0xfd],
    )?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-invalid-utf8",
        "records-0001.ndjson",
        "ndjson",
        "none",
        None,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-invalid-utf8")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_invalid_ndjson() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/04/trace-invalid-ndjson");
    fs::create_dir_all(&trace_dir)?;

    fs::write(trace_dir.join("records-0001.ndjson"), b"not_json\n")?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-invalid-ndjson",
        "records-0001.ndjson",
        "ndjson",
        "none",
        None,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-invalid-ndjson")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_unsupported_compression() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/05/trace-unsupported-compression");
    fs::create_dir_all(&trace_dir)?;

    fs::write(trace_dir.join("records-0001.ndjson"), b"0\n")?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-unsupported-compression",
        "records-0001.ndjson",
        "ndjson",
        "gzip",
        None,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-unsupported-compression")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_unsupported_format() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/06/trace-unsupported-format");
    fs::create_dir_all(&trace_dir)?;

    fs::write(trace_dir.join("records-0001.json"), b"0\n")?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-unsupported-format",
        "records-0001.json",
        "json",
        "none",
        None,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-unsupported-format")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");

    Ok(())
}
