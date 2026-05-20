fn build_zip_import_payload(trace_id: &str) -> Result<(String, Vec<u8>)> {
    let trace = json!({
        "trace_schema_version": 1,
        "trace_id": trace_id,
        "timestamp": "2026-02-03T00:00:00Z",
        "status": "ok",
        "summary": { "record_total": 1, "record_success": 1, "record_failed": 0 }
    });
    let trace_payload = serde_json::to_vec(&trace)?;

    let mut zip_writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = FileOptions::default().compression_method(CompressionMethod::Stored);
    zip_writer.start_file(format!("traces/2026/02/03/{trace_id}/trace.json"), options)?;
    zip_writer.write_all(&trace_payload)?;
    let zip_cursor = zip_writer.finish()?;
    let zip_bytes = zip_cursor.into_inner();

    let boundary = "BOUNDARY".to_string();
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"bundle\"; filename=\"bundle.zip\"\r\n",
    );
    body.extend_from_slice(b"Content-Type: application/zip\r\n\r\n");
    body.extend_from_slice(&zip_bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    Ok((boundary, body))
}
