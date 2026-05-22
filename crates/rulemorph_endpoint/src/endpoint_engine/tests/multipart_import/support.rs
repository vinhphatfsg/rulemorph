fn build_multipart_zip_body() -> (String, Vec<u8>) {
    let mut zip_writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    zip_writer
        .start_file("rules/ok.yaml", options)
        .expect("start file");
    zip_writer
        .write_all(
            br#"
version: 2
input:
  format: json
  json: {}
mappings: []
"#,
        )
        .expect("write file");
    let zip_bytes = zip_writer.finish().expect("finish zip").into_inner();
    let boundary = "BOUNDARY".to_string();
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"bundle\"; filename=\"bundle.zip\"\r\n",
    );
    body.extend_from_slice(b"Content-Type: application/zip\r\n\r\n");
    body.extend_from_slice(&zip_bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    (boundary, body)
}
