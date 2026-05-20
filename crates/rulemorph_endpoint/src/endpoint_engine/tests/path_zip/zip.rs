#[test]
fn zip_copy_stops_after_file_limit() {
    let mut input = std::io::Cursor::new(vec![b'x'; 12]);
    let mut output = Vec::new();
    let copied = copy_zip_entry_bounded(&mut input, &mut output, 8).expect("copy");
    assert_eq!(copied, 12);
    assert!(output.len() <= 8);
}

#[test]
fn zip_extract_rejects_too_many_entries() {
    let temp = tempfile::tempdir().expect("tempdir");
    let zip_path = temp.path().join("bundle.zip");
    let file = File::create(&zip_path).expect("create zip");
    let mut zip_writer = zip::ZipWriter::new(file);
    let options =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for index in 0..=MULTIPART_IMPORT_MAX_ENTRIES {
        zip_writer
            .start_file(format!("rules/{index}.yaml"), options)
            .expect("start file");
    }
    zip_writer.finish().expect("finish zip");

    let err = extract_zip(&zip_path, temp.path().join("out").as_path())
        .expect_err("zip should be rejected");
    assert!(err.contains("too many entries"));
}
