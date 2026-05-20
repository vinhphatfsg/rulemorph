#[test]
fn endpoint_path_matches_and_captures() {
    let path = EndpointPath::parse("/api/traces/{id}").unwrap();
    assert!(path.matches("/api/traces/abc"));
    let params = path.capture("/api/traces/abc");
    assert_eq!(params.get("id"), Some(&"abc".to_string()));
}

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

#[test]
fn compile_retry_defaults_to_none() {
    let retry = compile_retry(None).unwrap();
    assert!(retry.is_none());
}

#[test]
fn rule_ref_from_path_avoids_double_rules_prefix() {
    let base_dir = PathBuf::from("/tmp/rules");
    let path = base_dir.join("rules").join("endpoint.yaml");
    let rule_ref = rule_ref_from_path(&base_dir, &path);
    assert_eq!(rule_ref, "rules/endpoint.yaml");
}

#[test]
fn eval_expr_string_rejects_non_string() {
    let expr = parse_v2_expr(&json!(123)).expect("parse expr");
    let input = json!({});
    let err = eval_expr_string(&expr, &input, None).expect_err("expected error");
    assert_eq!(err.kind, EndpointErrorKind::Invalid);
    assert!(err.message.contains("expected string"));
}

#[test]
fn compile_network_rule_rejects_zero_timeout() {
    let raw = NetworkRuleFile {
        version: 2,
        rule_type: "network".to_string(),
        request: NetworkRequest {
            method: "GET".to_string(),
            url: json!("https://example.com"),
            headers: None,
        },
        timeout: "0s".to_string(),
        internal_auth: false,
        select: None,
        body: None,
        body_map: None,
        body_rule: None,
        catch: None,
        retry: None,
    };
    let err = compile_network_rule(raw, Path::new("network.yaml")).expect_err("expected error");
    assert!(err.to_string().contains("timeout must be > 0"));
}
