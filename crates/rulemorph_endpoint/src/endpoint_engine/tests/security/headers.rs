#[test]
fn build_headers_rejects_host_header() {
    let mut headers = HashMap::new();
    let expr = parse_v2_expr(&json!("example.com")).expect("parse expr");
    headers.insert("Host".to_string(), expr);
    let err = build_headers(&headers, &json!({}), None).expect_err("expected error");
    assert_eq!(err.kind, EndpointErrorKind::Invalid);
    assert!(err.message.contains("disallowed header"));
}
