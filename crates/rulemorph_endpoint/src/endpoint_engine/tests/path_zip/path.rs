#[test]
fn endpoint_path_matches_and_captures() {
    let path = EndpointPath::parse("/api/traces/{id}").unwrap();
    assert!(path.matches("/api/traces/abc"));
    let params = path.capture("/api/traces/abc");
    assert_eq!(params.get("id"), Some(&"abc".to_string()));
}
