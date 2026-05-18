#[test]
fn internal_hosts_match_loopback_aliases() {
    assert!(internal_hosts_match(Some("127.0.0.1"), Some("localhost")));
    assert!(internal_hosts_match(Some("localhost"), Some("::1")));
    assert!(!internal_hosts_match(
        Some("127.0.0.1"),
        Some("example.com")
    ));
}

#[test]
fn engine_config_allows_loopback_aliases_for_internal_base() {
    let config = EngineConfig::new(
        "http://localhost:8080".to_string(),
        std::path::PathBuf::from(".data"),
    );

    assert!(
        config
            .ssrf_private_allowlist
            .contains(&"localhost".to_string())
    );
    assert!(
        config
            .ssrf_private_allowlist
            .contains(&"127.0.0.1".to_string())
    );
    assert!(config.ssrf_private_allowlist.contains(&"::1".to_string()));
}

#[test]
fn build_headers_rejects_host_header() {
    let mut headers = HashMap::new();
    let expr = parse_v2_expr(&json!("example.com")).expect("parse expr");
    headers.insert("Host".to_string(), expr);
    let err = build_headers(&headers, &json!({}), None).expect_err("expected error");
    assert_eq!(err.kind, EndpointErrorKind::Invalid);
    assert!(err.message.contains("disallowed header"));
}

#[test]
fn ssrf_audit_log_populates_fields() {
    let rule = CompiledNetworkRule {
        request: CompiledNetworkRequest {
            method: Method::GET,
            url: parse_v2_expr(&json!("https://example.com")).expect("parse url"),
            headers: HashMap::new(),
        },
        timeout: std::time::Duration::from_secs(1),
        select: None,
        body: None,
        body_map: None,
        body_rule: None,
        body_rule_ref: None,
        rule_ref: Some("rules/network.yaml".to_string()),
        catch: None,
        retry: None,
        internal_auth: false,
        base_dir: PathBuf::from("."),
    };
    let context = RequestContext {
        tenant_id: Some("tenant-1".to_string()),
        internal_api_key: None,
    };
    let log = build_ssrf_audit_log(&rule, "https://example.com", "blocked", Some(&context));
    assert_eq!(log.tenant_id, "tenant-1");
    assert_eq!(log.rule_ref, "rules/network.yaml");
    assert_eq!(log.method, Method::GET);
    assert_eq!(log.url, "https://example.com/");
    assert_eq!(log.reason, "blocked");
}

#[test]
fn ssrf_audit_log_defaults_to_unknown() {
    let rule = CompiledNetworkRule {
        request: CompiledNetworkRequest {
            method: Method::POST,
            url: parse_v2_expr(&json!("https://example.com")).expect("parse url"),
            headers: HashMap::new(),
        },
        timeout: std::time::Duration::from_secs(1),
        select: None,
        body: None,
        body_map: None,
        body_rule: None,
        body_rule_ref: None,
        rule_ref: None,
        catch: None,
        retry: None,
        internal_auth: false,
        base_dir: PathBuf::from("."),
    };
    let log = build_ssrf_audit_log(&rule, "https://example.com", "blocked", None);
    assert_eq!(log.tenant_id, "unknown");
    assert_eq!(log.rule_ref, "unknown");
    assert_eq!(log.method, Method::POST);
}

#[test]
fn ssrf_audit_log_redacts_query() {
    let rule = CompiledNetworkRule {
        request: CompiledNetworkRequest {
            method: Method::GET,
            url: parse_v2_expr(&json!("https://example.com")).expect("parse url"),
            headers: HashMap::new(),
        },
        timeout: std::time::Duration::from_secs(1),
        select: None,
        body: None,
        body_map: None,
        body_rule: None,
        body_rule_ref: None,
        rule_ref: None,
        catch: None,
        retry: None,
        internal_auth: false,
        base_dir: PathBuf::from("."),
    };
    let log = build_ssrf_audit_log(&rule, "https://example.com/path?token=abc", "blocked", None);
    assert_eq!(log.url, "https://example.com/path");
}
