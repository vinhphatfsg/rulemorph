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
