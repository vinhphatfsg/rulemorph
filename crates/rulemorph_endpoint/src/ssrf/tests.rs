use super::resolve_ssrf_target;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[test]
fn allows_http_and_https_without_allowlist() {
    let result = tokio_test::block_on(resolve_ssrf_target(
        "https://example.com/path",
        &[],
        true,
        &[],
    ));
    assert!(result.is_ok());
    let result = tokio_test::block_on(resolve_ssrf_target(
        "http://example.com/path",
        &[],
        true,
        &[],
    ));
    assert!(result.is_ok());
}

#[test]
fn rejects_non_http_scheme() {
    let err = tokio_test::block_on(resolve_ssrf_target("file:///etc/passwd", &[], true, &[]))
        .unwrap_err();
    assert!(err.contains("disallowed scheme"));
}

#[test]
fn rejects_ip_literals() {
    let err = tokio_test::block_on(resolve_ssrf_target("http://127.0.0.1/meta", &[], true, &[]))
        .unwrap_err();
    assert!(err.contains("ip literal"));
}

#[test]
fn allows_ip_literal_with_private_allowlist() {
    let result = tokio_test::block_on(resolve_ssrf_target(
        "http://127.0.0.1/meta",
        &[],
        false,
        &["127.0.0.1".to_string()],
    ));
    assert!(result.is_ok());
}

#[test]
fn allowlist_accepts_subdomains() {
    let allowlist = vec!["example.com".to_string()];
    let result = tokio_test::block_on(resolve_ssrf_target(
        "https://www.example.com",
        &allowlist,
        true,
        &[],
    ));
    assert!(result.is_ok());
}

#[test]
fn allowlist_rejects_other_hosts() {
    let allowlist = vec!["example.com".to_string()];
    let err = tokio_test::block_on(resolve_ssrf_target(
        "https://evil.com",
        &allowlist,
        true,
        &[],
    ))
    .unwrap_err();
    assert!(err.contains("allowlist"));
}

#[test]
fn rejects_private_ips_when_disallowed() {
    let err =
        tokio_test::block_on(resolve_ssrf_target("http://localhost", &[], false, &[])).unwrap_err();
    assert!(err.contains("resolved ip"));
}

#[test]
fn allows_private_ips_when_enabled() {
    let result = tokio_test::block_on(resolve_ssrf_target("http://localhost", &[], true, &[]));
    assert!(result.is_ok());
}

#[test]
fn allowlist_can_bypass_private_restriction() {
    let result = tokio_test::block_on(resolve_ssrf_target(
        "http://localhost",
        &[],
        false,
        &["localhost".to_string()],
    ));
    assert!(result.is_ok());
}

#[test]
fn allowlist_bypasses_host_check_for_internal_targets() {
    let allowlist = vec!["example.com".to_string()];
    let result = tokio_test::block_on(resolve_ssrf_target(
        "http://localhost",
        &allowlist,
        false,
        &["localhost".to_string()],
    ));
    assert!(result.is_ok());
}

#[test]
fn private_allowlist_requires_exact_match() {
    assert!(super::hosts::host_equals("localhost", "localhost"));
    assert!(!super::hosts::host_equals("api.example.com", "example.com"));
}

#[test]
fn treats_ipv4_mapped_ipv6_as_private() {
    let ip = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x7f00, 0x0001));
    assert!(super::private_ip::is_private_ip(ip));
}

#[test]
fn treats_cgnat_as_private() {
    let ip = IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1));
    assert!(super::private_ip::is_private_ip(ip));
}

#[test]
fn treats_benchmarking_as_private() {
    let ip = IpAddr::V4(Ipv4Addr::new(198, 18, 0, 1));
    assert!(super::private_ip::is_private_ip(ip));
}

#[test]
fn treats_reserved_high_range_as_private() {
    let ip = IpAddr::V4(Ipv4Addr::new(240, 0, 0, 1));
    assert!(super::private_ip::is_private_ip(ip));
}

#[test]
fn allows_public_ipv4() {
    let ip = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
    assert!(!super::private_ip::is_private_ip(ip));
}
