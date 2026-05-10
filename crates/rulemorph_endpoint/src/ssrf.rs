use std::net::{IpAddr, SocketAddr};

use tokio::net::lookup_host;
use url::{Host, Url};

#[derive(Debug)]
pub struct ResolvedSsrfTarget {
    pub host: String,
    pub addr: SocketAddr,
}

pub async fn resolve_ssrf_target(
    url: &str,
    allowlist: &[String],
    allow_private: bool,
    allow_private_hosts: &[String],
) -> Result<ResolvedSsrfTarget, String> {
    let parsed = Url::parse(url).map_err(|err| format!("invalid url: {err}"))?;
    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(format!("disallowed scheme: {scheme}"));
    }
    let host = parsed
        .host()
        .ok_or_else(|| "url must include host".to_string())?;
    match host {
        Host::Ipv4(addr) => {
            let raw_host = addr.to_string();
            let normalized = normalize_domain(&raw_host);
            let allow_private_match = allow_private_hosts
                .iter()
                .map(|entry| normalize_domain(entry))
                .any(|entry| host_equals(&normalized, &entry));
            if !allow_private_match {
                return Err("ip literal is not allowed".to_string());
            }
            let port = parsed
                .port_or_known_default()
                .ok_or_else(|| "url must include port".to_string())?;
            Ok(ResolvedSsrfTarget {
                host: raw_host,
                addr: SocketAddr::new(IpAddr::V4(addr), port),
            })
        }
        Host::Ipv6(addr) => {
            let raw_host = addr.to_string();
            let normalized = normalize_domain(&raw_host);
            let allow_private_match = allow_private_hosts
                .iter()
                .map(|entry| normalize_domain(entry))
                .any(|entry| host_equals(&normalized, &entry));
            if !allow_private_match {
                return Err("ip literal is not allowed".to_string());
            }
            let port = parsed
                .port_or_known_default()
                .ok_or_else(|| "url must include port".to_string())?;
            Ok(ResolvedSsrfTarget {
                host: raw_host,
                addr: SocketAddr::new(IpAddr::V6(addr), port),
            })
        }
        Host::Domain(domain) => {
            let raw_domain = domain.to_string();
            let domain = normalize_domain(&raw_domain);
            let allow_private_match = allow_private_hosts
                .iter()
                .map(|entry| normalize_domain(entry))
                .any(|entry| host_equals(&domain, &entry));
            if !allowlist.is_empty() {
                let allowed = allowlist
                    .iter()
                    .map(|entry| normalize_domain(entry))
                    .any(|entry| host_matches(&domain, &entry));
                if !allowed && !allow_private_match {
                    return Err("host not in allowlist".to_string());
                }
            }
            let allow_private_for_host = allow_private || allow_private_match;

            let port = parsed
                .port_or_known_default()
                .ok_or_else(|| "url must include port".to_string())?;
            let addr = resolve_target_addr(&raw_domain, port, allow_private_for_host).await?;
            Ok(ResolvedSsrfTarget {
                host: raw_domain,
                addr,
            })
        }
    }
}

fn normalize_domain(domain: &str) -> String {
    domain.trim().trim_end_matches('.').to_ascii_lowercase()
}

fn host_equals(host: &str, allow: &str) -> bool {
    if allow.is_empty() {
        return false;
    }
    host == allow
}

fn host_matches(host: &str, allow: &str) -> bool {
    if allow.is_empty() {
        return false;
    }
    if host == allow {
        return true;
    }
    host.ends_with(&format!(".{allow}"))
}

async fn resolve_target_addr(
    host: &str,
    port: u16,
    allow_private: bool,
) -> Result<SocketAddr, String> {
    let mut found = false;
    let addrs = lookup_host_addrs(host, port).await?;
    let mut selected_v4: Option<SocketAddr> = None;
    let mut selected_v6: Option<SocketAddr> = None;
    for addr in addrs {
        found = true;
        if !allow_private && is_private_ip(addr.ip()) {
            return Err("resolved ip is not allowed".to_string());
        }
        if addr.is_ipv4() {
            if selected_v4.is_none() {
                selected_v4 = Some(addr);
            }
        } else if selected_v6.is_none() {
            selected_v6 = Some(addr);
        }
    }
    if !found {
        return Err("dns lookup returned no addresses".to_string());
    }
    selected_v4
        .or(selected_v6)
        .ok_or_else(|| "dns lookup returned no addresses".to_string())
}

async fn lookup_host_addrs(host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
    #[cfg(test)]
    {
        // Unit tests should not depend on external DNS.
        if host == "example.com" || host == "www.example.com" {
            return Ok(vec![SocketAddr::new(
                IpAddr::V4(std::net::Ipv4Addr::new(93, 184, 216, 34)),
                port,
            )]);
        }
    }

    lookup_host((host, port))
        .await
        .map(|iter| iter.collect())
        .map_err(|err| format!("dns lookup failed: {err}"))
}

fn is_private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_private_v4(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4() {
                return is_private_v4(v4);
            }
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
                || v6.is_multicast()
        }
    }
}

fn is_private_v4(v4: std::net::Ipv4Addr) -> bool {
    if v4.is_private()
        || v4.is_loopback()
        || v4.is_link_local()
        || v4.is_broadcast()
        || v4.is_documentation()
        || v4.is_unspecified()
        || v4.is_multicast()
    {
        return true;
    }
    let octets = v4.octets();
    let first = octets[0];
    let second = octets[1];
    match first {
        100 if (64..=127).contains(&second) => true, // 100.64.0.0/10 CGNAT
        198 if (18..=19).contains(&second) => true,  // 198.18.0.0/15 benchmarking
        240..=255 => true,                           // 240.0.0.0/4 reserved + broadcast
        192 if second == 0 && octets[2] == 0 => true, // 192.0.0.0/24 (IETF)
        _ => false,
    }
}

#[cfg(test)]
mod tests {
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
        let err =
            tokio_test::block_on(resolve_ssrf_target("http://127.0.0.1/meta", &[], true, &[]))
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
        let err = tokio_test::block_on(resolve_ssrf_target("http://localhost", &[], false, &[]))
            .unwrap_err();
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
        assert!(super::host_equals("localhost", "localhost"));
        assert!(!super::host_equals("api.example.com", "example.com"));
    }

    #[test]
    fn treats_ipv4_mapped_ipv6_as_private() {
        let ip = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x7f00, 0x0001));
        assert!(super::is_private_ip(ip));
    }

    #[test]
    fn treats_cgnat_as_private() {
        let ip = IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1));
        assert!(super::is_private_ip(ip));
    }

    #[test]
    fn treats_benchmarking_as_private() {
        let ip = IpAddr::V4(Ipv4Addr::new(198, 18, 0, 1));
        assert!(super::is_private_ip(ip));
    }

    #[test]
    fn treats_reserved_high_range_as_private() {
        let ip = IpAddr::V4(Ipv4Addr::new(240, 0, 0, 1));
        assert!(super::is_private_ip(ip));
    }

    #[test]
    fn allows_public_ipv4() {
        let ip = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
        assert!(!super::is_private_ip(ip));
    }
}
