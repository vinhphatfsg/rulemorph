use std::net::{IpAddr, SocketAddr};

use tokio::net::lookup_host;
use url::{Host, Url};

mod hosts;
mod private_ip;

use hosts::{host_equals, host_matches, normalize_domain};
use private_ip::is_private_ip;

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

#[cfg(test)]
mod tests;
