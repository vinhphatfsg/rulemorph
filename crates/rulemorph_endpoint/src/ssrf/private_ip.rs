use std::net::{IpAddr, Ipv4Addr};

pub(super) fn is_private_ip(ip: IpAddr) -> bool {
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

fn is_private_v4(v4: Ipv4Addr) -> bool {
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
