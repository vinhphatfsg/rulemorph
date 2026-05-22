use std::path::{Path, PathBuf};

mod endpoint;
mod network;
mod normal;

#[cfg(test)]
pub(super) use endpoint::{EndpointDef, EndpointStep};
pub(super) use endpoint::{EndpointRuleFile, endpoint_ops};
#[cfg(test)]
pub(super) use network::NetworkRequest;
pub(super) use network::{NetworkRuleFile, network_ops};
pub(super) use normal::normal_ops;

pub(super) fn resolve_rule_path(base_dir: &Path, rule: &str) -> PathBuf {
    let path = PathBuf::from(rule);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}
