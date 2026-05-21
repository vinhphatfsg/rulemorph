use std::path::PathBuf;

use rulemorph_server::{ServerConfig, TenantLayout};

pub(crate) fn resolve_tenant_layout(
    tenant_id: &str,
    data_dir: Option<PathBuf>,
) -> Result<TenantLayout, String> {
    let base_dir = data_dir.unwrap_or_else(ServerConfig::default_data_dir);
    TenantLayout::new(base_dir, tenant_id).map_err(|err| err.to_string())
}
