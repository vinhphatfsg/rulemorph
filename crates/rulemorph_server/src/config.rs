use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use rulemorph_endpoint::ApiMode;

use crate::server::UiSource;
use crate::tenant::TenantResolver;

#[derive(Clone)]
pub struct ServerConfig {
    pub port: u16,
    pub data_dir: PathBuf,
    pub ui_dir: Option<PathBuf>,
    pub rules_dir: Option<PathBuf>,
    pub api_mode: ApiMode,
    pub ui_enabled: bool,
    pub tenant_resolver: Option<Arc<dyn TenantResolver>>,
    pub internal_api_key: Option<String>,
    pub allow_unauth_internal: bool,
    pub rate_limit_per_sec: Option<u64>,
    pub ssrf_allowlist: Vec<String>,
    pub ssrf_allow_private: bool,
    pub ssrf_allow_any: bool,
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerConfig")
            .field("port", &self.port)
            .field("data_dir", &self.data_dir)
            .field("ui_dir", &self.ui_dir)
            .field("rules_dir", &self.rules_dir)
            .field("api_mode", &self.api_mode)
            .field("ui_enabled", &self.ui_enabled)
            .field("tenant_resolver", &self.tenant_resolver.is_some())
            .field("internal_api_key", &self.internal_api_key.is_some())
            .field("allow_unauth_internal", &self.allow_unauth_internal)
            .field("rate_limit_per_sec", &self.rate_limit_per_sec)
            .field("ssrf_allowlist", &self.ssrf_allowlist)
            .field("ssrf_allow_private", &self.ssrf_allow_private)
            .field("ssrf_allow_any", &self.ssrf_allow_any)
            .finish()
    }
}

impl ServerConfig {
    pub fn default_data_dir() -> PathBuf {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        cwd.join(".rulemorph")
    }

    pub fn default_ui_dir() -> PathBuf {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let release_path = cwd.join("ui").join("dist");
        if release_path.exists() {
            return release_path;
        }
        cwd.join("crates/rulemorph_ui/ui/dist")
    }

    pub fn default_rules_dir() -> PathBuf {
        let cwd = std::env::current_dir().unwrap_or_else(|_| Self::default_data_dir());
        cwd.join(".rulemorph").join("api_rules")
    }
}

pub(crate) fn resolve_ui_source(config: &ServerConfig) -> Result<UiSource> {
    if let Some(ui_dir) = config.ui_dir.clone() {
        if !ui_dir.exists() {
            anyhow::bail!("ui directory not found: {}", ui_dir.display());
        }
        return Ok(UiSource::Filesystem(ui_dir));
    }

    let default_dir = ServerConfig::default_ui_dir();
    if default_dir.exists() {
        return Ok(UiSource::Filesystem(default_dir));
    }

    #[cfg(feature = "embedded-ui")]
    {
        return Ok(UiSource::Embedded);
    }

    #[cfg(not(feature = "embedded-ui"))]
    {
        anyhow::bail!(
            "ui directory not found at {} and embedded UI is disabled",
            default_dir.display()
        );
    }
}

pub(crate) fn requires_ssrf_allowlist(
    api_mode: &ApiMode,
    tenant_auth_enabled: bool,
    ssrf_allowlist: &[String],
    ssrf_allow_any: bool,
) -> bool {
    tenant_auth_enabled
        && api_mode == &ApiMode::Rules
        && ssrf_allowlist.is_empty()
        && !ssrf_allow_any
}
