use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{Mutex, OnceCell, broadcast};

use crate::{TenantLayout, validate_tenant_id};
use rulemorph_endpoint::{ApiMode, EndpointEngine, EngineConfig, validate_rules_dir};
use rulemorph_trace::{TraceStore, start_trace_watcher};

#[derive(Clone)]
pub struct TenantResources {
    pub tenant_id: String,
    pub data_dir: PathBuf,
    pub rules_dir: PathBuf,
    pub auth_dir: PathBuf,
    pub store: Arc<TraceStore>,
    pub api_engine: Option<Arc<EndpointEngine>>,
    pub trace_events: broadcast::Sender<()>,
}

pub(crate) fn internal_auth_path_allowlist() -> Vec<String> {
    vec![
        "/internal/traces".to_string(),
        "/internal/traces/".to_string(),
        "/internal/api-graph".to_string(),
        "/internal/import".to_string(),
        "/internal/stream".to_string(),
    ]
}

pub struct TenantRegistry {
    base_dir: PathBuf,
    rules_dir: Option<PathBuf>,
    api_mode: ApiMode,
    ui_enabled: bool,
    port: u16,
    ssrf_allowlist: Vec<String>,
    ssrf_allow_private: bool,
    internal_api_key: Option<String>,
    tenants: Mutex<HashMap<String, Arc<OnceCell<Arc<TenantResources>>>>>,
}

pub struct TenantRegistryConfig {
    pub base_dir: PathBuf,
    pub rules_dir: Option<PathBuf>,
    pub api_mode: ApiMode,
    pub ui_enabled: bool,
    pub port: u16,
    pub ssrf_allowlist: Vec<String>,
    pub ssrf_allow_private: bool,
    pub internal_api_key: Option<String>,
}

impl TenantRegistry {
    pub fn new(config: TenantRegistryConfig) -> Self {
        Self {
            base_dir: config.base_dir,
            rules_dir: config.rules_dir,
            api_mode: config.api_mode,
            ui_enabled: config.ui_enabled,
            port: config.port,
            ssrf_allowlist: config.ssrf_allowlist,
            ssrf_allow_private: config.ssrf_allow_private,
            internal_api_key: config.internal_api_key,
            tenants: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_or_init(&self, tenant_id: &str) -> anyhow::Result<Arc<TenantResources>> {
        validate_tenant_id(tenant_id)?;
        let cell = {
            let mut guard = self.tenants.lock().await;
            guard
                .entry(tenant_id.to_string())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let resources = cell
            .get_or_try_init(|| async { self.init_resources(tenant_id).await })
            .await?;
        Ok(resources.clone())
    }

    async fn init_resources(&self, tenant_id: &str) -> anyhow::Result<Arc<TenantResources>> {
        let layout = TenantLayout::new(self.base_dir.clone(), tenant_id)?;
        tokio::fs::create_dir_all(layout.api_rules_dir()).await?;
        tokio::fs::create_dir_all(layout.auth_dir()).await?;

        let store = TraceStore::new(layout.data_dir()).await?;
        let (trace_events, _) = broadcast::channel(64);
        if self.ui_enabled {
            start_trace_watcher(layout.data_dir(), trace_events.clone());
        }

        let rules_dir = self.resolve_rules_dir(&layout);
        let api_engine = match self.api_mode {
            ApiMode::UiOnly => None,
            ApiMode::Rules => {
                if let Err(errs) = validate_rules_dir(&rules_dir) {
                    return Err(errs.into());
                }
                let internal_base = format!("http://localhost:{}", self.port);
                let mut config = EngineConfig::new(internal_base, layout.data_dir())
                    .with_ssrf_allowlist(self.ssrf_allowlist.clone())
                    .with_ssrf_allow_private(self.ssrf_allow_private)
                    .with_internal_auth_enabled(true)
                    .with_internal_auth_path_allowlist(internal_auth_path_allowlist());
                if let Some(internal_api_key) = self.internal_api_key.clone() {
                    config = config.with_internal_api_key(internal_api_key);
                }
                Some(Arc::new(EndpointEngine::load(rules_dir.clone(), config)?))
            }
        };

        Ok(Arc::new(TenantResources {
            tenant_id: tenant_id.to_string(),
            data_dir: layout.data_dir(),
            rules_dir,
            auth_dir: layout.auth_dir(),
            store: Arc::new(store),
            api_engine,
            trace_events,
        }))
    }

    fn resolve_rules_dir(&self, layout: &TenantLayout) -> PathBuf {
        match &self.rules_dir {
            Some(path) if path.is_absolute() => path.clone(),
            Some(path) => path.clone(),
            None => layout.api_rules_dir(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{ApiMode, TenantLayout, TenantRegistry, TenantRegistryConfig};

    #[test]
    fn tenant_registry_keeps_configured_relative_rules_dir_relative_to_cwd() {
        let registry = TenantRegistry::new(TenantRegistryConfig {
            base_dir: PathBuf::from("/tmp/rulemorph-data"),
            rules_dir: Some(PathBuf::from("./assets/api_rules")),
            api_mode: ApiMode::Rules,
            ui_enabled: false,
            port: 8080,
            ssrf_allowlist: Vec::new(),
            ssrf_allow_private: false,
            internal_api_key: None,
        });
        let layout = TenantLayout::new(PathBuf::from("/tmp/rulemorph-data"), "tenant-a")
            .expect("tenant layout");

        assert_eq!(
            registry.resolve_rules_dir(&layout),
            PathBuf::from("./assets/api_rules")
        );
    }
}
