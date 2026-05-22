use std::sync::Arc;

use anyhow::{Context, Result};
use rulemorph_endpoint::{ApiMode, EndpointEngine, EngineConfig, validate_rules_dir};
use rulemorph_trace::{TraceStore, start_trace_watcher};
use tokio::sync::broadcast;

use crate::ServerConfig;
use crate::server::{self, TenantResources};

pub(crate) async fn init_default_resources(
    config: &ServerConfig,
    validate_rules: bool,
) -> Result<Arc<TenantResources>> {
    let data_dir = config.data_dir.clone();
    let auth_dir = data_dir.join("auth");
    tokio::fs::create_dir_all(&auth_dir)
        .await
        .context("failed to create auth dir")?;
    let store = TraceStore::new(data_dir.clone())
        .await
        .context("failed to init trace store")?;
    let (trace_events, _) = broadcast::channel(64);
    if config.ui_enabled && validate_rules {
        start_trace_watcher(data_dir.clone(), trace_events.clone());
    }

    let rules_dir = config
        .rules_dir
        .clone()
        .unwrap_or_else(ServerConfig::default_rules_dir);
    let api_engine = if validate_rules && config.api_mode == ApiMode::Rules {
        if let Err(errs) = validate_rules_dir(&rules_dir) {
            return Err(errs.into());
        }
        let internal_base = format!("http://localhost:{}", config.port);
        let mut engine_config = EngineConfig::new(internal_base, data_dir.clone())
            .with_ssrf_allowlist(config.ssrf_allowlist.clone())
            .with_ssrf_allow_private(config.ssrf_allow_private)
            .with_internal_auth_enabled(true)
            .with_internal_auth_path_allowlist(server::internal_auth_path_allowlist());
        if let Some(internal_api_key) = config.internal_api_key.clone() {
            engine_config = engine_config.with_internal_api_key(internal_api_key);
        }
        Some(Arc::new(EndpointEngine::load(
            rules_dir.clone(),
            engine_config,
        )?))
    } else {
        None
    };

    Ok(Arc::new(TenantResources {
        tenant_id: "default".to_string(),
        data_dir,
        rules_dir,
        auth_dir,
        store: Arc::new(store),
        api_engine,
        trace_events,
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::init_default_resources;
    use crate::{ApiMode, ServerConfig, TenantContext, TenantResolver};

    #[tokio::test]
    async fn bootstrap_resources_do_not_require_default_tenant_rules() {
        let temp = tempdir().expect("tempdir");
        let tenant_rules_dir = temp.path().join("tenants/tenant-1/api_rules");
        tokio::fs::create_dir_all(&tenant_rules_dir)
            .await
            .expect("create tenant rules dir");

        let config = ServerConfig {
            port: 8080,
            data_dir: temp.path().to_path_buf(),
            ui_dir: None,
            rules_dir: None,
            api_mode: ApiMode::Rules,
            ui_enabled: false,
            tenant_resolver: Some(Arc::new(NoopTenantResolver)),
            internal_api_key: None,
            allow_unauth_internal: false,
            rate_limit_per_sec: None,
            ssrf_allowlist: vec!["localhost".to_string()],
            ssrf_allow_private: false,
            ssrf_allow_any: false,
        };

        let resources = init_default_resources(&config, false)
            .await
            .expect("bootstrap resources");
        assert!(resources.api_engine.is_none());
    }

    #[tokio::test]
    async fn default_resources_validate_rules_when_requested() {
        let temp = tempdir().expect("tempdir");
        let config = ServerConfig {
            port: 8080,
            data_dir: temp.path().to_path_buf(),
            ui_dir: None,
            rules_dir: Some(temp.path().join("missing-api-rules")),
            api_mode: ApiMode::Rules,
            ui_enabled: false,
            tenant_resolver: None,
            internal_api_key: None,
            allow_unauth_internal: false,
            rate_limit_per_sec: None,
            ssrf_allowlist: vec!["localhost".to_string()],
            ssrf_allow_private: false,
            ssrf_allow_any: false,
        };
        let err = match init_default_resources(&config, true).await {
            Ok(_) => panic!("validate rules should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("missing-api-rules"),
            "unexpected error: {err}"
        );
    }

    struct NoopTenantResolver;

    #[async_trait::async_trait]
    impl TenantResolver for NoopTenantResolver {
        async fn resolve(&self, _api_key: &str) -> anyhow::Result<Option<TenantContext>> {
            Ok(None)
        }
    }
}
