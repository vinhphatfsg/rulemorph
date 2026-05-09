mod api_graph;
mod api_keys;
mod server;
mod tenant;

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
pub use rulemorph_endpoint::{ApiMode, RulesDirError, RulesDirErrors, validate_rules_dir};
use rulemorph_endpoint::{EndpointEngine, EngineConfig};
use rulemorph_trace::{TraceStore, start_trace_watcher};
use tokio::sync::broadcast;

pub use api_keys::{
    ApiKeyInfo, ApiKeyIssueResult, ApiKeyRecord, ApiKeyResolver, ApiKeyStore, ParsedApiKey,
    parse_api_key,
};
pub use server::{AppState, RateLimiter, TenantRegistry, TenantResources, UiSource, build_router};
pub use tenant::{TenantContext, TenantLayout, TenantResolver, validate_tenant_id};

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

pub async fn run(config: ServerConfig) -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    if !config.ui_enabled && config.api_mode == ApiMode::UiOnly {
        anyhow::bail!("ui-only mode cannot be used with UI disabled");
    }
    if requires_ssrf_allowlist(
        &config.api_mode,
        config.tenant_resolver.is_some(),
        &config.ssrf_allowlist,
        config.ssrf_allow_any,
    ) {
        anyhow::bail!(
            "ssrf allowlist required when api key auth is enabled; use --ssrf-allowlist or --ssrf-allow-any"
        );
    }

    let ui_source = if config.ui_enabled {
        Some(resolve_ui_source(&config)?)
    } else {
        None
    };

    let (default_resources, tenant_registry) = if config.tenant_resolver.is_some() {
        let registry = Arc::new(TenantRegistry::new(
            config.data_dir.clone(),
            config.rules_dir.clone(),
            config.api_mode,
            config.ui_enabled,
            config.port,
            config.ssrf_allowlist.clone(),
            config.ssrf_allow_private,
            config.internal_api_key.clone(),
        ));
        let default_resources = init_default_resources(&config, false)
            .await
            .context("failed to init bootstrap resources")?;
        (default_resources, Some(registry))
    } else {
        let resources = init_default_resources(&config, true).await?;
        (resources, None)
    };

    let state = AppState {
        default_resources,
        tenant_registry,
        ui_source,
        api_mode: config.api_mode,
        tenant_resolver: config.tenant_resolver.clone(),
        internal_api_key: config.internal_api_key.clone(),
        allow_unauth_internal: config.allow_unauth_internal,
        rate_limiter: config
            .rate_limit_per_sec
            .filter(|limit| *limit > 0)
            .map(|limit| Arc::new(RateLimiter::new(limit))),
    };

    let app = build_router(state, config.ui_enabled);
    let addr = SocketAddr::from(([127, 0, 0, 1], config.port));
    tracing::info!("rulemorph server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind port")?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .context("server error")?;
    Ok(())
}

async fn init_default_resources(
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

fn resolve_ui_source(config: &ServerConfig) -> Result<UiSource> {
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

fn requires_ssrf_allowlist(
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::{ApiMode, ServerConfig, init_default_resources, requires_ssrf_allowlist};

    #[test]
    fn ssrf_allowlist_required_for_rules_with_tenant_auth() {
        let required = requires_ssrf_allowlist(&ApiMode::Rules, true, &[], false);
        assert!(required);
    }

    #[test]
    fn ssrf_allowlist_not_required_for_ui_only() {
        let required = requires_ssrf_allowlist(&ApiMode::UiOnly, true, &[], false);
        assert!(!required);
    }

    #[test]
    fn ssrf_allowlist_not_required_when_allow_any_enabled() {
        let required = requires_ssrf_allowlist(&ApiMode::Rules, true, &[], true);
        assert!(!required);
    }

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
    impl super::TenantResolver for NoopTenantResolver {
        async fn resolve(&self, _api_key: &str) -> anyhow::Result<Option<super::TenantContext>> {
            Ok(None)
        }
    }
}
