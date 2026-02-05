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
    if config.tenant_resolver.is_some()
        && config.ssrf_allowlist.is_empty()
        && !config.ssrf_allow_any
    {
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
        let default_resources = registry
            .get_or_init("default")
            .await
            .context("failed to init default tenant")?;
        (default_resources, Some(registry))
    } else {
        let data_dir = config.data_dir.clone();
        let auth_dir = data_dir.join("auth");
        tokio::fs::create_dir_all(&auth_dir)
            .await
            .context("failed to create auth dir")?;
        let store = TraceStore::new(data_dir.clone())
            .await
            .context("failed to init trace store")?;
        let (trace_events, _) = broadcast::channel(64);
        if config.ui_enabled {
            start_trace_watcher(data_dir.clone(), trace_events.clone());
        }
        let api_engine = match config.api_mode {
            ApiMode::UiOnly => None,
            ApiMode::Rules => {
                let rules_dir = config
                    .rules_dir
                    .clone()
                    .unwrap_or_else(ServerConfig::default_rules_dir);
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
                Some(Arc::new(EndpointEngine::load(rules_dir, engine_config)?))
            }
        };
        let resources = TenantResources {
            tenant_id: "default".to_string(),
            data_dir,
            rules_dir: config
                .rules_dir
                .clone()
                .unwrap_or_else(ServerConfig::default_rules_dir),
            auth_dir,
            store: Arc::new(store),
            api_engine,
            trace_events,
        };
        (Arc::new(resources), None)
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
