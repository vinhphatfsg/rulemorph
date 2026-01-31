mod api_graph;
mod server;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
pub use rulemorph_endpoint::{ApiMode, RulesDirError, RulesDirErrors, validate_rules_dir};
use rulemorph_endpoint::{EndpointEngine, EngineConfig};
use rulemorph_trace::{TraceStore, start_trace_watcher};
use tokio::sync::broadcast;

use server::{AppState, UiSource, build_router};

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub port: u16,
    pub data_dir: PathBuf,
    pub ui_dir: Option<PathBuf>,
    pub rules_dir: Option<PathBuf>,
    pub api_mode: ApiMode,
    pub ui_enabled: bool,
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

    let store = TraceStore::new(config.data_dir.clone())
        .await
        .context("failed to init trace store")?;
    let (trace_events, _) = broadcast::channel(64);
    if config.ui_enabled {
        start_trace_watcher(config.data_dir.clone(), trace_events.clone());
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
            let internal_base = format!("http://127.0.0.1:{}", config.port);
            Some(EndpointEngine::load(
                rules_dir,
                EngineConfig::new(internal_base, config.data_dir.clone()),
            )?)
        }
    };
    let ui_source = if config.ui_enabled {
        Some(resolve_ui_source(&config)?)
    } else {
        None
    };

    let state = AppState {
        store: Arc::new(store),
        ui_source,
        api_mode: config.api_mode,
        api_engine: api_engine.map(Arc::new),
        trace_events,
    };

    let app = build_router(state, config.ui_enabled);
    let addr = SocketAddr::from(([127, 0, 0, 1], config.port));
    tracing::info!("rulemorph server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind port")?;
    axum::serve(listener, app).await.context("server error")?;
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
