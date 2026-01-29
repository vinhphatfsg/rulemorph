mod endpoint_engine;
mod api_graph;
mod server;
mod trace_store;
mod trace_watch;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use endpoint_engine::{EndpointEngine, EngineConfig};
pub use endpoint_engine::{validate_rules_dir, ApiMode, RulesDirError, RulesDirErrors};
use server::{build_router, AppState};
use trace_store::TraceStore;
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct UiConfig {
    pub port: u16,
    pub data_dir: PathBuf,
    pub ui_dir: PathBuf,
    pub rules_dir: Option<PathBuf>,
    pub api_mode: ApiMode,
    pub ui_enabled: bool,
}

impl UiConfig {
    pub fn default_data_dir() -> PathBuf {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        cwd.join(".rulemorph")
    }

    pub fn default_ui_dir() -> PathBuf {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        cwd.join("crates/rulemorph_ui/ui/dist")
    }

    pub fn default_rules_dir() -> PathBuf {
        let cwd = std::env::current_dir().unwrap_or_else(|_| Self::default_data_dir());
        cwd.join(".rulemorph").join("api_rules")
    }
}

pub async fn run(config: UiConfig) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    if !config.ui_enabled && config.api_mode == ApiMode::UiOnly {
        anyhow::bail!("ui-only mode cannot be used with UI disabled");
    }

    let store = TraceStore::new(config.data_dir.clone())
        .await
        .context("failed to init trace store")?;
    let (trace_events, _) = broadcast::channel(64);
    if config.ui_enabled {
        trace_watch::start_trace_watcher(config.data_dir.clone(), trace_events.clone());
    }
    let api_engine = match config.api_mode {
        ApiMode::UiOnly => None,
        ApiMode::Rules => {
            let rules_dir = config.rules_dir.unwrap_or_else(UiConfig::default_rules_dir);
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
    let state = AppState {
        store: Arc::new(store),
        ui_dir: config.ui_dir.clone(),
        api_mode: config.api_mode,
        api_engine: api_engine.map(Arc::new),
        trace_events,
    };

    let app = build_router(state, config.ui_enabled);
    let addr = SocketAddr::from(([127, 0, 0, 1], config.port));
    tracing::info!("rulemorph ui listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind port")?;
    axum::serve(listener, app).await.context("server error")?;
    Ok(())
}
