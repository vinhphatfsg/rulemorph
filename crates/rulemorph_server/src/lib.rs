mod api_graph;
mod api_keys;
mod bootstrap;
mod config;
mod server;
mod tenant;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
pub use rulemorph_endpoint::{ApiMode, RulesDirError, RulesDirErrors, validate_rules_dir};

pub use api_keys::{
    ApiKeyInfo, ApiKeyIssueResult, ApiKeyRecord, ApiKeyResolver, ApiKeyStore, ParsedApiKey,
    parse_api_key,
};
use bootstrap::init_default_resources;
pub use config::ServerConfig;
use config::{requires_ssrf_allowlist, resolve_ui_source};
pub use server::{AppState, RateLimiter, TenantRegistry, TenantResources, UiSource, build_router};
pub use tenant::{TenantContext, TenantLayout, TenantResolver, validate_tenant_id};

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

#[cfg(test)]
mod tests {
    use super::{ApiMode, requires_ssrf_allowlist};

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
}
