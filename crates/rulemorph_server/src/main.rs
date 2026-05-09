use std::path::PathBuf;

use async_trait::async_trait;
use clap::{ArgAction, Parser, ValueEnum};
use rulemorph_server::{
    ApiKeyResolver, ApiMode, ServerConfig, TenantContext, TenantResolver, run, validate_tenant_id,
};

#[derive(Parser)]
#[command(name = "rulemorph-server")]
#[command(version, about = "Rulemorph UI/API server")]
struct Cli {
    #[arg(long, default_value_t = 8080)]
    port: u16,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    ui_dir: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = ApiModeArg::Rules)]
    api_mode: ApiModeArg,
    #[arg(long)]
    rules_dir: Option<PathBuf>,
    #[arg(long, default_value_t = 60)]
    rate_limit_per_sec: u64,
    #[arg(long, action = ArgAction::Append)]
    ssrf_allowlist: Vec<String>,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    ssrf_allow_private: bool,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    ssrf_allow_any: bool,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    no_ui: bool,
    #[arg(long)]
    api_key: Option<String>,
    #[arg(long)]
    tenant_id: Option<String>,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    api_key_store: bool,
    #[arg(long)]
    internal_api_key: Option<String>,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    allow_unauth_internal: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ApiModeArg {
    #[value(name = "ui-only", alias = "ui_only", alias = "native")]
    UiOnly,
    Rules,
}

struct StaticTenantResolver {
    api_key: String,
    tenant_id: String,
}

#[async_trait]
impl TenantResolver for StaticTenantResolver {
    async fn resolve(&self, api_key: &str) -> anyhow::Result<Option<TenantContext>> {
        if api_key == self.api_key {
            Ok(Some(TenantContext::new(self.tenant_id.clone())))
        } else {
            Ok(None)
        }
    }
}

impl From<ApiModeArg> for ApiMode {
    fn from(value: ApiModeArg) -> Self {
        match value {
            ApiModeArg::UiOnly => ApiMode::UiOnly,
            ApiModeArg::Rules => ApiMode::Rules,
        }
    }
}

fn build_static_tenant_resolver(
    api_key: Option<&str>,
    tenant_id: Option<String>,
) -> anyhow::Result<Option<std::sync::Arc<dyn TenantResolver>>> {
    let Some(api_key) = api_key else {
        return Ok(None);
    };
    let trimmed = api_key.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let tenant_id = tenant_id.unwrap_or_else(|| "default".to_string());
    validate_tenant_id(&tenant_id)
        .map_err(|err| anyhow::anyhow!("invalid --tenant-id: {}", err))?;
    Ok(Some(std::sync::Arc::new(StaticTenantResolver {
        api_key: trimmed.to_string(),
        tenant_id,
    }) as std::sync::Arc<dyn TenantResolver>))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let tenant_resolver = if cli.api_key_store {
        Some(std::sync::Arc::new(ApiKeyResolver::new(
            cli.data_dir
                .clone()
                .unwrap_or_else(ServerConfig::default_data_dir),
        )) as std::sync::Arc<dyn TenantResolver>)
    } else {
        build_static_tenant_resolver(cli.api_key.as_deref(), cli.tenant_id.clone())?
    };
    let internal_api_key = cli
        .internal_api_key
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());
    let config = ServerConfig {
        port: cli.port,
        data_dir: cli.data_dir.unwrap_or_else(ServerConfig::default_data_dir),
        ui_dir: cli.ui_dir,
        rules_dir: cli.rules_dir,
        api_mode: cli.api_mode.into(),
        ui_enabled: !cli.no_ui,
        tenant_resolver,
        internal_api_key,
        allow_unauth_internal: cli.allow_unauth_internal,
        rate_limit_per_sec: if cli.rate_limit_per_sec == 0 {
            None
        } else {
            Some(cli.rate_limit_per_sec)
        },
        ssrf_allowlist: cli
            .ssrf_allowlist
            .into_iter()
            .filter(|entry| !entry.trim().is_empty())
            .collect(),
        ssrf_allow_private: cli.ssrf_allow_private,
        ssrf_allow_any: cli.ssrf_allow_any,
    };
    run(config).await
}

#[cfg(test)]
mod tests {
    use super::build_static_tenant_resolver;

    #[test]
    fn static_tenant_resolver_rejects_invalid_tenant_id() {
        let err = match build_static_tenant_resolver(
            Some("static-key"),
            Some("tenant.invalid".to_string()),
        ) {
            Ok(_) => panic!("invalid tenant id should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("invalid --tenant-id"));
    }
}
