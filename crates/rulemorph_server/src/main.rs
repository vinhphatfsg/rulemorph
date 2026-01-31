use std::path::PathBuf;

use clap::{ArgAction, Parser, ValueEnum};
use rulemorph_server::{ApiMode, ServerConfig, run};

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
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    no_ui: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ApiModeArg {
    #[value(name = "ui-only", alias = "ui_only", alias = "native")]
    UiOnly,
    Rules,
}

impl From<ApiModeArg> for ApiMode {
    fn from(value: ApiModeArg) -> Self {
        match value {
            ApiModeArg::UiOnly => ApiMode::UiOnly,
            ApiModeArg::Rules => ApiMode::Rules,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = ServerConfig {
        port: cli.port,
        data_dir: cli.data_dir.unwrap_or_else(ServerConfig::default_data_dir),
        ui_dir: cli.ui_dir.unwrap_or_else(ServerConfig::default_ui_dir),
        rules_dir: cli.rules_dir,
        api_mode: cli.api_mode.into(),
        ui_enabled: !cli.no_ui,
    };
    run(config).await
}
