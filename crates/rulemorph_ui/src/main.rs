use rulemorph_ui::{run, ApiMode, UiConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = UiConfig {
        port: 8080,
        data_dir: UiConfig::default_data_dir(),
        ui_dir: UiConfig::default_ui_dir(),
        rules_dir: None,
        api_mode: ApiMode::UiOnly,
        ui_enabled: true,
    };
    run(config).await
}
