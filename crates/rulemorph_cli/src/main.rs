use clap::{Parser, Subcommand, ValueEnum};

#[cfg(feature = "server")]
mod api_keys;
mod core_commands;
mod emit;
mod generate;
mod input;
mod output;
#[cfg(feature = "server")]
mod server_commands;

#[derive(Parser)]
#[command(name = "rulemorph")]
#[command(version, about = "Transform CSV/JSON data using YAML rules")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Validate(core_commands::ValidateArgs),
    #[cfg(feature = "server")]
    ValidateRulesDir(server_commands::ValidateRulesDirArgs),
    Preflight(core_commands::PreflightArgs),
    Transform(core_commands::TransformArgs),
    Generate(generate::GenerateArgs),
    #[cfg(feature = "server")]
    Ui(server_commands::UiArgs),
    #[cfg(feature = "server")]
    PurgeTraces(server_commands::PurgeTracesArgs),
    #[cfg(feature = "server")]
    ApiKeys(api_keys::ApiKeysArgs),
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ErrorFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum FormatOverride {
    Csv,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum RulesFormatArg {
    Yaml,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LimitsProfileArg {
    Default,
    Large,
}

fn main() {
    let cli = Cli::parse();
    let exit_code = match cli.command {
        Commands::Validate(args) => core_commands::run_validate(args),
        #[cfg(feature = "server")]
        Commands::ValidateRulesDir(args) => server_commands::run_validate_rules_dir(args),
        Commands::Preflight(args) => core_commands::run_preflight(args),
        Commands::Transform(args) => core_commands::run_transform(args),
        Commands::Generate(args) => generate::run(args),
        #[cfg(feature = "server")]
        Commands::Ui(args) => server_commands::run_ui(args),
        #[cfg(feature = "server")]
        Commands::PurgeTraces(args) => server_commands::run_purge_traces(args),
        #[cfg(feature = "server")]
        Commands::ApiKeys(args) => api_keys::run(args),
    };
    std::process::exit(exit_code);
}
