use std::ffi::OsString;
use std::path::PathBuf;

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};

#[cfg(feature = "server")]
mod api_keys;
mod core_commands;
mod direct;
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
    #[arg(long = "rule")]
    rule: Option<String>,
    #[arg(short = 'F', long = "field")]
    fields: Vec<String>,
    #[arg(long = "output-map")]
    output_map: Option<String>,
    #[arg(short = 'i', long)]
    input: Option<PathBuf>,
    #[arg(short = 'f', long)]
    format: Option<DirectFormatArg>,
    #[arg(short = 'H', long = "headers")]
    headers: Option<String>,
    #[arg(long = "excel-data-range")]
    excel_data_range: Option<String>,
    #[arg(long)]
    excel_header_row: Option<usize>,
    #[arg(long)]
    excel_sheet: Option<String>,
    #[arg(long)]
    excel_sheet_index: Option<usize>,
    #[arg(short = 'o', long)]
    output: Option<PathBuf>,
    #[arg(
        long,
        help = "Emit one JSON value per line in direct mode. Each input record produces one line."
    )]
    ndjson: bool,
    #[arg(short = 'e', long)]
    error_format: Option<ErrorFormat>,
    #[arg(long = "limit")]
    limits: Vec<String>,
    #[arg(long, value_enum)]
    limits_profile: Option<LimitsProfileArg>,
    #[arg(long)]
    limits_file: Option<PathBuf>,
    #[arg(short = 'c', long)]
    context: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Commands>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum DirectFormatArg {
    Csv,
    Json,
    Excel,
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
    let cli = Cli::parse_from(normalize_rule_alias(std::env::args_os()));
    let has_direct_output_spec =
        cli.rule.is_some() || !cli.fields.is_empty() || cli.output_map.is_some();
    let has_direct_options = cli.has_direct_options();
    let Cli {
        rule,
        fields,
        output_map,
        input,
        format,
        headers,
        excel_data_range,
        excel_header_row,
        excel_sheet,
        excel_sheet_index,
        output,
        ndjson,
        error_format,
        limits,
        limits_profile,
        limits_file,
        context,
        command,
    } = cli;

    let exit_code = match (has_direct_output_spec, command) {
        (true, None) => direct::run(direct::DirectArgs {
            rule,
            fields,
            output_map,
            input,
            format,
            headers,
            excel_data_range,
            excel_header_row,
            excel_sheet,
            excel_sheet_index,
            output,
            ndjson,
            error_format,
            limits,
            limits_profile,
            limits_file,
            context,
        }),
        (true, Some(_)) => {
            eprintln!("--rule, --output-map, and --field cannot be used with a subcommand");
            2
        }
        (false, Some(command)) if has_direct_options => {
            eprintln!(
                "direct-mode options require --rule, --output-map, or --field and cannot be used before a subcommand"
            );
            let _ = command;
            2
        }
        (false, Some(Commands::Validate(args))) => core_commands::run_validate(args),
        #[cfg(feature = "server")]
        (false, Some(Commands::ValidateRulesDir(args))) => {
            server_commands::run_validate_rules_dir(args)
        }
        (false, Some(Commands::Preflight(args))) => core_commands::run_preflight(args),
        (false, Some(Commands::Transform(args))) => core_commands::run_transform(args),
        (false, Some(Commands::Generate(args))) => generate::run(args),
        #[cfg(feature = "server")]
        (false, Some(Commands::Ui(args))) => server_commands::run_ui(args),
        #[cfg(feature = "server")]
        (false, Some(Commands::PurgeTraces(args))) => server_commands::run_purge_traces(args),
        #[cfg(feature = "server")]
        (false, Some(Commands::ApiKeys(args))) => api_keys::run(args),
        (false, None) => {
            let _ = Cli::command().print_help();
            eprintln!();
            2
        }
    };
    std::process::exit(exit_code);
}

fn normalize_rule_alias(args: impl IntoIterator<Item = OsString>) -> Vec<OsString> {
    let mut after_options_marker = false;
    args.into_iter()
        .map(|arg| {
            if after_options_marker {
                arg
            } else if arg == "--" {
                after_options_marker = true;
                arg
            } else if arg == "-rule" {
                OsString::from("--rule")
            } else if let Some(value) = arg.to_str().and_then(|text| text.strip_prefix("-rule=")) {
                OsString::from(format!("--rule={}", value))
            } else {
                arg
            }
        })
        .collect()
}

impl Cli {
    fn has_direct_options(&self) -> bool {
        self.input.is_some()
            || self.format.is_some()
            || self.headers.is_some()
            || self.excel_data_range.is_some()
            || self.excel_header_row.is_some()
            || self.excel_sheet.is_some()
            || self.excel_sheet_index.is_some()
            || self.output.is_some()
            || self.ndjson
            || self.error_format.is_some()
            || !self.limits.is_empty()
            || self.limits_profile.is_some()
            || self.limits_file.is_some()
            || self.context.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os_args(args: &[&str]) -> Vec<OsString> {
        args.iter().map(OsString::from).collect()
    }

    #[test]
    fn normalize_rule_alias_rewrites_before_options_marker() {
        let args = normalize_rule_alias(os_args(&[
            "rulemorph",
            "-rule",
            "@input.test",
            "-rule=@input.id",
        ]));

        assert_eq!(
            args,
            os_args(&["rulemorph", "--rule", "@input.test", "--rule=@input.id",])
        );
    }

    #[test]
    fn normalize_rule_alias_preserves_args_after_options_marker() {
        let args = normalize_rule_alias(os_args(&[
            "rulemorph",
            "--rule",
            "@input.test",
            "--",
            "-rule",
            "-rule=@input.id",
        ]));

        assert_eq!(
            args,
            os_args(&[
                "rulemorph",
                "--rule",
                "@input.test",
                "--",
                "-rule",
                "-rule=@input.id",
            ])
        );
    }
}
