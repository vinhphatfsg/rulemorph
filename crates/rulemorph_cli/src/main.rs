use std::io::Write;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use rulemorph::{
    DtoLanguage, InputData, NormalizationOptions, RuleFile, generate_dto,
    preflight_validate_input_with_warnings_with_base_dir_and_options,
    transform_input_with_warnings_with_base_dir_and_options,
    transform_stream_input_with_base_dir_and_options, validate_rule_file_with_source,
};

#[cfg(feature = "server")]
mod api_keys;
mod emit;
mod input;
mod output;
#[cfg(feature = "server")]
mod server_commands;

use self::emit::{emit_transform_error, emit_transform_warnings, emit_validation_errors};
use self::input::{
    apply_format_override, load_context, load_input_bytes_with_limit, load_normalization_options,
    load_rule, rule_base_dir,
};
use self::output::{
    create_output_writer, emit_text_output, serialize_json_output, write_json_line,
};

#[derive(Parser)]
#[command(name = "rulemorph")]
#[command(version, about = "Transform CSV/JSON data using YAML rules")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Validate(ValidateArgs),
    #[cfg(feature = "server")]
    ValidateRulesDir(server_commands::ValidateRulesDirArgs),
    Preflight(PreflightArgs),
    Transform(TransformArgs),
    Generate(GenerateArgs),
    #[cfg(feature = "server")]
    Ui(server_commands::UiArgs),
    #[cfg(feature = "server")]
    PurgeTraces(server_commands::PurgeTracesArgs),
    #[cfg(feature = "server")]
    ApiKeys(api_keys::ApiKeysArgs),
}

#[derive(Args)]
struct ValidateArgs {
    #[arg(short = 'r', long)]
    rules: PathBuf,
    #[arg(long)]
    rules_format: Option<RulesFormatArg>,
    #[arg(short = 'e', long, default_value = "text")]
    error_format: ErrorFormat,
}

#[derive(Args)]
struct PreflightArgs {
    #[arg(short = 'r', long)]
    rules: PathBuf,
    #[arg(long)]
    rules_format: Option<RulesFormatArg>,
    #[arg(short = 'i', long)]
    input: PathBuf,
    #[arg(short = 'f', long)]
    format: Option<FormatOverride>,
    #[arg(short = 'c', long)]
    context: Option<PathBuf>,
    #[arg(short = 'e', long, default_value = "text")]
    error_format: ErrorFormat,
    #[arg(long = "limit")]
    limits: Vec<String>,
    #[arg(long, value_enum)]
    limits_profile: Option<LimitsProfileArg>,
    #[arg(long)]
    limits_file: Option<PathBuf>,
}

#[derive(Args)]
struct TransformArgs {
    #[arg(short = 'r', long)]
    rules: PathBuf,
    #[arg(long)]
    rules_format: Option<RulesFormatArg>,
    #[arg(short = 'i', long)]
    input: PathBuf,
    #[arg(short = 'f', long)]
    format: Option<FormatOverride>,
    #[arg(short = 'c', long)]
    context: Option<PathBuf>,
    #[arg(short = 'o', long)]
    output: Option<PathBuf>,
    #[arg(
        long,
        help = "Emit one JSON object per line. This streams output, but input normalization is bounded by the configured limits."
    )]
    ndjson: bool,
    #[arg(short = 'v', long)]
    validate: bool,
    #[arg(short = 'e', long, default_value = "text")]
    error_format: ErrorFormat,
    #[arg(long = "limit")]
    limits: Vec<String>,
    #[arg(long, value_enum)]
    limits_profile: Option<LimitsProfileArg>,
    #[arg(long)]
    limits_file: Option<PathBuf>,
}

#[derive(Args)]
struct GenerateArgs {
    #[arg(short = 'r', long)]
    rules: PathBuf,
    #[arg(long)]
    rules_format: Option<RulesFormatArg>,
    #[arg(short = 'l', long)]
    lang: DtoLanguageArg,
    #[arg(short = 'n', long)]
    name: Option<String>,
    #[arg(short = 'o', long)]
    output: Option<PathBuf>,
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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum DtoLanguageArg {
    Rust,
    #[value(alias = "ts")]
    TypeScript,
    Python,
    Go,
    Java,
    Kotlin,
    Swift,
}

fn main() {
    let cli = Cli::parse();
    let exit_code = match cli.command {
        Commands::Validate(args) => run_validate(args),
        #[cfg(feature = "server")]
        Commands::ValidateRulesDir(args) => server_commands::run_validate_rules_dir(args),
        Commands::Preflight(args) => run_preflight(args),
        Commands::Transform(args) => run_transform(args),
        Commands::Generate(args) => run_generate(args),
        #[cfg(feature = "server")]
        Commands::Ui(args) => server_commands::run_ui(args),
        #[cfg(feature = "server")]
        Commands::PurgeTraces(args) => server_commands::run_purge_traces(args),
        #[cfg(feature = "server")]
        Commands::ApiKeys(args) => api_keys::run(args),
    };
    std::process::exit(exit_code);
}

fn run_validate(args: ValidateArgs) -> i32 {
    let (rule, yaml) = match load_rule(&args.rules, args.rules_format) {
        Ok(value) => value,
        Err(code) => return code,
    };

    match validate_rule_file_with_source(&rule, &yaml) {
        Ok(()) => 0,
        Err(errors) => {
            emit_validation_errors(&errors, args.error_format);
            2
        }
    }
}

fn run_preflight(args: PreflightArgs) -> i32 {
    let (mut rule, _) = match load_rule(&args.rules, args.rules_format) {
        Ok(value) => value,
        Err(code) => return code,
    };

    apply_format_override(&mut rule, args.format);

    let options = match load_normalization_options(
        args.limits_profile,
        args.limits_file.as_ref(),
        &args.limits,
    ) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{}", message);
            return 2;
        }
    };

    let input = match load_input_bytes_with_limit(&args.input, options.max_input_bytes) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let context_value = match load_context(&args.context) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let base_dir = rule_base_dir(&args.rules);
    let warnings = match preflight_validate_input_with_warnings_with_base_dir_and_options(
        &rule,
        InputData::Bytes(&input),
        context_value.as_ref(),
        &base_dir,
        &options,
    ) {
        Ok(warnings) => warnings,
        Err(err) => {
            emit_transform_error(&err, args.error_format);
            return 3;
        }
    };

    emit_transform_warnings(&warnings, args.error_format);

    0
}

fn run_transform(args: TransformArgs) -> i32 {
    let (mut rule, yaml) = match load_rule(&args.rules, args.rules_format) {
        Ok(value) => value,
        Err(code) => return code,
    };

    apply_format_override(&mut rule, args.format);

    if args.validate {
        if let Err(errors) = validate_rule_file_with_source(&rule, &yaml) {
            emit_validation_errors(&errors, args.error_format);
            return 2;
        }
    }

    let options = match load_normalization_options(
        args.limits_profile,
        args.limits_file.as_ref(),
        &args.limits,
    ) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{}", message);
            return 2;
        }
    };

    let input = match load_input_bytes_with_limit(&args.input, options.max_input_bytes) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let context_value = match load_context(&args.context) {
        Ok(value) => value,
        Err(code) => return code,
    };

    if args.ndjson {
        return run_transform_ndjson(
            &rule,
            &input,
            context_value.as_ref(),
            args.output,
            args.error_format,
            &args.rules,
            &options,
        );
    }

    let base_dir = rule_base_dir(&args.rules);
    let (output, warnings) = match transform_input_with_warnings_with_base_dir_and_options(
        &rule,
        InputData::Bytes(&input),
        context_value.as_ref(),
        &base_dir,
        &options,
    ) {
        Ok(result) => result,
        Err(err) => {
            emit_transform_error(&err, args.error_format);
            return 3;
        }
    };

    let output_text = match serialize_json_output(&output) {
        Ok(text) => text,
        Err(()) => return 1,
    };

    emit_transform_warnings(&warnings, args.error_format);

    if emit_text_output(&output_text, args.output.as_ref()).is_err() {
        return 1;
    }

    0
}

fn run_transform_ndjson(
    rule: &RuleFile,
    input: &[u8],
    context: Option<&serde_json::Value>,
    output: Option<PathBuf>,
    error_format: ErrorFormat,
    rules_path: &PathBuf,
    options: &NormalizationOptions,
) -> i32 {
    let base_dir = rule_base_dir(rules_path);
    let stream = match transform_stream_input_with_base_dir_and_options(
        rule,
        InputData::Bytes(input),
        context,
        &base_dir,
        options,
    ) {
        Ok(stream) => stream,
        Err(err) => {
            emit_transform_error(&err, error_format);
            return 3;
        }
    };

    let mut writer = match create_output_writer(output.as_ref()) {
        Ok(writer) => writer,
        Err(()) => return 1,
    };

    for item in stream {
        let item = match item {
            Ok(item) => item,
            Err(err) => {
                emit_transform_error(&err, error_format);
                return 3;
            }
        };

        emit_transform_warnings(&item.warnings, error_format);

        let output = match item.output {
            Some(output) => output,
            None => continue,
        };
        if write_json_line(&mut writer, &output).is_err() {
            return 1;
        }
    }

    if let Err(err) = writer.flush() {
        eprintln!("failed to write output: {}", err);
        return 1;
    }

    0
}

fn run_generate(args: GenerateArgs) -> i32 {
    let (rule, _) = match load_rule(&args.rules, args.rules_format) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let lang = match args.lang {
        DtoLanguageArg::Rust => DtoLanguage::Rust,
        DtoLanguageArg::TypeScript => DtoLanguage::TypeScript,
        DtoLanguageArg::Python => DtoLanguage::Python,
        DtoLanguageArg::Go => DtoLanguage::Go,
        DtoLanguageArg::Java => DtoLanguage::Java,
        DtoLanguageArg::Kotlin => DtoLanguage::Kotlin,
        DtoLanguageArg::Swift => DtoLanguage::Swift,
    };

    let output = match generate_dto(&rule, lang, args.name.as_deref()) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("failed to generate dto: {}", err);
            return 1;
        }
    };

    if output::emit_text_output(&output, args.output.as_ref()).is_err() {
        return 1;
    }

    0
}
