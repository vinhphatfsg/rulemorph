use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
#[cfg(feature = "server")]
use std::time::Duration;

#[cfg(feature = "server")]
use clap::ArgAction;
use clap::{Args, Parser, Subcommand, ValueEnum};
use rulemorph::{
    DtoLanguage, InputData, NormalizationOptions, RuleFile, generate_dto,
    preflight_validate_input_with_warnings_with_base_dir_and_options,
    transform_input_with_warnings_with_base_dir_and_options,
    transform_stream_input_with_base_dir_and_options, validate_rule_file_with_source,
};
#[cfg(feature = "server")]
use rulemorph_server::{
    ApiMode, RulesDirErrors, ServerConfig, run as run_server, validate_rules_dir,
};
#[cfg(feature = "server")]
use rulemorph_trace::TraceStore;

#[cfg(feature = "server")]
mod api_keys;
mod emit;
mod input;

#[cfg(feature = "server")]
use self::emit::emit_rules_dir_errors;
use self::emit::{emit_transform_error, emit_transform_warnings, emit_validation_errors};
use self::input::{
    apply_format_override, load_context, load_input_bytes_with_limit, load_normalization_options,
    load_rule, rule_base_dir,
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
    ValidateRulesDir(ValidateRulesDirArgs),
    Preflight(PreflightArgs),
    Transform(TransformArgs),
    Generate(GenerateArgs),
    #[cfg(feature = "server")]
    Ui(UiArgs),
    #[cfg(feature = "server")]
    PurgeTraces(PurgeTracesArgs),
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

#[cfg(feature = "server")]
#[derive(Args)]
struct ValidateRulesDirArgs {
    #[arg(short = 'r', long)]
    rules_dir: PathBuf,
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

#[cfg(feature = "server")]
#[derive(Args)]
struct UiArgs {
    #[arg(long, default_value_t = 8080)]
    port: u16,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    ui_dir: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = UiApiMode::Rules)]
    api_mode: UiApiMode,
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
    internal_api_key: Option<String>,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    allow_unauth_internal: bool,
}

#[cfg(feature = "server")]
#[derive(Args)]
struct PurgeTracesArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    retention_days: u64,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    dry_run: bool,
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

#[cfg(feature = "server")]
#[derive(Clone, Copy, Debug, ValueEnum)]
enum UiApiMode {
    #[value(name = "ui-only", alias = "ui_only", alias = "native")]
    UiOnly,
    Rules,
}

fn main() {
    let cli = Cli::parse();
    let exit_code = match cli.command {
        Commands::Validate(args) => run_validate(args),
        #[cfg(feature = "server")]
        Commands::ValidateRulesDir(args) => run_validate_rules_dir(args),
        Commands::Preflight(args) => run_preflight(args),
        Commands::Transform(args) => run_transform(args),
        Commands::Generate(args) => run_generate(args),
        #[cfg(feature = "server")]
        Commands::Ui(args) => run_ui(args),
        #[cfg(feature = "server")]
        Commands::PurgeTraces(args) => run_purge_traces(args),
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

#[cfg(feature = "server")]
fn run_validate_rules_dir(args: ValidateRulesDirArgs) -> i32 {
    match validate_rules_dir(&args.rules_dir) {
        Ok(()) => 0,
        Err(errs) => {
            emit_rules_dir_errors(&errs, args.error_format);
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

    let output_text = match serde_json::to_string(&output) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("failed to serialize output JSON: {}", err);
            return 1;
        }
    };

    emit_transform_warnings(&warnings, args.error_format);

    if let Some(path) = args.output {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(err) = fs::create_dir_all(parent) {
                    eprintln!("failed to create output directory: {}", err);
                    return 1;
                }
            }
        }
        if let Err(err) = fs::write(&path, output_text.as_bytes()) {
            eprintln!("failed to write output: {}", err);
            return 1;
        }
    } else {
        println!("{}", output_text);
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

    let writer: Box<dyn Write> = match output {
        Some(path) => {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    if let Err(err) = fs::create_dir_all(parent) {
                        eprintln!("failed to create output directory: {}", err);
                        return 1;
                    }
                }
            }
            match fs::File::create(&path) {
                Ok(file) => Box::new(file),
                Err(err) => {
                    eprintln!("failed to write output: {}", err);
                    return 1;
                }
            }
        }
        None => Box::new(io::stdout()),
    };

    let mut writer = io::BufWriter::new(writer);

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
        let output_text = match serde_json::to_string(&output) {
            Ok(text) => text,
            Err(err) => {
                eprintln!("failed to serialize output JSON: {}", err);
                return 1;
            }
        };

        if let Err(err) = writeln!(writer, "{}", output_text) {
            eprintln!("failed to write output: {}", err);
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

    if let Some(path) = args.output {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(err) = fs::create_dir_all(parent) {
                    eprintln!("failed to create output directory: {}", err);
                    return 1;
                }
            }
        }
        if let Err(err) = fs::write(&path, output.as_bytes()) {
            eprintln!("failed to write output: {}", err);
            return 1;
        }
    } else {
        println!("{}", output);
    }

    0
}

#[cfg(feature = "server")]
fn run_ui(args: UiArgs) -> i32 {
    let data_dir = args.data_dir.unwrap_or_else(ServerConfig::default_data_dir);
    let ui_dir = args.ui_dir;
    let api_mode = match args.api_mode {
        UiApiMode::UiOnly => ApiMode::UiOnly,
        UiApiMode::Rules => ApiMode::Rules,
    };
    let ui_enabled = !args.no_ui;
    if !ui_enabled && api_mode == ApiMode::UiOnly {
        eprintln!("ui-only mode cannot be used with --no-ui");
        return 1;
    }

    let config = ServerConfig {
        port: args.port,
        data_dir,
        ui_dir,
        rules_dir: args.rules_dir,
        api_mode,
        ui_enabled,
        tenant_resolver: None,
        internal_api_key: args
            .internal_api_key
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string()),
        allow_unauth_internal: args.allow_unauth_internal,
        rate_limit_per_sec: if args.rate_limit_per_sec == 0 {
            None
        } else {
            Some(args.rate_limit_per_sec)
        },
        ssrf_allowlist: args
            .ssrf_allowlist
            .into_iter()
            .filter(|entry| !entry.trim().is_empty())
            .collect(),
        ssrf_allow_private: args.ssrf_allow_private,
        ssrf_allow_any: args.ssrf_allow_any,
    };

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("failed to start runtime: {}", err);
            return 1;
        }
    };

    if let Err(err) = runtime.block_on(run_server(config)) {
        if let Some(errs) = err.downcast_ref::<RulesDirErrors>() {
            eprintln!("{}", errs);
            return 2;
        }
        eprintln!("server error: {}", err);
        return 1;
    }

    0
}

#[cfg(feature = "server")]
fn run_purge_traces(args: PurgeTracesArgs) -> i32 {
    if args.retention_days == 0 {
        eprintln!("--retention-days must be greater than 0");
        return 1;
    }

    let data_dir = args.data_dir.unwrap_or_else(ServerConfig::default_data_dir);
    let retention = Duration::from_secs(args.retention_days.saturating_mul(86_400));

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("failed to start runtime: {}", err);
            return 1;
        }
    };

    let result = runtime.block_on(async {
        let store = TraceStore::new(data_dir).await?;
        store.purge_traces(retention, args.dry_run).await
    });

    match result {
        Ok(report) => {
            let purged = report.purged;
            if args.dry_run {
                println!("dry-run: {} trace(s) would be removed", purged.len());
            } else {
                println!("removed {} trace(s)", purged.len());
            }
            for trace in purged {
                let timestamp = trace.timestamp.as_deref().unwrap_or("unknown timestamp");
                println!("- {} ({}) {}", trace.trace_id, timestamp, trace.path);
            }
            if !report.failed.is_empty() {
                eprintln!("failed to remove {} trace(s)", report.failed.len());
                for failure in report.failed {
                    eprintln!(
                        "- {} ({}) {}",
                        failure.trace_id, failure.error, failure.path
                    );
                }
                return 2;
            }
            0
        }
        Err(err) => {
            eprintln!("purge failed: {}", err);
            1
        }
    }
}
