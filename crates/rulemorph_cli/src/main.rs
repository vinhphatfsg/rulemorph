use std::fs;
use std::io::{self, Read, Write};
use std::path::PathBuf;
#[cfg(feature = "server")]
use std::time::Duration;

#[cfg(feature = "server")]
use clap::ArgAction;
use clap::{Args, Parser, Subcommand, ValueEnum};
use rulemorph::{
    DtoLanguage, InputData, InputFormat, NormalizationOptions, RuleError, RuleFile, RuleFormat,
    TransformError, TransformErrorKind, TransformWarning, generate_dto,
    parse_rule_file_with_format, preflight_validate_input_with_warnings_with_base_dir,
    transform_input_with_warnings_with_base_dir_and_options,
    transform_stream_input_with_base_dir_and_options, validate_rule_file_with_source,
};
#[cfg(feature = "server")]
use rulemorph_server::{
    ApiKeyInfo, ApiKeyIssueResult, ApiKeyStore, ApiMode, RulesDirErrors, ServerConfig,
    TenantLayout, run as run_server, validate_rules_dir,
};
#[cfg(feature = "server")]
use rulemorph_trace::TraceStore;
use serde_json::json;

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
    ApiKeys(ApiKeysArgs),
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

#[cfg(feature = "server")]
#[derive(Args)]
struct ApiKeysArgs {
    #[command(subcommand)]
    command: ApiKeysCommand,
}

#[cfg(feature = "server")]
#[derive(Subcommand)]
enum ApiKeysCommand {
    Issue(ApiKeysIssueArgs),
    List(ApiKeysListArgs),
    Revoke(ApiKeysRevokeArgs),
    Rotate(ApiKeysRotateArgs),
}

#[cfg(feature = "server")]
#[derive(Args)]
struct ApiKeysIssueArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    label: Option<String>,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    json: bool,
}

#[cfg(feature = "server")]
#[derive(Args)]
struct ApiKeysListArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    json: bool,
}

#[cfg(feature = "server")]
#[derive(Args)]
struct ApiKeysRevokeArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    id: String,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    json: bool,
}

#[cfg(feature = "server")]
#[derive(Args)]
struct ApiKeysRotateArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    id: String,
    #[arg(long)]
    label: Option<String>,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    json: bool,
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
        Commands::ApiKeys(args) => run_api_keys(args),
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

    let input = match load_input_bytes_with_limit(
        &args.input,
        NormalizationOptions::default().max_input_bytes,
    ) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let context_value = match load_context(&args.context) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let base_dir = rule_base_dir(&args.rules);
    let warnings = match preflight_validate_input_with_warnings_with_base_dir(
        &rule,
        InputData::Bytes(&input),
        context_value.as_ref(),
        &base_dir,
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

#[cfg(feature = "server")]
fn run_api_keys(args: ApiKeysArgs) -> i32 {
    match args.command {
        ApiKeysCommand::Issue(args) => run_api_keys_issue(args),
        ApiKeysCommand::List(args) => run_api_keys_list(args),
        ApiKeysCommand::Revoke(args) => run_api_keys_revoke(args),
        ApiKeysCommand::Rotate(args) => run_api_keys_rotate(args),
    }
}

#[cfg(feature = "server")]
fn run_api_keys_issue(args: ApiKeysIssueArgs) -> i32 {
    let layout = match resolve_tenant_layout(&args.tenant_id, args.data_dir) {
        Ok(layout) => layout,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let path = layout.api_keys_path();
    let mut store = match ApiKeyStore::load_or_init(path, layout.tenant_id()) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let issued = match store.issue(args.label) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    emit_api_key_issue(&issued, args.json);
    0
}

#[cfg(feature = "server")]
fn run_api_keys_list(args: ApiKeysListArgs) -> i32 {
    let layout = match resolve_tenant_layout(&args.tenant_id, args.data_dir) {
        Ok(layout) => layout,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let path = layout.api_keys_path();
    let store = match ApiKeyStore::load(path, layout.tenant_id()) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let keys = store.map(|store| store.list()).unwrap_or_default();
    emit_api_key_list(&keys, args.json);
    0
}

#[cfg(feature = "server")]
fn run_api_keys_revoke(args: ApiKeysRevokeArgs) -> i32 {
    let layout = match resolve_tenant_layout(&args.tenant_id, args.data_dir) {
        Ok(layout) => layout,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let path = layout.api_keys_path();
    let mut store = match ApiKeyStore::load(path, layout.tenant_id()) {
        Ok(Some(store)) => store,
        Ok(None) => {
            eprintln!("api key store not found");
            return 2;
        }
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let revoked = match store.revoke(&args.id) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    if args.json {
        println!("{}", serde_json::json!({ "revoked": revoked }));
    } else {
        println!("revoked: {}", revoked);
    }
    if revoked { 0 } else { 2 }
}

#[cfg(feature = "server")]
fn run_api_keys_rotate(args: ApiKeysRotateArgs) -> i32 {
    let layout = match resolve_tenant_layout(&args.tenant_id, args.data_dir) {
        Ok(layout) => layout,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let path = layout.api_keys_path();
    let mut store = match ApiKeyStore::load_or_init(path, layout.tenant_id()) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let issued = match store.rotate(&args.id, args.label) {
        Ok(Some(issued)) => issued,
        Ok(None) => {
            eprintln!("api key not found");
            return 2;
        }
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    emit_api_key_issue(&issued, args.json);
    0
}

#[cfg(feature = "server")]
fn resolve_tenant_layout(
    tenant_id: &str,
    data_dir: Option<PathBuf>,
) -> Result<TenantLayout, String> {
    let base_dir = data_dir.unwrap_or_else(ServerConfig::default_data_dir);
    TenantLayout::new(base_dir, tenant_id).map_err(|err| err.to_string())
}

#[cfg(feature = "server")]
fn emit_api_key_issue(issued: &ApiKeyIssueResult, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(issued).unwrap_or_default()
        );
        return;
    }
    println!("id: {}", issued.id);
    println!("prefix: {}", issued.prefix);
    println!("key: {}", issued.key);
    if let Some(label) = issued.label.as_ref() {
        println!("label: {}", label);
    }
    println!("created_at: {}", issued.created_at);
}

#[cfg(feature = "server")]
fn emit_api_key_list(keys: &[ApiKeyInfo], json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(keys).unwrap_or_else(|_| "[]".to_string())
        );
        return;
    }
    if keys.is_empty() {
        println!("no api keys");
        return;
    }
    for key in keys {
        println!("id: {}", key.id);
        println!("prefix: {}", key.prefix);
        println!("created_at: {}", key.created_at);
        if let Some(revoked) = key.revoked_at.as_ref() {
            println!("revoked_at: {}", revoked);
        }
        if let Some(label) = key.label.as_ref() {
            println!("label: {}", label);
        }
        println!("---");
    }
}
fn load_rule(
    path: &PathBuf,
    override_format: Option<RulesFormatArg>,
) -> Result<(RuleFile, String), i32> {
    let yaml = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(err) => {
            eprintln!("failed to read rules: {}", err);
            return Err(1);
        }
    };

    let format = detect_rule_format(path, override_format);
    let rule = match parse_rule_file_with_format(&yaml, format) {
        Ok(rule) => rule,
        Err(err) => {
            eprintln!("failed to parse rules: {}", err);
            return Err(1);
        }
    };

    Ok((rule, yaml))
}

fn detect_rule_format(path: &PathBuf, override_format: Option<RulesFormatArg>) -> RuleFormat {
    match override_format {
        Some(RulesFormatArg::Yaml) => RuleFormat::Yaml,
        Some(RulesFormatArg::Json) => RuleFormat::Json,
        None => RuleFormat::from_path(path),
    }
}

fn rule_base_dir(path: &PathBuf) -> PathBuf {
    path.parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .to_path_buf()
}

fn apply_format_override(rule: &mut RuleFile, format: Option<FormatOverride>) {
    if let Some(format) = format {
        rule.input.format = match format {
            FormatOverride::Csv => InputFormat::Csv,
            FormatOverride::Json => InputFormat::Json,
        };
    }
}

fn load_input_bytes_with_limit(path: &PathBuf, max_input_bytes: usize) -> Result<Vec<u8>, i32> {
    match read_file_with_limit(path, max_input_bytes) {
        Ok(value) => Ok(value),
        Err(message) => {
            eprintln!("failed to read input: {}", message);
            Err(1)
        }
    }
}

fn read_file_with_limit(path: &PathBuf, max_bytes: usize) -> Result<Vec<u8>, String> {
    let metadata = fs::metadata(path).map_err(|err| err.to_string())?;
    if metadata.len() > max_bytes as u64 {
        return Err(format!("input exceeds max_input_bytes ({})", max_bytes));
    }
    let mut file = fs::File::open(path).map_err(|err| err.to_string())?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| err.to_string())?;
    if bytes.len() > max_bytes {
        return Err(format!("input exceeds max_input_bytes ({})", max_bytes));
    }
    Ok(bytes)
}

fn load_context(path: &Option<PathBuf>) -> Result<Option<serde_json::Value>, i32> {
    match path {
        Some(path) => match fs::read_to_string(path) {
            Ok(data) => match serde_json::from_str(&data) {
                Ok(json) => Ok(Some(json)),
                Err(err) => {
                    eprintln!("failed to parse context JSON: {}", err);
                    Err(1)
                }
            },
            Err(err) => {
                eprintln!("failed to read context: {}", err);
                Err(1)
            }
        },
        None => Ok(None),
    }
}

fn load_normalization_options(
    profile: Option<LimitsProfileArg>,
    file: Option<&PathBuf>,
    overrides: &[String],
) -> Result<NormalizationOptions, String> {
    let mut options = match profile.unwrap_or(LimitsProfileArg::Default) {
        LimitsProfileArg::Default => NormalizationOptions::default(),
        LimitsProfileArg::Large => NormalizationOptions::large(),
    };
    if let Some(file) = file {
        let raw = fs::read_to_string(file)
            .map_err(|err| format!("failed to read limits file: {}", err))?;
        let value = raw
            .parse::<toml::Value>()
            .map_err(|err| format!("failed to parse limits file: {}", err))?;
        let table = value
            .as_table()
            .ok_or_else(|| "limits file must contain a TOML table".to_string())?;
        for (name, value) in table {
            let value = value
                .as_integer()
                .ok_or_else(|| format!("limit `{}` must be an integer", name))?;
            apply_limit_override(&mut options, name, value.into())?;
        }
    }
    for item in overrides {
        let (name, value) = item
            .split_once('=')
            .ok_or_else(|| "limit override must use name=value".to_string())?;
        let value = value
            .parse::<i128>()
            .map_err(|_| format!("limit `{}` must be a positive integer", name))?;
        apply_limit_override(&mut options, name, value)?;
    }
    Ok(options)
}

fn apply_limit_override(
    options: &mut NormalizationOptions,
    name: &str,
    value: i128,
) -> Result<(), String> {
    if value <= 0 {
        return Err(format!("limit `{}` must be a positive integer", name));
    }
    let value = usize::try_from(value)
        .map_err(|_| format!("limit `{}` is too large for this platform", name))?;
    if value == usize::MAX {
        return Err(format!("limit `{}` is too large", name));
    }
    match name {
        "input-bytes" => options.max_input_bytes = value,
        "records" => options.max_records = value,
        "depth" => options.max_depth = value,
        "array-len" => options.max_array_len = value,
        "text-bytes" => options.max_text_bytes = value,
        "yaml-aliases" => options.max_yaml_aliases = value,
        "yaml-expanded-nodes" => options.max_yaml_expanded_nodes = value,
        "xml-nodes" => options.max_xml_nodes = value,
        "html-nodes" => options.max_html_nodes = value,
        "excel-zip-entries" => options.max_excel_zip_entries = value,
        "excel-uncompressed-bytes" => options.max_excel_uncompressed_bytes = value,
        "excel-entry-uncompressed-bytes" => options.max_excel_entry_uncompressed_bytes = value,
        "excel-sheets" => options.max_excel_sheets = value,
        "excel-rows" => options.max_excel_rows = value,
        "excel-cells" => options.max_excel_cells = value,
        "excel-shared-strings" => options.max_excel_shared_strings = value,
        "excel-shared-string-bytes" => options.max_excel_shared_string_bytes = value,
        "excel-styles" => options.max_excel_styles = value,
        _ => return Err(format!("unknown limit `{}`", name)),
    }
    Ok(())
}

fn emit_validation_errors(errors: &[RuleError], format: ErrorFormat) {
    match format {
        ErrorFormat::Text => {
            for err in errors {
                emit_validation_text(err);
            }
        }
        ErrorFormat::Json => {
            let values: Vec<_> = errors
                .iter()
                .map(|err| validation_error_json(err))
                .collect();
            eprintln!("{}", serde_json::to_string(&values).unwrap_or_default());
        }
    }
}

#[cfg(feature = "server")]
fn emit_rules_dir_errors(errors: &RulesDirErrors, format: ErrorFormat) {
    match format {
        ErrorFormat::Text => {
            eprintln!("{}", errors);
        }
        ErrorFormat::Json => {
            let values: Vec<_> = errors
                .errors
                .iter()
                .map(|err| rules_dir_error_json(err))
                .collect();
            eprintln!("{}", serde_json::to_string(&values).unwrap_or_default());
        }
    }
}

fn emit_validation_text(err: &RuleError) {
    let mut parts = Vec::new();
    parts.push(format!("E {}", err.code.as_str()));
    if let Some(path) = &err.path {
        parts.push(format!("path={}", path));
    }
    if let Some(location) = &err.location {
        parts.push(format!("line={}", location.line));
        parts.push(format!("col={}", location.column));
    }
    parts.push(format!("msg=\"{}\"", err.message));
    eprintln!("{}", parts.join(" "));
}

fn validation_error_json(err: &RuleError) -> serde_json::Value {
    let mut value = json!({
        "type": "validation",
        "code": err.code.as_str(),
        "message": err.message,
    });

    if let Some(path) = &err.path {
        value["path"] = json!(path);
    }
    if let Some(location) = &err.location {
        value["line"] = json!(location.line);
        value["column"] = json!(location.column);
    }

    value
}

#[cfg(feature = "server")]
fn rules_dir_error_json(err: &rulemorph_server::RulesDirError) -> serde_json::Value {
    let mut value = json!({
        "type": "rules_dir",
        "code": err.code,
        "message": err.message,
        "file": err.file.to_string_lossy(),
    });
    if let Some(path) = &err.path {
        value["path"] = json!(path);
    }
    if let Some(line) = err.line {
        value["line"] = json!(line);
    }
    if let Some(column) = err.column {
        value["column"] = json!(column);
    }
    value
}

fn emit_transform_error(err: &TransformError, format: ErrorFormat) {
    match format {
        ErrorFormat::Text => {
            let mut parts = Vec::new();
            parts.push(format!("E {}", transform_kind_to_str(&err.kind)));
            if let Some(path) = &err.path {
                parts.push(format!("path={}", path));
            }
            parts.push(format!("msg=\"{}\"", err.message));
            eprintln!("{}", parts.join(" "));
        }
        ErrorFormat::Json => {
            let mut value = json!({
                "type": "transform",
                "kind": transform_kind_to_str(&err.kind),
                "message": err.message,
            });
            if let Some(path) = &err.path {
                value["path"] = json!(path);
            }
            eprintln!(
                "{}",
                serde_json::to_string(&vec![value]).unwrap_or_default()
            );
        }
    }
}

fn emit_transform_warnings(warnings: &[TransformWarning], format: ErrorFormat) {
    if warnings.is_empty() {
        return;
    }

    match format {
        ErrorFormat::Text => {
            for warning in warnings {
                let mut parts = Vec::new();
                parts.push(format!("W {}", transform_kind_to_str(&warning.kind)));
                if let Some(path) = &warning.path {
                    parts.push(format!("path={}", path));
                }
                parts.push(format!("msg=\"{}\"", warning.message));
                eprintln!("{}", parts.join(" "));
            }
        }
        ErrorFormat::Json => {
            let values: Vec<_> = warnings
                .iter()
                .map(|warning| transform_warning_json(warning))
                .collect();
            eprintln!("{}", serde_json::to_string(&values).unwrap_or_default());
        }
    }
}

fn transform_warning_json(warning: &TransformWarning) -> serde_json::Value {
    let mut value = json!({
        "type": "warning",
        "kind": transform_kind_to_str(&warning.kind),
        "message": warning.message,
    });
    if let Some(path) = &warning.path {
        value["path"] = json!(path);
    }
    value
}

fn transform_kind_to_str(kind: &TransformErrorKind) -> &'static str {
    match kind {
        TransformErrorKind::InvalidInput => "InvalidInput",
        TransformErrorKind::InvalidRecordsPath => "InvalidRecordsPath",
        TransformErrorKind::InvalidRef => "InvalidRef",
        TransformErrorKind::InvalidTarget => "InvalidTarget",
        TransformErrorKind::MissingRequired => "MissingRequired",
        TransformErrorKind::TypeCastFailed => "TypeCastFailed",
        TransformErrorKind::ExprError => "ExprError",
        TransformErrorKind::AssertionFailed => "AssertionFailed",
    }
}
