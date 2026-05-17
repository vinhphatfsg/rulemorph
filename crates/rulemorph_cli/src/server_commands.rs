use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Args, ValueEnum};
use rulemorph_server::{
    ApiMode, RulesDirErrors, ServerConfig, run as run_server, validate_rules_dir,
};
use rulemorph_trace::TraceStore;

use super::ErrorFormat;
use super::emit::emit_rules_dir_errors;

#[derive(Args)]
pub(super) struct ValidateRulesDirArgs {
    #[arg(short = 'r', long)]
    rules_dir: PathBuf,
    #[arg(short = 'e', long, default_value = "text")]
    error_format: ErrorFormat,
}

#[derive(Args)]
pub(super) struct UiArgs {
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

#[derive(Args)]
pub(super) struct PurgeTracesArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    retention_days: u64,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    dry_run: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum UiApiMode {
    #[value(name = "ui-only", alias = "ui_only", alias = "native")]
    UiOnly,
    Rules,
}

pub(super) fn run_validate_rules_dir(args: ValidateRulesDirArgs) -> i32 {
    match validate_rules_dir(&args.rules_dir) {
        Ok(()) => 0,
        Err(errs) => {
            emit_rules_dir_errors(&errs, args.error_format);
            2
        }
    }
}

pub(super) fn run_ui(args: UiArgs) -> i32 {
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

pub(super) fn run_purge_traces(args: PurgeTracesArgs) -> i32 {
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
