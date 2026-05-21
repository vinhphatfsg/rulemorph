use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Args};
use rulemorph_server::ServerConfig;
use rulemorph_trace::TraceStore;

#[derive(Args)]
pub(crate) struct PurgeTracesArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    retention_days: u64,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    dry_run: bool,
}

pub(crate) fn run_purge_traces(args: PurgeTracesArgs) -> i32 {
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
