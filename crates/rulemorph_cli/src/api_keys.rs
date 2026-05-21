use std::path::PathBuf;

use clap::{Args, Subcommand};
use rulemorph_server::{ApiKeyStore, ServerConfig, TenantLayout};

mod output;

use self::output::{emit_api_key_issue, emit_api_key_list};

#[derive(Args)]
pub(super) struct ApiKeysArgs {
    #[command(subcommand)]
    command: ApiKeysCommand,
}

#[derive(Subcommand)]
enum ApiKeysCommand {
    Issue(ApiKeysIssueArgs),
    List(ApiKeysListArgs),
    Revoke(ApiKeysRevokeArgs),
    Rotate(ApiKeysRotateArgs),
}

#[derive(Args)]
struct ApiKeysIssueArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    label: Option<String>,
    #[arg(long, action = clap::ArgAction::SetTrue, default_value_t = false)]
    json: bool,
}

#[derive(Args)]
struct ApiKeysListArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long, action = clap::ArgAction::SetTrue, default_value_t = false)]
    json: bool,
}

#[derive(Args)]
struct ApiKeysRevokeArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    id: String,
    #[arg(long, action = clap::ArgAction::SetTrue, default_value_t = false)]
    json: bool,
}

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
    #[arg(long, action = clap::ArgAction::SetTrue, default_value_t = false)]
    json: bool,
}

pub(super) fn run(args: ApiKeysArgs) -> i32 {
    match args.command {
        ApiKeysCommand::Issue(args) => run_issue(args),
        ApiKeysCommand::List(args) => run_list(args),
        ApiKeysCommand::Revoke(args) => run_revoke(args),
        ApiKeysCommand::Rotate(args) => run_rotate(args),
    }
}

fn run_issue(args: ApiKeysIssueArgs) -> i32 {
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

fn run_list(args: ApiKeysListArgs) -> i32 {
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

fn run_revoke(args: ApiKeysRevokeArgs) -> i32 {
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

fn run_rotate(args: ApiKeysRotateArgs) -> i32 {
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

fn resolve_tenant_layout(
    tenant_id: &str,
    data_dir: Option<PathBuf>,
) -> Result<TenantLayout, String> {
    let base_dir = data_dir.unwrap_or_else(ServerConfig::default_data_dir);
    TenantLayout::new(base_dir, tenant_id).map_err(|err| err.to_string())
}
