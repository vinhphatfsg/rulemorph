use std::path::PathBuf;

use clap::Args;
use rulemorph_server::ApiKeyStore;

use super::layout::resolve_tenant_layout;

#[derive(Args)]
pub(crate) struct ApiKeysRevokeArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    id: String,
    #[arg(long, action = clap::ArgAction::SetTrue, default_value_t = false)]
    json: bool,
}

pub(crate) fn run_revoke(args: ApiKeysRevokeArgs) -> i32 {
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
