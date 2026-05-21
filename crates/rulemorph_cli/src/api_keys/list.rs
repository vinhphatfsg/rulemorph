use std::path::PathBuf;

use clap::Args;
use rulemorph_server::ApiKeyStore;

use super::layout::resolve_tenant_layout;
use super::output::emit_api_key_list;

#[derive(Args)]
pub(crate) struct ApiKeysListArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long, action = clap::ArgAction::SetTrue, default_value_t = false)]
    json: bool,
}

pub(crate) fn run_list(args: ApiKeysListArgs) -> i32 {
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
