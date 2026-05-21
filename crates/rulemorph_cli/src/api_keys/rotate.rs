use std::path::PathBuf;

use clap::Args;
use rulemorph_server::ApiKeyStore;

use super::layout::resolve_tenant_layout;
use super::output::emit_api_key_issue;

#[derive(Args)]
pub(crate) struct ApiKeysRotateArgs {
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

pub(crate) fn run_rotate(args: ApiKeysRotateArgs) -> i32 {
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
