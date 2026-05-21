use std::path::PathBuf;

use clap::Args;
use rulemorph_server::ApiKeyStore;

use super::layout::resolve_tenant_layout;
use super::output::emit_api_key_issue;

#[derive(Args)]
pub(crate) struct ApiKeysIssueArgs {
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    label: Option<String>,
    #[arg(long, action = clap::ArgAction::SetTrue, default_value_t = false)]
    json: bool,
}

pub(crate) fn run_issue(args: ApiKeysIssueArgs) -> i32 {
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
