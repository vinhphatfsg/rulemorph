use clap::{Args, Subcommand};

mod issue;
mod layout;
mod list;
mod output;
mod revoke;
mod rotate;

use self::issue::{ApiKeysIssueArgs, run_issue};
use self::list::{ApiKeysListArgs, run_list};
use self::revoke::{ApiKeysRevokeArgs, run_revoke};
use self::rotate::{ApiKeysRotateArgs, run_rotate};

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

pub(super) fn run(args: ApiKeysArgs) -> i32 {
    match args.command {
        ApiKeysCommand::Issue(args) => run_issue(args),
        ApiKeysCommand::List(args) => run_list(args),
        ApiKeysCommand::Revoke(args) => run_revoke(args),
        ApiKeysCommand::Rotate(args) => run_rotate(args),
    }
}
