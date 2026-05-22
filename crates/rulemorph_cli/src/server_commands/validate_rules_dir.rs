use std::path::PathBuf;

use clap::Args;
use rulemorph_server::validate_rules_dir;

use super::super::ErrorFormat;
use super::super::emit::emit_rules_dir_errors;

#[derive(Args)]
pub(crate) struct ValidateRulesDirArgs {
    #[arg(short = 'r', long)]
    rules_dir: PathBuf,
    #[arg(short = 'e', long, default_value = "text")]
    error_format: ErrorFormat,
}

pub(crate) fn run_validate_rules_dir(args: ValidateRulesDirArgs) -> i32 {
    match validate_rules_dir(&args.rules_dir) {
        Ok(()) => 0,
        Err(errs) => {
            emit_rules_dir_errors(&errs, args.error_format);
            2
        }
    }
}
