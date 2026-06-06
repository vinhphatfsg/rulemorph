use std::path::PathBuf;

use clap::Args;
use rulemorph::validate_rule_file_with_source_and_base_dir;

use super::super::emit::emit_validation_errors;
use super::super::input::{load_rule, rule_base_dir};
use super::super::{ErrorFormat, RulesFormatArg};

#[derive(Args)]
pub(crate) struct ValidateArgs {
    #[arg(short = 'r', long)]
    rules: PathBuf,
    #[arg(long)]
    rules_format: Option<RulesFormatArg>,
    #[arg(short = 'e', long, default_value = "text")]
    error_format: ErrorFormat,
}

pub(crate) fn run_validate(args: ValidateArgs) -> i32 {
    let (rule, yaml) = match load_rule(&args.rules, args.rules_format) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let base_dir = rule_base_dir(&args.rules);
    match validate_rule_file_with_source_and_base_dir(&rule, &yaml, &base_dir) {
        Ok(()) => 0,
        Err(errors) => {
            emit_validation_errors(&errors, args.error_format);
            2
        }
    }
}
