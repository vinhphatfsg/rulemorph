use std::path::PathBuf;

use clap::Args;
use rulemorph::validate_rule_file_with_source;

use super::super::emit::emit_validation_errors;
use super::super::input::load_rule;
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

    match validate_rule_file_with_source(&rule, &yaml) {
        Ok(()) => 0,
        Err(errors) => {
            emit_validation_errors(&errors, args.error_format);
            2
        }
    }
}
