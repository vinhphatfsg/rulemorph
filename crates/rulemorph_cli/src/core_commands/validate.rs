use std::path::PathBuf;

use clap::Args;
use rulemorph::{legacy_v1_rule_warning, validate_rule_file_with_source_and_base_dir};

use super::super::emit::{emit_transform_warnings, emit_validation_errors};
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
        Ok(()) => {
            if let Some(warning) = legacy_v1_rule_warning(&rule) {
                emit_transform_warnings(&[warning], args.error_format);
            }
            0
        }
        Err(errors) => {
            emit_validation_errors(&errors, args.error_format);
            2
        }
    }
}
