use std::path::PathBuf;

use clap::Args;
use rulemorph::{InputData, preflight_validate_input_with_warnings_with_base_dir_and_options};

use super::super::emit::{emit_transform_error, emit_transform_warnings};
use super::super::input::{
    apply_format_override, load_context, load_input_bytes_with_limit, load_normalization_options,
    load_rule, rule_base_dir,
};
use super::super::{ErrorFormat, FormatOverride, LimitsProfileArg, RulesFormatArg};

#[derive(Args)]
pub(crate) struct PreflightArgs {
    #[arg(short = 'r', long)]
    rules: PathBuf,
    #[arg(long)]
    rules_format: Option<RulesFormatArg>,
    #[arg(short = 'i', long)]
    input: PathBuf,
    #[arg(short = 'f', long)]
    format: Option<FormatOverride>,
    #[arg(short = 'c', long)]
    context: Option<PathBuf>,
    #[arg(short = 'e', long, default_value = "text")]
    error_format: ErrorFormat,
    #[arg(long = "limit")]
    limits: Vec<String>,
    #[arg(long, value_enum)]
    limits_profile: Option<LimitsProfileArg>,
    #[arg(long)]
    limits_file: Option<PathBuf>,
}

pub(crate) fn run_preflight(args: PreflightArgs) -> i32 {
    let (mut rule, _) = match load_rule(&args.rules, args.rules_format) {
        Ok(value) => value,
        Err(code) => return code,
    };

    apply_format_override(&mut rule, args.format);

    let options = match load_normalization_options(
        args.limits_profile,
        args.limits_file.as_ref(),
        &args.limits,
    ) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{}", message);
            return 2;
        }
    };

    let input = match load_input_bytes_with_limit(&args.input, options.max_input_bytes) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let context_value = match load_context(&args.context) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let base_dir = rule_base_dir(&args.rules);
    let warnings = match preflight_validate_input_with_warnings_with_base_dir_and_options(
        &rule,
        InputData::Bytes(&input),
        context_value.as_ref(),
        &base_dir,
        &options,
    ) {
        Ok(warnings) => warnings,
        Err(err) => {
            emit_transform_error(&err, args.error_format);
            return 3;
        }
    };

    emit_transform_warnings(&warnings, args.error_format);

    0
}
