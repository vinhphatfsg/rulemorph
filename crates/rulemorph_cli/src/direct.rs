use std::path::{Path, PathBuf};

use rulemorph::{
    InputData, transform_input_with_warnings_with_base_dir_and_options, validate_rule_file,
};

use super::emit::{emit_transform_error, emit_transform_warnings, emit_validation_errors};
use super::input::{load_context, load_input_bytes_from_path_or_stdin, load_normalization_options};
use super::output::{emit_text_output, serialize_json_output};
use super::{DirectFormatArg, ErrorFormat, LimitsProfileArg};

mod input_config;
mod output_spec;
mod output_unwrap;
mod rule_build;
mod streaming;

use input_config::{
    build_direct_input_config, output_cell_record_budget, resolve_direct_input_format,
    validate_direct_options,
};
use output_spec::parse_direct_output_spec;
use output_unwrap::unwrap_direct_output;
use rule_build::build_direct_rule;
use streaming::run_direct_ndjson;

pub(super) const DIRECT_VALUE_TARGET: &str = "__rulemorph_direct_value";

pub(crate) struct DirectArgs {
    pub(crate) rule: Option<String>,
    pub(crate) fields: Vec<String>,
    pub(crate) output_map: Option<String>,
    pub(crate) input: Option<PathBuf>,
    pub(crate) format: Option<DirectFormatArg>,
    pub(crate) headers: Option<String>,
    pub(crate) excel_data_range: Option<String>,
    pub(crate) excel_header_row: Option<usize>,
    pub(crate) excel_sheet: Option<String>,
    pub(crate) excel_sheet_index: Option<usize>,
    pub(crate) output: Option<PathBuf>,
    pub(crate) ndjson: bool,
    pub(crate) error_format: Option<ErrorFormat>,
    pub(crate) limits: Vec<String>,
    pub(crate) limits_profile: Option<LimitsProfileArg>,
    pub(crate) limits_file: Option<PathBuf>,
    pub(crate) context: Option<PathBuf>,
}

pub(crate) fn run(args: DirectArgs) -> i32 {
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

    let input =
        match load_input_bytes_from_path_or_stdin(args.input.as_ref(), options.max_input_bytes) {
            Ok(value) => value,
            Err(code) => return code,
        };

    let input_format = resolve_direct_input_format(args.format, args.input.as_ref(), &input);
    if let Err(message) = validate_direct_options(&args, input_format) {
        eprintln!("{}", message);
        return 2;
    }

    let output_cell_record_budget =
        output_cell_record_budget(input_format, &input, options.max_records);
    let output_spec = match parse_direct_output_spec(&args, &options, output_cell_record_budget) {
        Ok(output_spec) => output_spec,
        Err(err) => {
            eprintln!("{}", err.message);
            return err.exit_code;
        }
    };
    let input_config = match build_direct_input_config(&args, input_format, &input, &output_spec) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{}", message);
            return 2;
        }
    };

    let rule = match build_direct_rule(&output_spec, input_format, input_config.rule_config) {
        Ok(rule) => rule,
        Err(message) => {
            eprintln!("{}", message);
            return 1;
        }
    };
    if let Err(errors) = validate_rule_file(&rule) {
        emit_validation_errors(&errors, args.error_format.unwrap_or(ErrorFormat::Text));
        return 2;
    }

    let context = match load_context(&args.context) {
        Ok(context) => context,
        Err(code) => return code,
    };

    if args.ndjson {
        return run_direct_ndjson(
            &rule,
            &input,
            context.as_ref(),
            args.output,
            args.error_format.unwrap_or(ErrorFormat::Text),
            &options,
            output_spec.output_mode(),
        );
    }

    let (output, warnings) = match transform_input_with_warnings_with_base_dir_and_options(
        &rule,
        InputData::Bytes(&input),
        context.as_ref(),
        Path::new("."),
        &options,
    ) {
        Ok(result) => result,
        Err(err) => {
            emit_transform_error(&err, args.error_format.unwrap_or(ErrorFormat::Text));
            return 3;
        }
    };

    let output = unwrap_direct_output(
        output,
        output_spec.output_mode(),
        input_config.unwrap_single_output,
    );
    let output_text = match serialize_json_output(&output) {
        Ok(text) => text,
        Err(()) => return 1,
    };

    emit_transform_warnings(&warnings, args.error_format.unwrap_or(ErrorFormat::Text));

    if emit_text_output(&output_text, args.output.as_ref()).is_err() {
        return 1;
    }

    0
}
