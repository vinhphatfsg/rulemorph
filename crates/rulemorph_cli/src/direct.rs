use std::path::{Path, PathBuf};

use rulemorph::{
    InputData, RuleFormat, parse_rule_file_with_format,
    transform_input_with_warnings_with_base_dir_and_options,
};

use super::emit::{emit_transform_error, emit_transform_warnings};
use super::input::{load_input_bytes_from_path_or_stdin, load_normalization_options};
use super::output::{emit_text_output, serialize_json_output};
use super::{ErrorFormat, FormatOverride, LimitsProfileArg};

const DIRECT_VALUE_TARGET: &str = "__rulemorph_direct_value";

pub(crate) struct DirectArgs {
    pub(crate) rule: String,
    pub(crate) input: Option<PathBuf>,
    pub(crate) format: Option<FormatOverride>,
    pub(crate) output: Option<PathBuf>,
    pub(crate) error_format: Option<ErrorFormat>,
    pub(crate) limits: Vec<String>,
    pub(crate) limits_profile: Option<LimitsProfileArg>,
    pub(crate) limits_file: Option<PathBuf>,
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
    let unwrap_single_output = should_unwrap_single_output(&input, args.format);

    let rule = match build_direct_rule(&args.rule, args.format) {
        Ok(rule) => rule,
        Err(message) => {
            eprintln!("{}", message);
            return 1;
        }
    };

    let (output, warnings) = match transform_input_with_warnings_with_base_dir_and_options(
        &rule,
        InputData::Bytes(&input),
        None,
        Path::new("."),
        &options,
    ) {
        Ok(result) => result,
        Err(err) => {
            emit_transform_error(&err, args.error_format.unwrap_or(ErrorFormat::Text));
            return 3;
        }
    };

    let output = unwrap_direct_output(output, unwrap_single_output);
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

fn build_direct_rule(
    inline_rule: &str,
    format: Option<FormatOverride>,
) -> Result<rulemorph::RuleFile, String> {
    let input_format = match format.unwrap_or(FormatOverride::Json) {
        FormatOverride::Csv => "csv",
        FormatOverride::Json => "json",
    };
    let expr = parse_inline_expr(inline_rule);
    let rule_json = serde_json::json!({
        "version": 2,
        "input": {
            "format": input_format,
            input_format: {}
        },
        "mappings": [
            {
                "target": DIRECT_VALUE_TARGET,
                "expr": expr
            }
        ]
    });
    let source = serde_json::to_string(&rule_json)
        .map_err(|err| format!("failed to encode inline rule: {}", err))?;
    parse_rule_file_with_format(&source, RuleFormat::Json)
        .map_err(|err| format!("failed to parse inline rule: {}", err))
}

fn parse_inline_expr(inline_rule: &str) -> serde_json::Value {
    serde_json::from_str(inline_rule)
        .unwrap_or_else(|_| serde_json::Value::String(inline_rule.to_string()))
}

fn should_unwrap_single_output(input: &[u8], format: Option<FormatOverride>) -> bool {
    match format.unwrap_or(FormatOverride::Json) {
        FormatOverride::Csv => false,
        FormatOverride::Json => serde_json::from_slice::<serde_json::Value>(input)
            .map(|value| matches!(value, serde_json::Value::Object(_)))
            .unwrap_or(false),
    }
}

fn unwrap_direct_output(
    output: serde_json::Value,
    unwrap_single_output: bool,
) -> serde_json::Value {
    match output {
        serde_json::Value::Array(records) if unwrap_single_output && records.len() == 1 => {
            unwrap_direct_record(records.into_iter().next().unwrap())
        }
        serde_json::Value::Array(records) => {
            serde_json::Value::Array(records.into_iter().map(unwrap_direct_record).collect())
        }
        value => value,
    }
}

fn unwrap_direct_record(record: serde_json::Value) -> serde_json::Value {
    match record {
        serde_json::Value::Object(mut object) => object
            .remove(DIRECT_VALUE_TARGET)
            .unwrap_or(serde_json::Value::Null),
        value => value,
    }
}
