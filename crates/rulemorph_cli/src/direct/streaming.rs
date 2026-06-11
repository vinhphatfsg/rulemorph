use std::io::Write;
use std::path::{Path, PathBuf};

use rulemorph::{
    InputData, NormalizationOptions, RuleFile, transform_stream_input_with_base_dir_and_options,
};

use super::super::ErrorFormat;
use super::super::emit::{emit_transform_error, emit_transform_warnings};
use super::super::output::{create_output_writer, write_json_line};
use super::output_spec::DirectOutputMode;
use super::output_unwrap::unwrap_direct_record;

pub(super) fn run_direct_ndjson(
    rule: &RuleFile,
    input: &[u8],
    context: Option<&serde_json::Value>,
    output: Option<PathBuf>,
    error_format: ErrorFormat,
    options: &NormalizationOptions,
    output_mode: DirectOutputMode,
) -> i32 {
    let stream = match transform_stream_input_with_base_dir_and_options(
        rule,
        InputData::Bytes(input),
        context,
        Path::new("."),
        options,
    ) {
        Ok(stream) => stream,
        Err(err) => {
            emit_transform_error(&err, error_format);
            return 3;
        }
    };

    let mut writer = match create_output_writer(output.as_ref()) {
        Ok(writer) => writer,
        Err(()) => return 1,
    };

    for item in stream {
        let item = match item {
            Ok(item) => item,
            Err(err) => {
                emit_transform_error(&err, error_format);
                return 3;
            }
        };

        emit_transform_warnings(&item.warnings, error_format);

        let output = match item.output {
            Some(output) => output,
            None => continue,
        };
        let output = unwrap_direct_record(output, output_mode);
        if write_json_line(&mut writer, &output).is_err() {
            return 1;
        }
    }

    if let Err(err) = writer.flush() {
        eprintln!("failed to write output: {}", err);
        return 1;
    }

    0
}
