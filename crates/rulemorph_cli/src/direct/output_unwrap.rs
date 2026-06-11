use super::DIRECT_VALUE_TARGET;
use super::output_spec::DirectOutputMode;

pub(super) fn unwrap_direct_output(
    output: serde_json::Value,
    output_mode: DirectOutputMode,
    unwrap_single_output: bool,
) -> serde_json::Value {
    match output {
        serde_json::Value::Array(records) if unwrap_single_output && records.len() == 1 => {
            unwrap_direct_record(records.into_iter().next().unwrap(), output_mode)
        }
        serde_json::Value::Array(records) => serde_json::Value::Array(
            records
                .into_iter()
                .map(|record| unwrap_direct_record(record, output_mode))
                .collect(),
        ),
        value => value,
    }
}

pub(super) fn unwrap_direct_record(
    record: serde_json::Value,
    output_mode: DirectOutputMode,
) -> serde_json::Value {
    match output_mode {
        DirectOutputMode::RecordObject => record,
        DirectOutputMode::Value => match record {
            serde_json::Value::Object(mut object) => object
                .remove(DIRECT_VALUE_TARGET)
                .unwrap_or(serde_json::Value::Null),
            value => value,
        },
    }
}
