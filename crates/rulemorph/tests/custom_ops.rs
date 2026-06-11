use rulemorph::{
    CustomOpDef, DtoLanguage, ErrorCode, Expr, InputData, Mapping, RuleType, RuleTypeKind,
    TraceAttributeValue, TraceEventKind, TransformErrorKind, TransformTraceOptions, generate_dto,
    parse_rule_file, transform, transform_input_with_trace, validate_rule_file,
};
use serde_json::{Value as JsonValue, json};

fn parse(yaml: &str) -> rulemorph::RuleFile {
    parse_rule_file(yaml).expect("rule parses")
}

fn json_rule_type() -> RuleType {
    RuleType {
        kind: RuleTypeKind::Json,
        nullable: false,
    }
}

fn nested_with_custom_call(index: usize, call_count: usize) -> JsonValue {
    if index == call_count {
        return json!("@input.seed");
    }

    let mut with_fields = serde_json::Map::new();
    with_fields.insert(
        "next".to_string(),
        nested_with_custom_call(index + 1, call_count),
    );
    let mut with_option = serde_json::Map::new();
    with_option.insert("with".to_string(), JsonValue::Object(with_fields));
    let mut call = serde_json::Map::new();
    call.insert(
        format!("d{}", index),
        JsonValue::Array(vec![JsonValue::Object(with_option)]),
    );
    JsonValue::Object(call)
}

#[path = "custom_ops/calls.rs"]
mod calls;
#[path = "custom_ops/contracts.rs"]
mod contracts;
#[path = "custom_ops/dto.rs"]
mod dto;
#[path = "custom_ops/trace.rs"]
mod trace;
#[path = "custom_ops/v1_compat.rs"]
mod v1_compat;
#[path = "custom_ops/validation.rs"]
mod validation;
