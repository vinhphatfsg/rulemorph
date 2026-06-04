use rulemorph::{
    InputData, TransformErrorKind, parse_rule_file, transform_input, transform_input_with_options,
    validate_rule_file,
};

fn transform_json(rule_yaml: &str, input: &str) -> serde_json::Value {
    let rule = parse_rule_file(rule_yaml).expect("parse rule");
    let output = transform_input(&rule, InputData::Text(input), None).expect("transform");
    output
        .as_array()
        .and_then(|items| items.first())
        .cloned()
        .unwrap_or(output)
}

fn transform_err(rule_yaml: &str, input: &str) -> String {
    let rule = parse_rule_file(rule_yaml).expect("parse rule");
    let err = transform_input(&rule, InputData::Text(input), None).expect_err("transform error");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    err.message
}

mod dynamodb {
    use super::*;
    include!("typed_value_codec/dynamodb.rs");
}

mod firestore_phase4 {
    use super::*;
    include!("typed_value_codec/firestore_phase4.rs");
}

mod mongo_phase5 {
    use super::*;
    include!("typed_value_codec/mongo_phase5.rs");
}

mod options_validation {
    use super::*;
    include!("typed_value_codec/options_validation.rs");
}

mod provider_decode_errors {
    use super::*;
    include!("typed_value_codec/provider_decode_errors.rs");
}

mod provider_constraints {
    use super::*;
    include!("typed_value_codec/provider_constraints.rs");
}

mod roundtrip {
    use super::*;
    include!("typed_value_codec/roundtrip.rs");
}

mod resource_and_edge_cases {
    use super::*;
    include!("typed_value_codec/resource_and_edge_cases.rs");
}
