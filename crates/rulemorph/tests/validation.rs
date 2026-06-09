use std::fs;

use rulemorph::{
    ErrorCode, RuleFormat, parse_rule_file, parse_rule_file_with_format, validate_rule_file,
    validate_rule_file_with_source, validate_rule_file_with_source_and_base_dir,
};

mod common;

use common::validation::{fixtures_dir, load_expected_errors, load_rule, normalize_errors};

include!("validation/core.rs");

include!("validation/input_format.rs");

include!("validation/rule_format.rs");

#[test]
fn typed_value_codecs_are_validated_statically() {
    let version_one_codec_rule = parse_rule_file(
        r#"
version: 1
input:
  format: json
  json: {}
codecs:
  ddb:
    profile: dynamodb_item
mappings:
  - target: x
    source: id
"#,
    )
    .expect("parse rule");
    let errors =
        validate_rule_file(&version_one_codec_rule).expect_err("codecs are rejected for version 1");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidStep
            && err.path.as_deref() == Some("codecs")
            && err
                .message
                .contains("codecs is only supported in version 2")
    }));

    let typo_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
codecs:
  bad:
    profile: dynamodb_item
    field_type:
      tags: string_set
mappings:
  - target: x
    value: 1
"#,
    )
    .expect("parse rule");
    let errors = validate_rule_file(&typo_rule).expect_err("codec typo should fail validation");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape
            && err.message.contains("unknown typed value option")
    }));

    let missing_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: x
    expr:
      - "@input"
      - to_typed_value:
          codec: missing
"#,
    )
    .expect("parse rule");
    let errors =
        validate_rule_file(&missing_rule).expect_err("unknown codec should fail validation");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape && err.message.contains("unknown codec binding")
    }));

    let dynamic_options_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: x
    expr:
      - "@input"
      - op: to_typed_value
        args:
          - - "@input"
            - to_typed_value:
                codec: missing
"#,
    )
    .expect("parse rule");
    let errors = validate_rule_file(&dynamic_options_rule)
        .expect_err("dynamic option expressions should still scan nested codec refs");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape && err.message.contains("unknown codec binding")
    }));

    let string_codec_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
codecs:
  ddb: dynamodb_item
mappings:
  - target: x
    value: 1
"#,
    )
    .expect("parse rule");
    let errors = validate_rule_file(&string_codec_rule)
        .expect_err("codec bindings must fail closed on non-object values");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape
            && err.message.contains("codec binding must be an object")
    }));

    let missing_profile_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
codecs:
  bad:
    field_types:
      tags: string_set
mappings:
  - target: x
    expr:
      - "@input"
      - to_typed_value: {}
"#,
    )
    .expect("parse rule");
    let errors = validate_rule_file(&missing_profile_rule)
        .expect_err("codec options without profile or codec should fail validation");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape && err.message.contains("require profile or codec")
    }));

    let invalid_codec_ref_type_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: x
    expr:
      - "@input"
      - to_typed_value:
          codec: 123
"#,
    )
    .expect("parse rule");
    let errors =
        validate_rule_file(&invalid_codec_ref_type_rule).expect_err("codec refs must be strings");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape && err.message.contains("codec must be a string")
    }));

    let invalid_shorthand_profile_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: x
    expr:
      - "@input"
      - to_typed_value: dynamodb_itme
"#,
    )
    .expect("parse rule");
    let errors = validate_rule_file(&invalid_shorthand_profile_rule)
        .expect_err("string shorthand profile must be validated");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape
            && err.message.contains("unknown typed value profile")
    }));

    let nested_codec_binding_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
codecs:
  base:
    profile: dynamodb_item
  bad:
    codec: base
mappings:
  - target: x
    value: 1
"#,
    )
    .expect("parse rule");
    let errors = validate_rule_file(&nested_codec_binding_rule)
        .expect_err("codec bindings must not reference other codecs");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape
            && err
                .message
                .contains("codec binding cannot reference another codec")
    }));

    let invalid_value_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
codecs:
  bad:
    profile: dynamodb_itme
    decode:
      mode: typo
    field_types:
      id: unknown_type
mappings:
  - target: x
    value: 1
"#,
    )
    .expect("parse rule");
    let errors = validate_rule_file(&invalid_value_rule)
        .expect_err("codec option values should fail validation");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape
            && err.message.contains("unknown typed value profile")
    }));
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape && err.message.contains("unsupported decode mode")
    }));
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape && err.message.contains("unsupported field type")
    }));

    let mut field_types = String::new();
    for index in 0..500 {
        field_types.push_str(&format!("      f{}: string_set\n", index));
    }
    let mut hints = String::new();
    for index in 0..500 {
        hints.push_str(&format!(
            "      - path: h{}\n        type: string_set\n",
            index
        ));
    }
    let mut number_strings = String::new();
    for index in 0..25 {
        number_strings.push_str(&format!("      - n{}\n", index));
    }
    let too_many_hints_yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
codecs:
  too_many:
    profile: dynamodb_item
    field_types:
{}    hints:
{}    number_strings:
{}mappings:
  - target: x
    value: 1
"#,
        field_types, hints, number_strings
    );
    let too_many_hints_rule = parse_rule_file(&too_many_hints_yaml).expect("parse rule");
    let errors = validate_rule_file(&too_many_hints_rule)
        .expect_err("combined hint count should fail validation");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape && err.message.contains("hint count")
    }));
}

#[test]
fn typed_value_codec_validation_matches_runtime_hint_and_profile_rules() {
    let valid_quoted_path_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
codecs:
  ddb:
    profile: dynamodb_item
    field_types:
      m["x..y"]: string_set
mappings:
  - target: x
    value: 1
"#,
    )
    .expect("parse rule");
    validate_rule_file(&valid_quoted_path_rule)
        .expect("quoted hint paths with dots should validate");

    let invalid_bracket_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
codecs:
  ddb:
    profile: dynamodb_item
    field_types:
      a[0]: string_set
mappings:
  - target: x
    value: 1
"#,
    )
    .expect("parse rule");
    let errors = validate_rule_file(&invalid_bracket_rule)
        .expect_err("numeric hint index should fail static validation");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape
            && err.message.contains("invalid hint path bracket syntax")
    }));

    let duplicate_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
codecs:
  ddb:
    profile: dynamodb_item
    field_types:
      tags: string_set
    hints:
      - path: tags
        type: string_set
mappings:
  - target: x
    value: 1
"#,
    )
    .expect("parse rule");
    let errors =
        validate_rule_file(&duplicate_rule).expect_err("duplicate hints should fail validation");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape && err.message.contains("duplicate field type path")
    }));

    let non_mongo_policy_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
codecs:
  ddb:
    profile: dynamodb_item
    allow_dollar_prefixed_fields: true
mappings:
  - target: x
    value: 1
"#,
    )
    .expect("parse rule");
    let errors = validate_rule_file(&non_mongo_policy_rule)
        .expect_err("Mongo-only policy should fail on non-Mongo profile");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape
            && err
                .message
                .contains("MongoDB options require mongo_extended_json profile")
    }));

    let wrong_provider_type_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
codecs:
  ddb:
    profile: dynamodb_item
    field_types:
      id: object_id
mappings:
  - target: x
    value: 1
"#,
    )
    .expect("parse rule");
    let errors = validate_rule_file(&wrong_provider_type_rule)
        .expect_err("profile-incompatible hint type should fail validation");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape
            && err
                .message
                .contains("field type is not supported by dynamodb_item profile")
    }));

    let wrong_provider_type_override_rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
codecs:
  ddb:
    profile: dynamodb_item
mappings:
  - target: x
    expr:
      - "@input"
      - to_typed_value:
          codec: ddb
          field_types:
            id: object_id
"#,
    )
    .expect("parse rule");
    let errors = validate_rule_file(&wrong_provider_type_override_rule)
        .expect_err("profile-incompatible override should fail validation");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape
            && err
                .message
                .contains("field type is not supported by dynamodb_item profile")
    }));
}

#[test]
fn typed_value_codec_refs_are_validated_across_v2_conditions_and_defs() {
    let cases = [
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: x
    expr:
      - "@input"
      - if:
          cond:
            eq:
              - ["@input", { to_typed_value: { codec: missing } }]
              - {}
          then: ["$", { get: id }]
          else: ["lit:none"]
"#,
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  encode:
    input: json
    returns: json
    expr:
      - "@input"
      - to_typed_value:
          codec: missing
mappings:
  - target: item
    expr:
      - "@input"
      - encode:
          - with: {}
"#,
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: id
    expr: "@input.id"
finalize:
  filter:
    eq:
      - ["@item", { to_typed_value: { codec: missing } }]
      - {}
"#,
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: ids
    expr:
      - "@input.items"
      - map:
          - if:
              cond:
                eq:
                  - ["@item", { to_typed_value: { codec: missing } }]
                  - {}
              then: ["@item"]
              else: ["lit:none"]
"#,
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: id
    expr: "@input.id"
finalize:
  wrap:
    item:
      - "@out"
      - to_typed_value:
          codec: missing
"#,
    ];

    for yaml in cases {
        let rule = parse_rule_file(yaml).expect("parse rule");
        let errors = validate_rule_file(&rule).expect_err("unknown codec should fail validation");
        assert!(
            errors.iter().any(|err| {
                err.code == ErrorCode::InvalidExprShape
                    && err.message.contains("unknown codec binding")
            }),
            "expected unknown codec validation error, got {errors:?}",
        );
    }
}

#[test]
fn typed_value_codec_refs_are_validated_in_v2_steps_conditions() {
    let cases = [
        r#"
version: 2
input:
  format: json
  json: {}
steps:
  - record_when:
      eq:
        - ["@input", { to_typed_value: { codec: missing } }]
        - {}
    mappings:
      - target: x
        value: 1
"#,
        r#"
version: 2
input:
  format: json
  json: {}
steps:
  - asserts:
      - when:
          eq:
            - ["@input", { to_typed_value: { codec: missing } }]
            - {}
        error:
          code: INVALID
          message: invalid
    mappings:
      - target: x
        value: 1
"#,
        r#"
version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when:
        eq:
          - ["@input", { to_typed_value: { codec: missing } }]
          - {}
      then: tests/fixtures/tv34_branch_return_true/then.yaml
"#,
    ];

    for yaml in cases {
        let rule = parse_rule_file(yaml).expect("parse rule");
        let errors = validate_rule_file(&rule).expect_err("unknown codec should fail validation");
        assert!(
            errors.iter().any(|err| {
                err.code == ErrorCode::InvalidExprShape
                    && err.message.contains("unknown codec binding")
            }),
            "expected unknown codec validation error, got {errors:?}",
        );
    }
}

// =============================================================================
// v2 Validation Tests
// =============================================================================

include!("validation/v2.rs");
