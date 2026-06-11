use super::*;

#[test]
fn custom_op_json_contract_accepts_null() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  echo:
    input: json
    returns: json
    expr: "$"
mappings:
  - target: value
    expr:
      - "@input.value"
      - echo
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("json contract accepts any JSON value");

    let output = transform(&rule, r#"[{"value":null}]"#, None)
        .expect("json contract accepts null at runtime");

    assert_eq!(output, json!([{ "value": null }]));
}

#[test]
fn custom_op_type_contracts_cover_canonical_optional_nullable_and_wrappers() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  decorate:
    input:
      label: string
      note: { type: string, nullable: true }
      memo: { type: string, optional: true, nullable: true }
      payload: json
    returns: { label: string, note: string?, payload: json }
    mappings:
      - target: label
        expr: ["$", { get: ["label"] }]
        required: true
      - target: note
        expr: ["$", { get: ["note"] }]
        required: false
      - target: payload
        expr: ["$", { get: ["payload"] }]
        required: true
mappings:
  - target: decorated
    expr:
      - "@input.item"
      - decorate:
          - with:
              label: { value: "$.quantity" }
              note: { expr: "$.note" }
              payload:
                value: { expr: "$.quantity" }
    required: true
"#;
    let rule = parse(yaml);
    let output = transform(&rule, r#"[{"item":{"note":null}}]"#, None)
        .expect("canonical type and wrappers succeed");
    assert_eq!(
        output,
        json!([{ "decorated": { "label": "$.quantity", "note": null, "payload": { "expr": "$.quantity" } } }])
    );

    let err = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  bad:
    input: { type: string, optional: true }
    returns: string
    expr: ["$"]
mappings:
  - target: value
    expr: ["@input.value", bad]
"#,
    )
    .expect_err("top-level object contracts treat `type` as a field name");
    assert!(
        err.to_string()
            .contains("type literal must be a string, array, or object")
    );

    let err = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  bad:
    input:
      label: { type: string, optional: "true" }
    returns: string
    expr: ["$"]
mappings:
  - target: value
    expr: ["@input.value", bad]
"#,
    )
    .expect_err("canonical type options require booleans");
    assert!(
        err.to_string()
            .contains("type option `optional` must be boolean")
    );
}

#[test]
fn custom_op_object_contract_allows_type_field_name() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  item_type:
    input: { type: string }
    returns: string
    expr: ["$", { get: ["type"] }]
mappings:
  - target: kind
    expr: ["@input.item", item_type]
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("type field object contract validates");
    let output = transform(&rule, r#"[{"item":{"type":"physical"}}]"#, None)
        .expect("type field object contract transforms");

    assert_eq!(output, json!([{ "kind": "physical" }]));

    let nested_yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  item_type:
    input: { meta: { type: string } }
    returns: string
    expr: ["$", { get: ["meta.type"] }]
mappings:
  - target: kind
    expr: ["@input.item", item_type]
"#;
    let nested_rule = parse(nested_yaml);
    validate_rule_file(&nested_rule).expect("nested type field object contract validates");
    let output = transform(
        &nested_rule,
        r#"[{"item":{"meta":{"type":"physical"}}}]"#,
        None,
    )
    .expect("nested type field object contract transforms");

    assert_eq!(output, json!([{ "kind": "physical" }]));
}

#[test]
fn custom_op_type_contract_rejects_duplicate_fields_after_optional_normalization() {
    let err = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  check:
    input: { id: int, id?: string }
    returns: json
    expr: "$"
mappings:
  - target: result
    expr: ["@input", check]
"#,
    )
    .expect_err("id and id? normalize to the same object field");

    assert!(err.to_string().contains("object field `id` is duplicated"));
}

#[test]
fn custom_op_mappings_body_returns_must_be_object_or_json() {
    let invalid = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  scalar:
    input: json
    returns: string
    mappings:
      - target: value
        value: "ok"
  array:
    input: json
    returns: [string]
    mappings:
      - target: value
        value: "ok"
mappings:
  - target: scalar
    expr: ["@input", scalar]
  - target: array
    expr: ["@input", array]
"#,
    );
    let errors = validate_rule_file(&invalid).expect_err("non-object mappings returns fail");
    assert_eq!(
        errors
            .iter()
            .filter(|err| err.code == ErrorCode::InvalidTypeName
                && err
                    .message
                    .contains("custom op mappings body returns must be object or json"))
            .count(),
        2
    );

    let valid = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  typed:
    input: json
    returns: { value: string }
    mappings:
      - target: value
        value: "ok"
  broad:
    input: json
    returns: json
    mappings:
      - target: value
        value: "ok"
mappings:
  - target: typed
    expr: ["@input", typed]
  - target: broad
    expr: ["@input", broad]
"#,
    );
    validate_rule_file(&valid).expect("object and json mappings returns are valid");
}
