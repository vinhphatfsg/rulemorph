#[test]
fn valid_rules_should_pass_validation() {
    let cases = [
        "t01_csv_basic",
        "t02_csv_no_header",
        "t03_json_out_context",
        "t04_json_root_coalesce_default",
        "t05_expr_transforms",
        "t06_lookup_context",
        "t07_array_index_paths",
        "t08_escaped_keys",
        "t09_when_mapping",
        "t10_when_compare",
        "t11_when_logical_ops",
        "t13_expr_extended",
        "t14_expr_chain",
        "t15_record_when",
        "t16_array_ops",
        "t17_json_ops_merge",
        "t18_json_ops_deep_merge",
        "t19_json_ops_pick",
        "t20_json_ops_omit",
        "t21_json_ops_keys_values_entries",
        "t22_json_ops_object_flatten",
        "t23_json_ops_object_unflatten",
        "t24_json_ops_missing",
        "t25_json_ops_get_chain",
        "t26_chain_all_ops",
        "t27_json_ops_from_entries",
        "t28_expr_chain_nested",
        "t29_json_ops_len",
        "t31_yaml_input",
        "t32_toml_input",
        "t33_xml_input",
        "t34_excel_input",
        "t35_html_input",
        "t36_spreadsheets_plugin_products",
        "t37_spreadsheets_plugin_orders",
        "t38_spreadsheets_plugin_survey",
        "t39_pyproject_dependency_inventory",
        "t40_cargo_dependency_feature_inventory",
        "t41_github_actions_matrix",
        "t42_openapi_endpoint_catalog",
        "t44_math_ops",
    ];

    for case in cases {
        let rule = load_rule(case);
        if let Err(errors) = validate_rule_file(&rule) {
            let codes: Vec<&'static str> = errors.iter().map(|e| e.code.as_str()).collect();
            panic!("expected valid rules for {}, got {:?}", case, codes);
        }
    }
}

#[test]
fn invalid_rules_should_match_expected_errors() {
    let cases = [
        "v01_missing_mapping_value",
        "v02_duplicate_target",
        "v03_invalid_ref_namespace",
        "v04_forward_out_reference",
        "v05_unknown_op",
        "v06_invalid_delimiter_length",
        "v07_invalid_lookup_args",
        "v08_invalid_path",
        "v09_invalid_when_type",
        "v10_invalid_record_when_type",
        "v11_invalid_item_ref",
        "r02_json_ops_invalid_path_pick",
    ];

    for case in cases {
        let rule = load_rule(case);
        let expected = load_expected_errors(case);
        let errors = validate_rule_file(&rule).unwrap_err();
        let actual = normalize_errors(errors);
        assert_eq!(actual, expected, "error mismatch for fixture {}", case);
    }
}

#[test]
fn invalid_rules_report_error_codes() {
    let rule = load_rule("v01_missing_mapping_value");
    let errors = validate_rule_file(&rule).unwrap_err();
    let codes: Vec<ErrorCode> = errors.iter().map(|e| e.code.clone()).collect();
    assert!(codes.contains(&ErrorCode::MissingMappingValue));
}

#[test]
fn v1_chain_concat_and_coalesce_require_explicit_operand() {
    for op in ["concat", "coalesce"] {
        let yaml = format!(
            r#"
version: 1
input:
  format: json
  json: {{}}
mappings:
  - target: value
    expr:
      chain:
        - {{ ref: input.name }}
        - op: {}
"#,
            op
        );
        let rule = parse_rule_file(&yaml).expect("rule parses");
        let errors =
            validate_rule_file(&rule).expect_err("chain op should require an explicit operand");

        assert!(
            errors.iter().any(|err| {
                err.code == ErrorCode::InvalidArgs
                    && err.path.as_deref() == Some("mappings[0].expr.chain[1].args")
            }),
            "expected InvalidArgs for {op}, got {errors:?}"
        );
    }
}

#[test]
fn out_ref_parent_object_resolves_after_nested_targets_but_missing_sibling_does_not() {
    let valid = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: base.id
    value: "u1"
  - target: base.active
    value: true
  - target: copy
    expr: "@out.base"
"#,
    )
    .expect("valid rule parses");
    validate_rule_file(&valid).expect("parent object ref should resolve after nested targets");

    let invalid = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: base.id
    value: "u1"
  - target: copy
    expr: "@out.base.missing"
"#,
    )
    .expect("invalid rule parses");
    let errors = validate_rule_file(&invalid).expect_err("missing sibling remains forward");
    assert!(
        errors.iter().any(|err| {
            err.code == ErrorCode::ForwardOutReference
                && err.path.as_deref() == Some("mappings[1].expr[0]")
        }),
        "expected ForwardOutReference for missing sibling, got {errors:?}"
    );
}

#[test]
fn out_ref_index_requires_produced_array_parent_not_nested_object() {
    let valid_array_parent = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: base
    value:
      - id: "u1"
  - target: first
    expr: "@out.base[0].id"
"#,
    )
    .expect("valid rule parses");
    validate_rule_file(&valid_array_parent)
        .expect("index ref should resolve when the array parent target was produced");

    let invalid_expr = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: base.id
    value: "u1"
  - target: first
    expr: "@out.base[0]"
"#,
    )
    .expect("invalid rule parses");
    let errors =
        validate_rule_file(&invalid_expr).expect_err("nested object target is not an array parent");
    assert!(
        errors.iter().any(|err| {
            err.code == ErrorCode::ForwardOutReference
                && err.path.as_deref() == Some("mappings[1].expr[0]")
        }),
        "expected ForwardOutReference for indexed expr, got {errors:?}"
    );

    let invalid_source = parse_rule_file(
        r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: base.id
    value: "u1"
  - target: first
    source: out.base[0]
"#,
    )
    .expect("invalid source rule parses");
    let errors = validate_rule_file(&invalid_source)
        .expect_err("source refs should keep index tokens in forward checks");
    assert!(
        errors.iter().any(|err| {
            err.code == ErrorCode::ForwardOutReference
                && err.path.as_deref() == Some("mappings[1].source")
        }),
        "expected ForwardOutReference for indexed source, got {errors:?}"
    );
}

#[test]
fn branch_validation_rejects_unresolvable_base_dir_instead_of_disabling_escape_check() {
    let outside_dir = std::env::temp_dir().join(format!(
        "rulemorph-validation-outside-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&outside_dir);
    std::fs::create_dir_all(&outside_dir).expect("create outside dir");
    let outside_rule = outside_dir.join("outside.yaml");
    std::fs::write(
        &outside_rule,
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: ok
    value: true
"#,
    )
    .expect("write outside rule");

    let yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
steps:
  - branch:
      when: {{ eq: [1, 1] }}
      then: {}
      return: false
"#,
        outside_rule.display()
    );
    let rule = parse_rule_file(&yaml).expect("rule parses");
    let missing_base = outside_dir.join("missing-base");
    let errors = validate_rule_file_with_source_and_base_dir(&rule, &yaml, &missing_base)
        .expect_err("unresolvable base dir must fail closed");

    assert!(
        errors.iter().any(|err| {
            err.code == ErrorCode::InvalidStep
                && err.path.as_deref() == Some("steps[0].branch.then")
        }),
        "expected InvalidStep for unresolvable branch base dir, got {errors:?}"
    );

    let _ = std::fs::remove_dir_all(&outside_dir);
}
