use proptest::prelude::*;
use rulemorph_output::*;
use serde_json::{Value, json};

const RULE: &str = "version: 2\ninput: {format: json, json: {}}\nmappings:\n  - target: text\n    source: text\n    type: string\n    required: true\nfinalize:\n  wrap: ['@out', {map: ['@item.text']}]\n";
fn complete(text: &str) -> RawInput<'_> {
    RawInput {
        text,
        completion: InputCompletion::Complete,
    }
}
fn schema() -> Value {
    json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"array","items":{"type":"string"}})
}

#[test]
fn fixture_repair_is_conservative_and_offsets_are_utf8_bytes() {
    let cases: Value = serde_json::from_str(include_str!("fixtures/repair_cases.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let result = repair_json_candidate(case["raw"].as_str().unwrap());
        if case["ok"] == true {
            let candidate = result.unwrap_or_else(|e| panic!("{}: {e}", case["name"]));
            assert_eq!(
                serde_json::from_str::<Value>(&candidate.text).unwrap(),
                case["expected"],
                "{}",
                case["name"]
            );
            assert_eq!(
                json!(
                    candidate
                        .repairs
                        .iter()
                        .map(|edit| edit.byte_offset)
                        .collect::<Vec<_>>()
                ),
                case["offsets"],
                "{}",
                case["name"]
            );
            assert!(candidate.repairs.iter().all(|edit| edit.inserted == ","));
        } else {
            let error = result.expect_err(case["name"].as_str().unwrap());
            assert_eq!(
                format!("{:?}", error.kind),
                case["kind"].as_str().unwrap(),
                "{}",
                case["name"]
            );
        }
    }
}

#[test]
fn explicit_text_projection_does_not_guess_title_or_description() {
    let result = normalize_to_schema(
        complete(
            r#"[{"text":"first","title":"wrong","description":"also wrong"},{"text":"第二"}]"#,
        ),
        RULE,
        &schema(),
    )
    .unwrap();
    assert_eq!(result.value(), &json!(["first", "第二"]));
    assert!(result.repairs().is_empty());
    let error =
        normalize_to_schema(complete(r#"[{"title":"not text"}]"#), RULE, &schema()).unwrap_err();
    assert_eq!(error.kind, OutputErrorKind::Transform);
    assert!(!error.to_string().contains("not text"));
}

#[test]
fn repair_requires_opt_in_and_trace_survives_schema_failure() {
    let raw = r#"[{"text":"a"} {"text":"b"}]"#;
    let rejected = normalize_to_schema(complete(raw), RULE, &schema()).unwrap_err();
    assert_eq!(rejected.kind, OutputErrorKind::InputSyntax);
    assert_eq!(rejected.repairs.len(), 1);
    let success =
        normalize_to_schema_with_repair(complete(raw), RULE, &schema(), RepairMode::MissingCommas)
            .unwrap();
    assert_eq!(success.value(), &json!(["a", "b"]));
    assert_eq!(success.repairs().len(), 1);
    let bad_schema = json!({"type":"array","items":{"type":"integer"}});
    let failure = normalize_to_schema_with_repair(
        complete(raw),
        RULE,
        &bad_schema,
        RepairMode::MissingCommas,
    )
    .unwrap_err();
    assert_eq!(failure.kind, OutputErrorKind::OutputValidation);
    assert_eq!(failure.repairs.len(), 1);
    assert!(!failure.diagnostics.is_empty());
}

#[test]
fn explicit_numeric_conversion_and_missing_required_values() {
    let rules = "version: 2\ninput: {format: json, json: {}}\nmappings:\n  - target: n\n    source: n\n    type: int\n    required: true\n";
    let schema = json!({"type":"array","items":{"type":"object","required":["n"],"properties":{"n":{"type":"integer","minimum":1}},"additionalProperties":false}});
    assert_eq!(
        normalize_to_schema(complete(r#"[{"n":"2"}]"#), rules, &schema)
            .unwrap()
            .value(),
        &json!([{"n":2}])
    );
    assert!(normalize_to_schema(complete(r#"[{}]"#), rules, &schema).is_err());
    assert!(normalize_to_schema(complete(r#"[{"n":null}]"#), rules, &schema).is_err());
    assert!(normalize_to_schema(complete(r#"[{"n":"nonsense"}]"#), rules, &schema).is_err());
}

#[test]
fn unsupported_keywords_formats_and_refs_never_silently_pass() {
    for schema in [
        json!({"$ref":"https://evil.test/schema"}),
        json!({"$ref":"file:///etc/passwd"}),
        json!({"$ref":"#/properties/a"}),
        json!({"format":"email"}),
        json!({"pattern":"(a+)+$"}),
        json!({"typo":"string"}),
        json!({"$schema":"http://json-schema.org/draft-07/schema#"}),
        json!({"properties":{"x":{"format":"bogus"}}}),
    ] {
        assert_eq!(
            validate_output(&json!({}), &schema).unwrap_err().kind,
            OutputErrorKind::UnsupportedSchema
        );
    }
    assert_eq!(
        validate_output(&json!(1), &json!({"type":123}))
            .unwrap_err()
            .kind,
        OutputErrorKind::InvalidSchema
    );
    // Property names and enum values are data, not schema positions.
    validate_output(
        &json!({"format":"email"}),
        &json!({"type":"object","properties":{"format":{"enum":["email"]}}}),
    )
    .unwrap();
}

#[test]
fn complete_final_value_is_validated_with_no_prefix_success() {
    assert_eq!(
        validate_output(&json!(["ok", 5]), &schema())
            .unwrap_err()
            .kind,
        OutputErrorKind::OutputValidation
    );
    validate_output(
        &json!({"a":2}),
        &json!({"allOf":[{"type":"object"},{"required":["a"],"properties":{"a":{"minimum":1}}}]}),
    )
    .unwrap();
    assert!(validate_output(&json!({"a":0}), &json!({"properties":{"a":{"minimum":1}}})).is_err());
    assert!(validate_output(&json!(1), &json!(false)).is_err());
    validate_output(&json!(1), &json!(true)).unwrap();
}

#[test]
fn truncated_or_unknown_generation_is_never_accepted_even_if_json_is_valid() {
    for completion in [InputCompletion::Unknown, InputCompletion::Truncated] {
        assert_eq!(
            normalize_to_schema(
                RawInput {
                    text: r#"[{"text":"ok"}]"#,
                    completion
                },
                RULE,
                &schema()
            )
            .unwrap_err()
            .kind,
            OutputErrorKind::IncompleteInput
        );
    }
    assert_eq!(
        normalize_to_schema_with_repair(
            complete(r#"[{"text":"ok"}"#),
            RULE,
            &schema(),
            RepairMode::MissingCommas
        )
        .unwrap_err()
        .kind,
        OutputErrorKind::IncompleteInput
    );
}

#[test]
fn size_depth_processing_and_output_limits_fail_instead_of_truncating() {
    assert_eq!(
        repair_json_candidate(&" ".repeat(65537)).unwrap_err().kind,
        OutputErrorKind::LimitExceeded
    );
    let nested = format!("{}0{}", "[".repeat(34), "]".repeat(34));
    assert_eq!(
        repair_json_candidate(&nested).unwrap_err().kind,
        OutputErrorKind::LimitExceeded
    );
    assert_eq!(
        validate_output(&json!("x".repeat(65537)), &json!(true))
            .unwrap_err()
            .kind,
        OutputErrorKind::LimitExceeded
    );
    let values = json!(vec![json!({"text":"x"}); 129]);
    assert!(normalize_to_schema(complete(&values.to_string()), RULE, &schema()).is_err());
    let mappings = (0..64)
        .map(|i| json!({"target":format!("v{i}"),"source":"text"}))
        .collect::<Vec<_>>();
    let rules =
        json!({"version":2,"input":{"format":"json","json":{}},"mappings":mappings}).to_string();
    assert_eq!(
        normalize_to_schema(
            complete(&json!([{"text":"x".repeat(2048)}]).to_string()),
            &rules,
            &json!(true)
        )
        .unwrap_err()
        .kind,
        OutputErrorKind::LimitExceeded
    );
}

#[test]
fn file_branches_expressions_and_record_truncation_are_not_available() {
    for rules in [
        RULE.replace("required: true", "required: false"),
        RULE.replace("target: text", "target: text[999999999]"),
        RULE.replace("source: text", "expr: [0, {range: [0, 10000000]}]"),
        RULE.replace("wrap:", "limit: 1\n  wrap:"),
    ] {
        assert!(normalize_to_schema(complete(r#"[{"text":"x"}]"#), &rules, &schema()).is_err());
    }
    let branch = "version: 2\ninput: {format: json, json: {}}\nsteps:\n - branch: {when: true, then: ../../secret.yaml}";
    assert_eq!(
        normalize_to_schema(complete("[]"), branch, &schema())
            .unwrap_err()
            .kind,
        OutputErrorKind::UnsupportedRules
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn every_success_contains_exactly_the_explicit_fields_and_satisfies_schema(values in prop::collection::vec("[^\\p{C}]{0,40}",0..16)) {
        let input = Value::Array(values.iter().map(|text| json!({"text":text,"title":"never select this"})).collect());
        let result = normalize_to_schema(complete(&input.to_string()),RULE,&schema()).unwrap();
        prop_assert_eq!(result.value(), &json!(values));
        prop_assert!(jsonschema::options().with_draft(jsonschema::Draft::Draft202012).offline().build(&schema()).unwrap().is_valid(result.value()));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn arbitrary_unicode_input_never_panics_or_returns_non_json_candidate(raw in ".{0,1024}") {
        if let Ok(candidate) = repair_json_candidate(&raw) {
            prop_assert!(rulemorph::serde_guard::parse_json_value_strict(&candidate.text).is_ok());
            let mut reconstructed = candidate.text.clone();
            // All edits are insertions: reverse removal exactly recovers original bytes.
            for (index, edit) in candidate.repairs.iter().enumerate().rev() {
                reconstructed.remove(edit.byte_offset + index);
            }
            prop_assert_eq!(reconstructed, raw);
        }
    }
}
