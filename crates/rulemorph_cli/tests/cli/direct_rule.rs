#[test]
fn direct_rule_reads_json_from_stdin() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-rule")
            .arg("@input.test")
            .write_stdin(r#"{ "test": 1 }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "1\n");
}

#[test]
fn direct_rule_accepts_equals_alias() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-rule=@input.test")
            .write_stdin(r#"{ "test": 1 }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "1\n");
}

#[test]
fn direct_rule_preserves_singleton_json_array_shape() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.test")
            .write_stdin(r#"[{ "test": 1 }]"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "[1]\n");
}

#[test]
fn direct_rule_outputs_null_for_missing_object_value() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.missing")
            .write_stdin(r#"{ "test": 1 }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "null\n");
}

#[test]
fn direct_rule_outputs_null_items_for_missing_array_values() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.missing")
            .write_stdin(r#"[{ "test": 1 }, { "test": 2 }]"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "[null,null]\n");
}

#[test]
fn direct_rule_distinguishes_missing_from_empty_object() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule").arg("{}").write_stdin(r#"{ "test": 1 }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "{}\n");
}

#[test]
fn direct_rule_rejects_duplicate_keys_in_inline_json() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg(r#"{ "a": 1, "a": 2 }"#)
            .write_stdin(r#"{ "test": 1 }"#);
    });

    assert_eq!(output.status.code(), Some(1));
    assert!(stderr_string(output).contains("duplicate key"));
}

#[test]
fn direct_rule_unwraps_bom_prefixed_json_object() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.test")
            .write_stdin(Vec::from(b"\xef\xbb\xbf{ \"test\": 1 }".as_slice()));
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "1\n");
}

#[test]
fn direct_options_cannot_be_ignored_before_subcommand() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-i")
            .arg("input.json")
            .arg("-H")
            .arg("id")
            .arg("transform")
            .arg("-r")
            .arg("rules.yaml");
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("direct-mode options require --rule"));
}

#[test]
fn direct_rule_infers_headerless_csv_numeric_fields_from_stdin() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.0")
            .write_stdin("a,test,1\n");
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "\"a\"\n");
}

#[test]
fn direct_rule_applies_csv_headers_from_stdin() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-H")
            .arg("id,name,age")
            .arg("--rule")
            .arg("@input.id")
            .write_stdin("a,test,1\n");
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "\"a\"\n");
}

#[test]
fn direct_rule_reads_headerless_csv_file_with_headers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let input_path = temp_dir.path().join("non_header.csv");
    fs::write(&input_path, "a,test,1\n").unwrap();

    let output = rulemorph_output(|cmd| {
        cmd.arg("-H")
            .arg("id,name,age")
            .arg("--rule")
            .arg("@input.id")
            .arg("-i")
            .arg(&input_path);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "\"a\"\n");
}

#[test]
fn direct_rule_reads_headered_csv_file_by_extension() {
    let temp_dir = tempfile::tempdir().unwrap();
    let input_path = temp_dir.path().join("with_header.csv");
    fs::write(&input_path, "id,name,age\na,test,1\n").unwrap();

    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule").arg("@input.id").arg("-i").arg(&input_path);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "\"a\"\n");
}

#[test]
fn direct_rule_outputs_multi_row_inferred_csv_as_array() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.0")
            .write_stdin("a,test,1\nb,demo,2\n");
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "[\"a\",\"b\"]\n");
}

#[test]
fn direct_rule_supports_pipe_expr_for_headerless_csv_numeric_fields() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg(r#"["@input.0", {"concat": ["-", "@input.1"]}]"#)
            .write_stdin("a,test,1\n");
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "\"a-test\"\n");
}

#[test]
fn direct_rule_supports_pipe_expr_for_csv_headers() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-H")
            .arg("id,name,age")
            .arg("--rule")
            .arg(r#"["@input.name", "trim", "uppercase"]"#)
            .write_stdin("u1, Alice ,42\n");
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "\"ALICE\"\n");
}

#[test]
fn direct_field_outputs_object_for_json_object_input() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-F")
            .arg("id=@input.id")
            .arg("-F")
            .arg(r#"name=["@input.name","trim","uppercase"]"#)
            .arg("-F")
            .arg("kind=lit:user")
            .write_stdin(r#"{ "id": "u1", "name": " Alice " }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(
        actual,
        serde_json::json!({
            "id": "u1",
            "name": "ALICE",
            "kind": "user"
        })
    );
}

#[test]
fn direct_field_outputs_object_array_for_json_array_input() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-F")
            .arg("id=@input.id")
            .arg("-F")
            .arg(r#"age=["@input.age","int"]"#)
            .write_stdin(r#"[{ "id": "u1", "age": "42" }, { "id": "u2", "age": "7" }]"#);
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(
        actual,
        serde_json::json!([
            { "id": "u1", "age": 42 },
            { "id": "u2", "age": 7 }
        ])
    );
}

#[test]
fn direct_field_infers_headerless_csv_numeric_fields_from_stdin() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-F")
            .arg("id=@input.0")
            .arg("-F")
            .arg(r#"name=["@input.1","uppercase"]"#)
            .write_stdin("u1,Alice,42\n");
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(
        actual,
        serde_json::json!({
            "id": "u1",
            "name": "ALICE"
        })
    );
}

#[test]
fn direct_field_preserves_ordered_out_references() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-F")
            .arg(r#"name=["@input.name","trim"]"#)
            .arg("-F")
            .arg(r##"label=["@out.name",{"concat":["#","@input.id"]}]"##)
            .write_stdin(r#"{ "id": "u1", "name": " Alice " }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(
        actual,
        serde_json::json!({
            "name": "Alice",
            "label": "Alice#u1"
        })
    );
}

#[test]
fn direct_output_map_outputs_nested_object_for_json_object_input() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--output-map")
            .arg(r#"{"user.id":"@input.id","user.name":["@input.name","trim"],"kind":"lit:user"}"#)
            .write_stdin(r#"{ "id": "u1", "name": " Alice " }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(
        actual,
        serde_json::json!({
            "user": {
                "id": "u1",
                "name": "Alice"
            },
            "kind": "user"
        })
    );
}

#[test]
fn direct_output_map_outputs_object_array_for_json_array_input() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--output-map")
            .arg(r#"{"id":"@input.id","age":["@input.age","int"]}"#)
            .write_stdin(r#"[{ "id": "u1", "age": "42" }, { "id": "u2", "age": "7" }]"#);
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(
        actual,
        serde_json::json!([
            { "id": "u1", "age": 42 },
            { "id": "u2", "age": 7 }
        ])
    );
}

#[test]
fn direct_output_map_infers_headerless_csv_numeric_fields_from_stdin() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--output-map")
            .arg(r#"{"id":"@input.0","name":["@input.1","uppercase"]}"#)
            .write_stdin("u1,Alice,42\n");
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(
        actual,
        serde_json::json!({
            "id": "u1",
            "name": "ALICE"
        })
    );
}

#[test]
fn direct_output_map_infers_headerless_csv_numeric_fields_inside_if_step() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--output-map")
            .arg(
                r#"{"label":["lit:x",{"if":{"cond":{"eq":["@input.0","u1"]},"then":["@input.1"],"else":["lit:no"]}}]}"#,
            )
            .write_stdin("u1,Alice\n");
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(
        actual,
        serde_json::json!({
            "label": "Alice"
        })
    );
}

#[test]
fn direct_output_map_literal_object_does_not_trigger_headerless_numeric_inference() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--output-map")
            .arg(r#"{"copy":{"value":"@input.0"}}"#)
            .write_stdin("id,name\nu1,Alice\n");
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(
        actual,
        serde_json::json!({
            "copy": {
                "value": "@input.0"
            }
        })
    );
}

#[test]
fn direct_output_map_literal_pipe_start_does_not_trigger_headerless_numeric_inference() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--output-map")
            .arg(r#"{"copy":[{"value":"@input.0"}]}"#)
            .write_stdin("id,name\nu1,Alice\n");
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(
        actual,
        serde_json::json!({
            "copy": {
                "value": "@input.0"
            }
        })
    );
}

#[test]
fn direct_rule_reads_context() {
    let temp_dir = tempfile::tempdir().unwrap();
    let context_path = temp_dir.path().join("context.json");
    fs::write(&context_path, r#"{ "tenant_id": "t1" }"#).unwrap();

    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@context.tenant_id")
            .arg("-c")
            .arg(context_path)
            .write_stdin(r#"{ "id": "u1" }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "\"t1\"\n");
}

#[test]
fn direct_field_reads_context() {
    let temp_dir = tempfile::tempdir().unwrap();
    let context_path = temp_dir.path().join("context.json");
    fs::write(&context_path, r#"{ "tenant_id": "t1" }"#).unwrap();

    let output = rulemorph_output(|cmd| {
        cmd.arg("-F")
            .arg("id=@input.id")
            .arg("-F")
            .arg("tenant=@context.tenant_id")
            .arg("-c")
            .arg(context_path)
            .write_stdin(r#"{ "id": "u1" }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(
        actual,
        serde_json::json!({
            "id": "u1",
            "tenant": "t1"
        })
    );
}

#[test]
fn direct_output_map_reads_context() {
    let temp_dir = tempfile::tempdir().unwrap();
    let context_path = temp_dir.path().join("context.json");
    fs::write(&context_path, r#"{ "tenant_id": "t1" }"#).unwrap();

    let output = rulemorph_output(|cmd| {
        cmd.arg("--output-map")
            .arg(r#"{"id":"@input.id","tenant":"@context.tenant_id"}"#)
            .arg("-c")
            .arg(context_path)
            .write_stdin(r#"{ "id": "u1" }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(
        actual,
        serde_json::json!({
            "id": "u1",
            "tenant": "t1"
        })
    );
}

#[test]
fn direct_output_specs_are_mutually_exclusive() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.id")
            .arg("-F")
            .arg("id=@input.id")
            .write_stdin(r#"{ "id": "u1" }"#);
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("exactly one of --rule, --output-map, or --field"));
}

#[test]
fn direct_field_rejects_json_looking_malformed_rhs() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-F")
            .arg(r#"age=["@input.age","int""#)
            .write_stdin(r#"{ "age": "42" }"#);
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("--field expr looks like JSON but failed to parse"));
}

#[test]
fn direct_output_map_rejects_evaluated_out_reference() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--output-map")
            .arg(r#"{"id":"@input.id","label":"@out.id"}"#)
            .write_stdin(r#"{ "id": "u1" }"#);
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("--output-map does not support @out references"));
}

#[test]
fn direct_output_map_rejects_evaluated_root_out_reference() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--output-map")
            .arg(r#"{"id":"@input.id","copy":"@out"}"#)
            .write_stdin(r#"{ "id": "u1" }"#);
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("--output-map does not support @out references"));
}

#[test]
fn direct_output_map_rejects_evaluated_v1_root_out_reference() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--output-map")
            .arg(r#"{"id":"@input.id","copy":{"ref":"out"}}"#)
            .write_stdin(r#"{ "id": "u1" }"#);
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("--output-map does not support @out references"));
}

#[test]
fn direct_output_map_rejects_evaluated_out_reference_inside_if_step() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--output-map")
            .arg(
                r#"{"zid":"@input.id","label":["lit:x",{"if":{"cond":{"eq":["@out.zid","u1"]},"then":["lit:yes"],"else":["lit:no"]}}]}"#,
            )
            .write_stdin(r#"{ "id": "u1" }"#);
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("--output-map does not support @out references"));
}

#[test]
fn direct_output_map_allows_literal_out_string() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--output-map")
            .arg(r#"{"label":"lit:@out.id"}"#)
            .write_stdin(r#"{ "id": "u1" }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_str(&stdout_string(output)).unwrap();
    assert_eq!(actual, serde_json::json!({ "label": "@out.id" }));
}

#[test]
fn direct_field_rejects_output_cell_limit_for_large_record_budget() {
    let mut fields = Vec::new();
    for index in 0..101 {
        fields.push(("-F".to_string(), format!("f{}=@input.0", index)));
    }

    let output = rulemorph_output(|cmd| {
        for (flag, value) in fields {
            cmd.arg(flag).arg(value);
        }
        cmd.write_stdin("a\n");
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("direct output cells must be at most 10000000"));
}

#[test]
fn direct_rule_preserves_legacy_explicit_csv_array_shape() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-f")
            .arg("csv")
            .arg("--rule")
            .arg("@input.id")
            .write_stdin("id\na\n");
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "[\"a\"]\n");
}

#[test]
fn direct_rule_uses_json_default_for_unknown_extension_input_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let input_path = temp_dir.path().join("input.txt");
    fs::write(&input_path, r#"{ "id": "a" }"#).unwrap();

    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule").arg("@input.id").arg("-i").arg(&input_path);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "\"a\"\n");
}

#[test]
fn direct_rule_rejects_json_looking_malformed_stdin_as_transform_error() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule").arg("@input.id").write_stdin("{not-json\n");
    });

    assert_eq!(output.status.code(), Some(3));
}

#[test]
fn direct_rule_treats_dash_input_as_stdin_for_detection() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-i")
            .arg("-")
            .arg("--rule")
            .arg("@input.0")
            .write_stdin("a,b\n");
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "\"a\"\n");
}

#[test]
fn direct_rule_rejects_headers_for_json_input() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-H")
            .arg("id")
            .arg("--rule")
            .arg("@input.id")
            .write_stdin(r#"{ "id": "a" }"#);
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("--headers requires CSV direct input"));
}

#[test]
fn direct_rule_rejects_excel_options_for_csv_input() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--excel-header-row")
            .arg("1")
            .arg("--excel-data-range")
            .arg("A2:D2")
            .arg("--rule")
            .arg("@input.id")
            .write_stdin("id,name\na,test\n");
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("--excel-* options require Excel direct input"));
}

#[test]
fn direct_rule_rejects_duplicate_csv_headers() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-H")
            .arg("id,id")
            .arg("--rule")
            .arg("@input.id")
            .write_stdin("a,b\n");
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("--headers must be unique"));
}

#[test]
fn direct_rule_rejects_too_many_csv_headers() {
    let headers = (0..10_001)
        .map(|index| format!("c{}", index))
        .collect::<Vec<_>>()
        .join(",");

    let output = rulemorph_output(|cmd| {
        cmd.arg("-H")
            .arg(headers)
            .arg("--rule")
            .arg("@input.c0")
            .write_stdin("a\n");
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("--headers has too many fields"));
}

#[test]
fn direct_rule_rejects_malformed_csv_when_inferring_numeric_fields() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.0")
            .write_stdin(Vec::from(b"a,\xff\n".as_slice()));
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("failed to infer CSV columns"));
}

#[test]
fn direct_rule_reads_excel_single_row_range() {
    let input_path = fixtures_dir().join("t34_excel_input").join("input.xlsx");

    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.id")
            .arg("-i")
            .arg(input_path)
            .arg("--excel-header-row")
            .arg("1")
            .arg("--excel-data-range")
            .arg("A2:D2");
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "1\n");
}

#[test]
fn direct_rule_reads_excel_multi_row_range() {
    let input_path = fixtures_dir().join("t34_excel_input").join("input.xlsx");

    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.id")
            .arg("-i")
            .arg(input_path)
            .arg("--excel-header-row")
            .arg("1")
            .arg("--excel-data-range")
            .arg("A2:D3");
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "[1,2]\n");
}

#[test]
fn direct_rule_supports_numeric_pipe_expr_for_excel_range() {
    let input_path = fixtures_dir().join("t34_excel_input").join("input.xlsx");

    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg(r#"["@input.score", {"+": [7.5]}, "round"]"#)
            .arg("-i")
            .arg(input_path)
            .arg("--excel-header-row")
            .arg("1")
            .arg("--excel-data-range")
            .arg("A2:D2");
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "50\n");
}

#[test]
fn direct_rule_rejects_excel_input_without_required_range() {
    let input_path = fixtures_dir().join("t34_excel_input").join("input.xlsx");

    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.id")
            .arg("-i")
            .arg(input_path)
            .arg("--excel-header-row")
            .arg("1");
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("--excel-data-range is required"));
}

#[test]
fn direct_rule_rejects_excel_input_without_required_header_row() {
    let input_path = fixtures_dir().join("t34_excel_input").join("input.xlsx");

    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.id")
            .arg("-i")
            .arg(input_path)
            .arg("--excel-data-range")
            .arg("A2:D2");
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("--excel-header-row is required"));
}

#[test]
fn direct_rule_rejects_excel_range_starting_on_header_row() {
    let input_path = fixtures_dir().join("t34_excel_input").join("input.xlsx");

    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.id")
            .arg("-i")
            .arg(input_path)
            .arg("--excel-header-row")
            .arg("1")
            .arg("--excel-data-range")
            .arg("A1:D2");
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(
        stderr_string(output).contains("--excel-data-range is the data range and must start after --excel-header-row")
    );
}

#[test]
fn direct_rule_rejects_excel_sheet_selector_conflict() {
    let input_path = fixtures_dir().join("t34_excel_input").join("input.xlsx");

    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.id")
            .arg("-i")
            .arg(input_path)
            .arg("--excel-header-row")
            .arg("1")
            .arg("--excel-data-range")
            .arg("A2:D2")
            .arg("--excel-sheet")
            .arg("Users")
            .arg("--excel-sheet-index")
            .arg("0");
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(
        stderr_string(output).contains("--excel-sheet and --excel-sheet-index cannot be used together")
    );
}
