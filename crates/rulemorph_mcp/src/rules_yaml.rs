use serde_yaml::Value as YamlValue;

mod format;
mod input;
mod mappings;
mod refs;

pub(crate) use self::format::apply_format_override;
pub(crate) use self::input::{build_input_yaml, update_yaml_input_spec};
pub(crate) use self::mappings::{update_yaml_mapping, yaml_mappings_sequence_mut};
pub(crate) use self::refs::collect_missing_refs;

pub(crate) fn yaml_key(key: &str) -> YamlValue {
    YamlValue::String(key.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rulemorph::{Expr, ExprRef, RuleFile};
    use serde_json::json;
    use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};

    use super::*;

    fn yaml(value: &str) -> YamlValue {
        serde_yaml::from_str(value).unwrap()
    }

    #[test]
    fn build_input_yaml_keeps_json_records_path_and_csv_default() {
        assert_eq!(
            build_input_yaml("json", Some("items")),
            yaml(
                r#"
format: json
json:
  records_path: items
"#
            )
        );
        assert_eq!(
            build_input_yaml("csv", Some("ignored")),
            yaml(
                r#"
format: csv
csv: {}
"#
            )
        );
    }

    #[test]
    fn update_yaml_input_spec_preserves_non_mapping_boundaries() {
        let mut root = yaml(
            r#"
input:
  format: csv
"#,
        );

        update_yaml_input_spec(&mut root, Some("json"), Some("users"));

        assert_eq!(
            root,
            yaml(
                r#"
input:
  format: json
  json:
    records_path: users
"#
            )
        );

        let mut scalar_root = YamlValue::String("not-a-map".to_string());
        update_yaml_input_spec(&mut scalar_root, Some("json"), Some("users"));
        assert_eq!(scalar_root, YamlValue::String("not-a-map".to_string()));
    }

    #[test]
    fn update_yaml_mapping_switches_source_and_optional_value_shapes() {
        let mut mappings = vec![yaml(
            r#"
target: name
value: null
expr:
  op: trim
  args: []
"#,
        )];

        assert!(matches!(
            update_yaml_mapping(&mut mappings, 0, Some("input.name")),
            Ok(())
        ));
        assert_eq!(
            mappings[0],
            yaml(
                r#"
target: name
source: input.name
"#
            )
        );

        assert!(matches!(
            update_yaml_mapping(&mut mappings, 0, None),
            Ok(())
        ));
        assert_eq!(
            mappings[0],
            yaml(
                r#"
target: name
value: null
required: false
"#
            )
        );
    }

    #[test]
    fn collect_missing_refs_deduplicates_by_target_and_reference() {
        let expr = Expr::Ref(ExprRef {
            ref_path: "input.missing".to_string(),
        });
        let when = Expr::Ref(ExprRef {
            ref_path: "input.present".to_string(),
        });
        let input_paths = HashSet::from(["present".to_string()]);
        let mut out = Vec::new();
        let mut seen = HashSet::new();

        collect_missing_refs(
            "name",
            Some(&expr),
            Some(&when),
            &input_paths,
            &mut out,
            &mut seen,
        );
        collect_missing_refs("name", Some(&expr), None, &input_paths, &mut out, &mut seen);

        assert_eq!(
            out,
            vec![json!({
                "target": "name",
                "ref": "input.missing",
                "path": "missing"
            })]
        );
    }

    #[test]
    fn apply_format_override_accepts_known_formats_and_rejects_unknown() {
        let mut rule: RuleFile = serde_yaml::from_str(
            r#"
version: 2
input:
  format: csv
mappings: []
"#,
        )
        .unwrap();

        apply_format_override(&mut rule, Some("JSON")).unwrap();
        assert!(matches!(rule.input.format, rulemorph::InputFormat::Json));
        apply_format_override(&mut rule, Some("markdown")).unwrap();
        assert!(matches!(
            rule.input.format,
            rulemorph::InputFormat::Markdown
        ));
        assert_eq!(
            apply_format_override(&mut rule, Some("parquet")),
            Err("unknown format: parquet".to_string())
        );
    }

    #[test]
    fn yaml_mappings_sequence_errors_keep_messages() {
        let mut root = YamlValue::Mapping(YamlMapping::new());
        let Err(crate::errors::CallError::Tool { message, errors }) =
            yaml_mappings_sequence_mut(&mut root)
        else {
            panic!("expected tool error");
        };
        assert_eq!(message, "rules yaml is missing mappings");
        assert!(errors.is_some());
    }
}
