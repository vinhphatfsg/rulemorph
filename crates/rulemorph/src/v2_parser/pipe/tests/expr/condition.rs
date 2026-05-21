#[test]
fn test_parse_v2_condition_from_record_when() {
        // Test parsing conditions as used in record_when
        let value = json!({
            "all": [
                { "gt": ["@input.score", 0] },
                { "eq": ["@input.active", true] }
            ]
        });

        let cond = crate::v2_parser::parse_v2_condition(&value).unwrap();
        if let V2Condition::All(conditions) = cond {
            assert_eq!(conditions.len(), 2);
        } else {
            panic!("Expected All condition");
        }
}

#[test]
fn test_parse_v2_mapping_when_condition() {
        // mapping.when with v2 condition syntax
        let value = json!({ "eq": ["@input.role", "admin"] });
        let cond = crate::v2_parser::parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Eq);
            assert_eq!(comp.args.len(), 2);
        } else {
            panic!("Expected Comparison condition");
        }
}
