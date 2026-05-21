use super::*;
use serde_json::json;

#[test]
fn test_parse_condition_all() {
    let value = json!({
        "all": [
            { "eq": ["@input.status", "active"] },
            { "gt": ["@input.age", 18] }
        ]
    });

    let cond = parse_v2_condition(&value).unwrap();
    if let V2Condition::All(conditions) = cond {
        assert_eq!(conditions.len(), 2);
    } else {
        panic!("Expected All condition");
    }
}

#[test]
fn test_parse_condition_any() {
    let value = json!({
        "any": [
            { "eq": ["@input.role", "admin"] },
            { "eq": ["@input.role", "moderator"] }
        ]
    });

    let cond = parse_v2_condition(&value).unwrap();
    if let V2Condition::Any(conditions) = cond {
        assert_eq!(conditions.len(), 2);
    } else {
        panic!("Expected Any condition");
    }
}

#[test]
fn test_parse_condition_eq() {
    let value = json!({ "eq": ["@input.name", "John"] });
    let cond = parse_v2_condition(&value).unwrap();

    if let V2Condition::Comparison(comp) = cond {
        assert_eq!(comp.op, V2ComparisonOp::Eq);
        assert_eq!(comp.args.len(), 2);
    } else {
        panic!("Expected Comparison condition");
    }
}

#[test]
fn test_parse_condition_ne() {
    let value = json!({ "ne": ["@input.status", "deleted"] });
    let cond = parse_v2_condition(&value).unwrap();

    if let V2Condition::Comparison(comp) = cond {
        assert_eq!(comp.op, V2ComparisonOp::Ne);
    } else {
        panic!("Expected Comparison condition");
    }
}

#[test]
fn test_parse_condition_gt() {
    let value = json!({ "gt": ["@input.age", 18] });
    let cond = parse_v2_condition(&value).unwrap();

    if let V2Condition::Comparison(comp) = cond {
        assert_eq!(comp.op, V2ComparisonOp::Gt);
    } else {
        panic!("Expected Comparison condition");
    }
}

#[test]
fn test_parse_condition_gte() {
    let value = json!({ "gte": ["@input.score", 60] });
    let cond = parse_v2_condition(&value).unwrap();

    if let V2Condition::Comparison(comp) = cond {
        assert_eq!(comp.op, V2ComparisonOp::Gte);
    } else {
        panic!("Expected Comparison condition");
    }
}

#[test]
fn test_parse_condition_lt() {
    let value = json!({ "lt": ["@input.count", 100] });
    let cond = parse_v2_condition(&value).unwrap();

    if let V2Condition::Comparison(comp) = cond {
        assert_eq!(comp.op, V2ComparisonOp::Lt);
    } else {
        panic!("Expected Comparison condition");
    }
}

#[test]
fn test_parse_condition_lte() {
    let value = json!({ "lte": ["@input.retries", 3] });
    let cond = parse_v2_condition(&value).unwrap();

    if let V2Condition::Comparison(comp) = cond {
        assert_eq!(comp.op, V2ComparisonOp::Lte);
    } else {
        panic!("Expected Comparison condition");
    }
}

#[test]
fn test_parse_condition_match() {
    let value = json!({ "match": ["@input.email", "^[a-z]+@"] });
    let cond = parse_v2_condition(&value).unwrap();

    if let V2Condition::Comparison(comp) = cond {
        assert_eq!(comp.op, V2ComparisonOp::Match);
    } else {
        panic!("Expected Comparison condition");
    }
}

#[test]
fn test_parse_nested_conditions() {
    let value = json!({
        "all": [
            { "any": [
                { "eq": ["@input.type", "A"] },
                { "eq": ["@input.type", "B"] }
            ]},
            { "gt": ["@input.value", 0] }
        ]
    });

    let cond = parse_v2_condition(&value).unwrap();
    if let V2Condition::All(conditions) = cond {
        assert_eq!(conditions.len(), 2);
        assert!(matches!(conditions[0], V2Condition::Any(_)));
    } else {
        panic!("Expected All condition");
    }
}
