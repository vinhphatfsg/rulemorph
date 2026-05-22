use std::collections::HashSet;

use crate::error::ErrorCode;
use crate::v2_validator::V2ValidationCtx;

use super::validate_no_cyclic_dependencies;

#[test]
fn test_no_cycle() {
    let mut ctx = V2ValidationCtx::new(None);
    let targets = vec![
        ("a".to_string(), HashSet::new()),
        ("b".to_string(), ["a".to_string()].into_iter().collect()),
        ("c".to_string(), ["b".to_string()].into_iter().collect()),
    ];

    validate_no_cyclic_dependencies(&targets, "mappings", &mut ctx);

    assert!(!ctx.has_errors());
}

#[test]
fn test_self_reference_cycle() {
    let mut ctx = V2ValidationCtx::new(None);
    let targets = vec![("a".to_string(), ["a".to_string()].into_iter().collect())];

    validate_no_cyclic_dependencies(&targets, "mappings", &mut ctx);

    assert!(ctx.has_errors());
    assert_eq!(ctx.errors()[0].code, ErrorCode::CyclicDependency);
}

#[test]
fn test_indirect_cycle() {
    let mut ctx = V2ValidationCtx::new(None);
    let targets = vec![
        ("a".to_string(), ["b".to_string()].into_iter().collect()),
        ("b".to_string(), ["a".to_string()].into_iter().collect()),
    ];

    validate_no_cyclic_dependencies(&targets, "mappings", &mut ctx);

    assert!(ctx.has_errors());
    assert_eq!(ctx.errors()[0].code, ErrorCode::CyclicDependency);
}
