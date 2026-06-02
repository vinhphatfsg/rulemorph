use super::*;

#[test]
fn test_validate_item_ref_outside_map() {
    let mut ctx = V2ValidationCtx::new(None);
    let scope = V2Scope::new(); // No @item scope
    let v2_ref = V2Ref::Item("value".to_string());

    validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

    assert!(ctx.has_errors());
    assert_eq!(ctx.errors()[0].code, ErrorCode::InvalidItemRef);
}

#[test]
fn test_validate_item_ref_inside_map() {
    let mut ctx = V2ValidationCtx::new(None);
    let scope = V2Scope::new().with_item();
    let v2_ref = V2Ref::Item("value".to_string());

    validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

    assert!(!ctx.has_errors());
}

#[test]
fn test_validate_item_index() {
    let mut ctx = V2ValidationCtx::new(None);
    let scope = V2Scope::new().with_item();
    let v2_ref = V2Ref::Item("index".to_string());

    validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

    assert!(!ctx.has_errors());
}

#[test]
fn test_validate_item_ref_invalid_subpath() {
    let scope = V2Scope::new().with_item();

    let mut ctx = V2ValidationCtx::new(None);
    let v2_ref = V2Ref::Item("value..foo".to_string());
    validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);
    assert!(
        ctx.errors()
            .iter()
            .any(|err| err.code == ErrorCode::InvalidPath)
    );

    let mut ctx = V2ValidationCtx::new(None);
    let v2_ref = V2Ref::Item("index..foo".to_string());
    validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);
    assert!(
        ctx.errors()
            .iter()
            .any(|err| err.code == ErrorCode::InvalidPath)
    );
}

#[test]
fn test_validate_undefined_local() {
    let mut ctx = V2ValidationCtx::new(None);
    let scope = V2Scope::new();
    let v2_ref = V2Ref::Local("undefined_var".to_string());

    validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

    assert!(ctx.has_errors());
    assert_eq!(ctx.errors()[0].code, ErrorCode::UndefinedVariable);
}

#[test]
fn test_validate_defined_local() {
    let mut ctx = V2ValidationCtx::new(None);
    let mut scope = V2Scope::new();
    scope.add_binding("x".to_string());
    let v2_ref = V2Ref::Local("x".to_string());

    validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

    assert!(!ctx.has_errors());
}

#[test]
fn test_validate_pipe_ref_requires_pipe_scope() {
    let mut ctx = V2ValidationCtx::new(None);
    let scope = V2Scope::new();
    let v2_ref = V2Ref::Pipe("value".to_string());

    validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

    assert!(ctx.has_errors());
    assert_eq!(ctx.errors()[0].code, ErrorCode::InvalidRefNamespace);

    let mut ctx = V2ValidationCtx::new(None);
    let scope = V2Scope::new().with_pipe();
    validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

    assert!(!ctx.has_errors());
}
