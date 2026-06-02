use super::super::*;

#[test]
fn test_scope_new() {
    let scope = V2Scope::new();
    assert!(!scope.allows_item());
    assert!(!scope.allows_acc());
    assert!(!scope.allows_pipe());
    assert!(!scope.has_binding("x"));
}

#[test]
fn test_scope_with_item() {
    let scope = V2Scope::new().with_item();
    assert!(scope.allows_item());
    assert!(!scope.allows_acc());
}

#[test]
fn test_scope_with_acc() {
    let scope = V2Scope::new().with_acc();
    assert!(!scope.allows_item());
    assert!(scope.allows_acc());
}

#[test]
fn test_scope_with_pipe() {
    let scope = V2Scope::new().with_pipe();
    assert!(scope.allows_pipe());
}

#[test]
fn test_scope_let_binding() {
    let mut scope = V2Scope::new();
    assert!(!scope.has_binding("x"));
    scope.add_binding("x".to_string());
    assert!(scope.has_binding("x"));
    assert!(!scope.has_binding("y"));
}

#[test]
fn test_scope_lexical_parent() {
    let mut parent = V2Scope::new();
    parent.add_binding("x".to_string());

    let child = V2Scope::with_parent(&parent);
    assert!(child.has_binding("x")); // Inherited from parent
    assert!(!child.has_binding("y"));
}

#[test]
fn test_scope_child_binding_not_in_parent() {
    let parent = V2Scope::new();
    let mut child = V2Scope::with_parent(&parent);
    child.add_binding("y".to_string());

    assert!(child.has_binding("y"));
    assert!(!parent.has_binding("y")); // Child binding not in parent
}
