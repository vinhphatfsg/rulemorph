use std::collections::HashSet;

use crate::error::{ErrorCode, RuleError};
use crate::locator::YamlLocator;
use crate::path::PathToken;

// =============================================================================
// Scope Management (Lexical Scoping)
// =============================================================================

/// Scope tracking for lexical scoping of let bindings
#[derive(Debug, Clone)]
pub struct V2Scope {
    /// Let-bound variable names in current scope
    let_bindings: HashSet<String>,
    /// Whether @item/@item.index is available
    item_available: bool,
    /// Whether @acc is available
    acc_available: bool,
    /// Whether $ / $.path pipe-value references are available
    pipe_available: bool,
    /// Parent scope (for lexical scoping)
    parent: Option<Box<V2Scope>>,
}

impl V2Scope {
    /// Create a new empty scope
    pub fn new() -> Self {
        Self {
            let_bindings: HashSet::new(),
            item_available: false,
            acc_available: false,
            pipe_available: false,
            parent: None,
        }
    }

    /// Create a new child scope inheriting from parent
    pub fn with_parent(parent: &V2Scope) -> Self {
        Self {
            let_bindings: HashSet::new(),
            item_available: parent.item_available,
            acc_available: parent.acc_available,
            pipe_available: parent.pipe_available,
            parent: Some(Box::new(parent.clone())),
        }
    }

    /// Enable @item references in this scope
    pub fn with_item(mut self) -> Self {
        self.item_available = true;
        self
    }

    /// Enable @acc references in this scope
    pub fn with_acc(mut self) -> Self {
        self.acc_available = true;
        self
    }

    /// Enable $ / $.path references in this scope
    pub fn with_pipe(mut self) -> Self {
        self.pipe_available = true;
        self
    }

    /// Add a let binding to the current scope
    pub fn add_binding(&mut self, name: String) {
        self.let_bindings.insert(name);
    }

    /// Check if a let binding exists in scope chain
    pub fn has_binding(&self, name: &str) -> bool {
        if self.let_bindings.contains(name) {
            return true;
        }
        if let Some(ref parent) = self.parent {
            return parent.has_binding(name);
        }
        false
    }

    /// Check if @item is available
    pub fn allows_item(&self) -> bool {
        self.item_available
    }

    /// Check if @acc is available
    pub fn allows_acc(&self) -> bool {
        self.acc_available
    }

    /// Check if pipe-value references are available
    pub fn allows_pipe(&self) -> bool {
        self.pipe_available
    }
}

impl Default for V2Scope {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Validation Context
// =============================================================================

/// Context for v2 validation
pub struct V2ValidationCtx<'a> {
    /// YAML source locator for error positions
    locator: Option<&'a YamlLocator>,
    /// Accumulated errors
    errors: Vec<RuleError>,
    /// Previously computed output targets (for @out forward reference check)
    pub(super) produced_targets: HashSet<Vec<PathToken>>,
    /// Whether @out forward references are allowed
    pub(super) allow_any_out_ref: bool,
    /// Whether @context was referenced (for informational purposes)
    pub context_referenced: bool,
    /// Custom operations available in the current rule.
    pub(super) custom_op_names: HashSet<String>,
}

impl<'a> V2ValidationCtx<'a> {
    /// Create a new validation context
    pub fn new(locator: Option<&'a YamlLocator>) -> Self {
        Self {
            locator,
            errors: Vec::new(),
            produced_targets: HashSet::new(),
            allow_any_out_ref: false,
            context_referenced: false,
            custom_op_names: HashSet::new(),
        }
    }

    /// Create context with existing produced targets
    pub fn with_produced_targets(
        locator: Option<&'a YamlLocator>,
        produced_targets: HashSet<Vec<PathToken>>,
        allow_any_out_ref: bool,
    ) -> Self {
        Self {
            locator,
            errors: Vec::new(),
            produced_targets,
            allow_any_out_ref,
            context_referenced: false,
            custom_op_names: HashSet::new(),
        }
    }

    pub(crate) fn with_custom_op_names(mut self, names: HashSet<String>) -> Self {
        self.custom_op_names = names;
        self
    }

    pub(crate) fn is_custom_op(&self, name: &str) -> bool {
        self.custom_op_names.contains(name)
    }

    /// Push an error with path
    pub fn push_error(&mut self, code: ErrorCode, message: impl Into<String>, path: &str) {
        let mut err = RuleError::new(code, message).with_path(path);
        if let Some(locator) = self.locator
            && let Some(location) = locator.location_for(path)
        {
            err = err.with_location(location.line, location.column);
        }
        self.errors.push(err);
    }

    /// Add a produced target
    pub fn add_produced_target(&mut self, tokens: Vec<PathToken>) {
        self.produced_targets.insert(tokens);
    }

    /// Get reference to produced targets
    pub fn produced_targets(&self) -> &HashSet<Vec<PathToken>> {
        &self.produced_targets
    }

    /// Consume context and return errors if any
    pub fn finish(self) -> Result<(), Vec<RuleError>> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self.errors)
        }
    }

    /// Check if there are errors
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Get collected errors
    pub fn errors(&self) -> &[RuleError] {
        &self.errors
    }
}
