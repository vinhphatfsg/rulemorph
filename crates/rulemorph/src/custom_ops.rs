use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::error::{ErrorCode, RuleError, TransformError, TransformErrorKind};
use crate::locator::YamlLocator;
use crate::model::{CustomOpDef, Mapping, RuleFile, RuleType, RuleTypeField, RuleTypeKind};
use crate::path::{PathToken, parse_path};
use crate::v2_model::{
    V2CallArg, V2Condition, V2CustomCallStep, V2Expr, V2ObjectFieldValue, V2Pipe, V2Ref, V2Start,
    V2Step, object_field_rule_path,
};
use crate::v2_operator::is_valid_operator;
use crate::v2_parser::{
    custom_call_step_candidate, parse_custom_call_step, parse_v2_condition, parse_v2_expr,
};
use crate::v2_validator::{
    V2Scope, V2ValidationCtx, validate_v2_condition, validate_v2_expr as validate_v2_expr_rules,
};

pub(crate) const MAX_DEFS: usize = 128;
pub(crate) const MAX_CUSTOM_OP_BODY_NODES: usize = 2048;
pub(crate) const MAX_CUSTOM_OP_CALL_DEPTH: usize = 64;
pub(crate) const MAX_CUSTOM_OP_CALLS_PER_RECORD: usize = 100_000;
pub(crate) const MAX_TYPE_DEPTH: usize = 32;
pub(crate) const MAX_TYPE_FIELDS: usize = 512;

#[derive(Debug, Clone, Copy)]
pub(crate) enum ContractMode {
    InputWidth,
    AdapterExact,
    OutputExact,
}

mod call_sites;
mod contract;
mod dependencies;
mod helpers;
mod mappings_shape;
mod step_call_sites;
mod v2_expr_call_sites;

pub(crate) use call_sites::validate_custom_call_sites;
pub(crate) use contract::{build_with_object, check_contract};
pub(crate) use helpers::is_reserved_or_builtin_custom_op_name;

use dependencies::validate_dependency_cycles;
use helpers::*;
use mappings_shape::validate_custom_mappings_shape;
use step_call_sites::{
    parse_known_custom_call_literal_start, validate_condition_call_sites,
    validate_custom_call_site, validate_step_call_sites,
};
use v2_expr_call_sites::{
    validate_condition_expr_call_sites, validate_expr_call_sites, validate_pipe_call_sites,
    validate_v2_expr_call_sites,
};

pub(crate) fn validate_defs(
    rule: &RuleFile,
    locator: Option<&YamlLocator>,
) -> Result<(), Vec<RuleError>> {
    let mut errors = Vec::new();
    if !rule.defs.is_empty() && rule.version != 2 {
        push_rule_error(
            &mut errors,
            locator,
            ErrorCode::InvalidStep,
            "defs is only supported in version 2",
            "defs",
        );
        return Err(errors);
    }

    if rule.defs.len() > MAX_DEFS {
        push_rule_error(
            &mut errors,
            locator,
            ErrorCode::InvalidStep,
            "custom op defs exceed configured limit",
            "defs",
        );
    }

    for (name, def) in &rule.defs {
        let def_path = format!("defs.{}", name);
        if !is_valid_custom_op_name(name) {
            push_rule_error(
                &mut errors,
                locator,
                ErrorCode::InvalidStep,
                format!("custom op name `{}` is invalid", name),
                &def_path,
            );
        }
        if is_reserved_or_builtin_custom_op_name(name) {
            push_rule_error(
                &mut errors,
                locator,
                ErrorCode::UnknownOp,
                format!(
                    "custom op `{}` must not shadow a built-in or reserved op",
                    name
                ),
                &def_path,
            );
        }
        validate_type_limits(
            &def.input,
            &format!("{}.input", def_path),
            locator,
            &mut errors,
        );
        if let Some(returns) = &def.returns {
            validate_type_limits(
                returns,
                &format!("{}.returns", def_path),
                locator,
                &mut errors,
            );
        }
        match (&def.expr, &def.mappings) {
            (Some(_), Some(_)) => push_rule_error(
                &mut errors,
                locator,
                ErrorCode::InvalidStep,
                "custom op must define only one of expr or mappings",
                &def_path,
            ),
            (None, None) => push_rule_error(
                &mut errors,
                locator,
                ErrorCode::InvalidStep,
                "custom op must define expr or mappings",
                &def_path,
            ),
            (Some(_), None) if def.returns.is_none() => push_rule_error(
                &mut errors,
                locator,
                ErrorCode::InvalidStep,
                "custom op expr body requires returns",
                &format!("{}.returns", def_path),
            ),
            _ => {}
        }

        let nodes = custom_op_body_nodes(def);
        if nodes > MAX_CUSTOM_OP_BODY_NODES {
            push_rule_error(
                &mut errors,
                locator,
                ErrorCode::InvalidStep,
                "custom op body exceeds configured node limit",
                &def_path,
            );
        }

        validate_custom_mappings_shape(def, &def_path, locator, &mut errors);
    }

    validate_dependency_cycles(rule, locator, &mut errors);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}
