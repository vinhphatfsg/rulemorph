use std::path::Path;

use anyhow::Result;
use rulemorph::transform_record_with_base_dir;
use serde_json::Value as JsonValue;

use super::catch::CatchSpec;
use super::error::EndpointError;
use super::rule_loader::{RuleKind, load_rule_kind};
use super::rule_ref::resolve_rule_path;
use super::{EndpointEngine, empty_object};

impl EndpointEngine {
    pub(super) fn run_catch(
        &self,
        catch: &CatchSpec,
        error: &EndpointError,
        input: &JsonValue,
        params: Option<&JsonValue>,
        base_dir: &Path,
        base_context: &JsonValue,
    ) -> Result<Option<JsonValue>, EndpointError> {
        if let Some(target) = catch.match_target(error) {
            let target_path = resolve_rule_path(base_dir, &target.to_string_lossy());
            let rule = match load_rule_kind(&target_path)
                .map_err(|err| EndpointError::invalid(err.to_string()))?
            {
                RuleKind::Normal(rule) => rule,
                RuleKind::Network(_) => {
                    return Err(EndpointError::invalid("catch rule must be normal"));
                }
            };
            let error_context = self.step_context(base_context, params, Some(error));
            let output = transform_record_with_base_dir(
                &rule.rule,
                input,
                Some(&error_context),
                &rule.base_dir,
            )
            .map_err(EndpointError::from_transform)?
            .unwrap_or_else(empty_object);
            return Ok(Some(output));
        }
        Ok(None)
    }
}
