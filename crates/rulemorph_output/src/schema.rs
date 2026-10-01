use crate::{OutputError, OutputErrorKind, SchemaDiagnostic, limits};
use jsonschema::{Draft, Validator};
use serde_json::Value;

pub(crate) struct OutputSchema {
    validator: Validator,
    nodes: usize,
}

impl OutputSchema {
    pub(crate) fn compile(schema: &Value) -> Result<Self, OutputError> {
        let nodes = limits::check_value(schema, 256, 16 * 1024)?;
        check_keywords(schema, 0)?;
        let validator = jsonschema::options()
            .with_draft(Draft::Draft202012)
            .offline()
            .should_validate_formats(true)
            .should_ignore_unknown_formats(false)
            .build(schema)
            .map_err(|_| {
                OutputError::new(
                    OutputErrorKind::InvalidSchema,
                    "invalid Draft 2020-12 schema",
                )
            })?;
        Ok(Self { validator, nodes })
    }

    pub(crate) fn validate(&self, value: &Value) -> Result<(), OutputError> {
        let nodes = limits::check_value(value, limits::MAX_NODES, limits::MAX_BYTES)?;
        if nodes.saturating_mul(self.nodes) > 262_144 {
            return Err(limits::limit(
                "schema validation budget exceeds 262144 node pairs",
            ));
        }
        // A single validator evaluates the complete final value; no prefix success.
        if self.validator.is_valid(value) {
            return Ok(());
        }
        let diagnostics = self
            .validator
            .iter_errors(value)
            .take(16)
            .map(|e| SchemaDiagnostic {
                instance_path: e.instance_path().to_string(),
                schema_path: e.schema_path().to_string(),
            })
            .collect();
        let mut error = OutputError::new(
            OutputErrorKind::OutputValidation,
            "complete output does not satisfy schema (up to 16 diagnostic locations)",
        );
        error.diagnostics = diagnostics;
        Err(error)
    }
}

fn check_keywords(schema: &Value, depth: usize) -> Result<(), OutputError> {
    if depth > 16 {
        return Err(limits::limit("schema depth exceeds 16"));
    }
    if schema.is_boolean() {
        return Ok(());
    }
    let object = schema.as_object().ok_or_else(|| {
        OutputError::new(
            OutputErrorKind::InvalidSchema,
            "schema must be object or boolean",
        )
    })?;
    for (key, value) in object {
        match key.as_str() {
            "$schema" => {
                if value.as_str() != Some("https://json-schema.org/draft/2020-12/schema") {
                    return Err(OutputError::new(
                        OutputErrorKind::UnsupportedSchema,
                        "only Draft 2020-12 is supported",
                    ));
                }
            }
            "properties" => {
                let fields = value.as_object().ok_or_else(|| {
                    OutputError::new(
                        OutputErrorKind::InvalidSchema,
                        "properties must be an object",
                    )
                })?;
                for schema in fields.values() {
                    check_keywords(schema, depth + 1)?;
                }
            }
            "items" | "additionalProperties" | "not" => check_keywords(value, depth + 1)?,
            "prefixItems" | "allOf" | "anyOf" | "oneOf" => {
                let schemas = value.as_array().ok_or_else(|| {
                    OutputError::new(
                        OutputErrorKind::InvalidSchema,
                        "schema list must be an array",
                    )
                })?;
                for schema in schemas {
                    check_keywords(schema, depth + 1)?;
                }
            }
            "type" | "required" | "enum" | "const" | "minItems" | "maxItems" | "minProperties"
            | "maxProperties" | "minLength" | "maxLength" | "minimum" | "maximum"
            | "exclusiveMinimum" | "exclusiveMaximum" | "multipleOf" | "title" | "description"
            | "$comment" => {}
            _ => {
                return Err(OutputError::new(
                    OutputErrorKind::UnsupportedSchema,
                    format!(
                        "unsupported schema keyword: {key}; format and references are not supported"
                    ),
                ));
            }
        }
    }
    Ok(())
}
