//! Complete, schema-validated output with explicit Rulemorph normalization.
//! Repairs are conservative syntax candidates, never a semantic guarantee.
mod error;
mod limits;
mod repair;
mod rules;
mod schema;

pub use error::{OutputError, OutputErrorKind, RepairEdit, SchemaDiagnostic};
pub use repair::{RepairCandidate, repair_json_candidate};
use rulemorph::{InputData, NormalizationOptions};
use schema::OutputSchema;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputCompletion {
    Complete,
    Truncated,
    Unknown,
}

/// The caller must propagate the provider's generation completion status.
#[derive(Debug, Clone, Copy)]
pub struct RawInput<'a> {
    pub text: &'a str,
    pub completion: InputCompletion,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RepairMode {
    #[default]
    Reject,
    /// Explicitly accept only comma insertions between whitespace-separated tokens.
    MissingCommas,
}

/// Immutable final value validated in full against the supplied schema.
#[derive(Debug, Clone, Serialize)]
pub struct ValidatedOutput {
    value: Value,
    repairs: Vec<RepairEdit>,
}
impl ValidatedOutput {
    pub fn value(&self) -> &Value {
        &self.value
    }
    pub fn repairs(&self) -> &[RepairEdit] {
        &self.repairs
    }
    pub fn into_value(self) -> Value {
        self.value
    }
}

/// Validate a complete value. This differs from core's validate_rule_file.
pub fn validate_output(value: &Value, schema: &Value) -> Result<(), OutputError> {
    OutputSchema::compile(schema)?.validate(value)
}

/// Strict JSON -> explicit rules -> validation of the complete final value.
pub fn normalize_to_schema(
    raw_input: RawInput<'_>,
    rules_text: &str,
    schema: &Value,
) -> Result<ValidatedOutput, OutputError> {
    normalize_to_schema_with_repair(raw_input, rules_text, schema, RepairMode::Reject)
}

/// Opt-in repair candidate -> explicit rules -> complete final schema validation.
pub fn normalize_to_schema_with_repair(
    raw_input: RawInput<'_>,
    rules_text: &str,
    schema: &Value,
    repair: RepairMode,
) -> Result<ValidatedOutput, OutputError> {
    if raw_input.completion != InputCompletion::Complete {
        return Err(OutputError::new(
            OutputErrorKind::IncompleteInput,
            "generation is truncated or completion status is unknown",
        ));
    }
    limits::check_text(raw_input.text)?;
    let schema = OutputSchema::compile(schema)?;
    let rule = rules::parse(rules_text)?;
    let candidate = repair_json_candidate(raw_input.text)?;
    let process = || {
        if repair == RepairMode::Reject && !candidate.repairs.is_empty() {
            return Err(OutputError::new(
                OutputErrorKind::InputSyntax,
                "missing comma; syntax repair requires explicit opt-in",
            ));
        }
        let options = NormalizationOptions {
            max_input_bytes: limits::MAX_BYTES,
            max_records: 128,
            max_depth: 32,
            max_array_len: 256,
            max_text_bytes: limits::MAX_BYTES,
            max_object_depth: 32,
            max_object_fields: 256,
            max_generated_json_nodes: limits::MAX_NODES,
            max_generated_json_bytes: limits::MAX_BYTES,
            ..NormalizationOptions::default()
        };
        // Fully consume normalization so malformed or excessive later records fail.
        rulemorph::normalize_records_with_options(
            &rule,
            InputData::Text(&candidate.text),
            &options,
        )
        .map_err(|_| OutputError::new(OutputErrorKind::Transform, "input normalization failed"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            OutputError::new(
                OutputErrorKind::Transform,
                "input normalization/record limit failed",
            )
        })?;
        let (value, warnings) =
            rulemorph::transform_with_warnings_with_options(&rule, &candidate.text, None, &options)
                .map_err(|_| {
                    OutputError::new(
                        OutputErrorKind::Transform,
                        "explicit rule transformation failed; no values synthesized by schema",
                    )
                })?;
        if !warnings.is_empty() {
            return Err(OutputError::new(
                OutputErrorKind::Transform,
                "transformation produced warnings; no success payload returned",
            ));
        }
        schema.validate(&value)?;
        Ok(value)
    };
    match process() {
        Ok(value) => Ok(ValidatedOutput {
            value,
            repairs: candidate.repairs,
        }),
        Err(mut error) => {
            error.repairs = candidate.repairs;
            Err(error)
        }
    }
}
