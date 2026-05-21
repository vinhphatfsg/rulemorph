mod code;
mod rule;
mod transform;

pub use code::ErrorCode;
pub use rule::{RuleError, ValidationResult, YamlLocation};
pub use transform::{TransformError, TransformErrorKind, TransformWarning};

#[cfg(test)]
mod tests {
    use super::{ErrorCode, TransformError, TransformErrorKind, TransformWarning};

    #[test]
    fn error_code_strings_are_stable() {
        let cases = [
            (ErrorCode::InvalidVersion, "InvalidVersion"),
            (ErrorCode::MissingInputFormat, "MissingInputFormat"),
            (ErrorCode::InvalidInputFormat, "InvalidInputFormat"),
            (ErrorCode::MissingCsvSection, "MissingCsvSection"),
            (ErrorCode::MissingJsonSection, "MissingJsonSection"),
            (ErrorCode::MissingYamlSection, "MissingYamlSection"),
            (ErrorCode::MissingTomlSection, "MissingTomlSection"),
            (ErrorCode::MissingXmlSection, "MissingXmlSection"),
            (ErrorCode::MissingHtmlSection, "MissingHtmlSection"),
            (ErrorCode::MissingExcelSection, "MissingExcelSection"),
            (ErrorCode::InvalidDelimiterLength, "InvalidDelimiterLength"),
            (ErrorCode::MissingCsvColumns, "MissingCsvColumns"),
            (ErrorCode::MissingExcelColumns, "MissingExcelColumns"),
            (ErrorCode::InvalidInputOption, "InvalidInputOption"),
            (ErrorCode::DuplicateInputField, "DuplicateInputField"),
            (ErrorCode::MissingTarget, "MissingTarget"),
            (ErrorCode::DuplicateTarget, "DuplicateTarget"),
            (
                ErrorCode::SourceValueExprExclusive,
                "SourceValueExprExclusive",
            ),
            (ErrorCode::MissingMappingValue, "MissingMappingValue"),
            (ErrorCode::InvalidWhenType, "InvalidWhenType"),
            (ErrorCode::InvalidRefNamespace, "InvalidRefNamespace"),
            (ErrorCode::ForwardOutReference, "ForwardOutReference"),
            (ErrorCode::UnknownOp, "UnknownOp"),
            (ErrorCode::InvalidArgs, "InvalidArgs"),
            (ErrorCode::InvalidExprShape, "InvalidExprShape"),
            (ErrorCode::InvalidPath, "InvalidPath"),
            (ErrorCode::InvalidTypeName, "InvalidTypeName"),
            (ErrorCode::UndefinedVariable, "UndefinedVariable"),
            (ErrorCode::InvalidItemRef, "InvalidItemRef"),
            (ErrorCode::InvalidAccRef, "InvalidAccRef"),
            (ErrorCode::CyclicDependency, "CyclicDependency"),
            (ErrorCode::EmptyPipe, "EmptyPipe"),
            (ErrorCode::InvalidPipeStep, "InvalidPipeStep"),
            (ErrorCode::MissingMappings, "MissingMappings"),
            (ErrorCode::StepsMappingExclusive, "StepsMappingExclusive"),
            (ErrorCode::InvalidStep, "InvalidStep"),
            (ErrorCode::InvalidFinalize, "InvalidFinalize"),
        ];

        for (code, expected) in cases {
            assert_eq!(code.as_str(), expected);
        }
    }

    #[test]
    fn transform_error_kind_names_are_stable() {
        let cases = [
            (TransformErrorKind::InvalidInput, "invalid_input"),
            (
                TransformErrorKind::InvalidRecordsPath,
                "invalid_records_path",
            ),
            (TransformErrorKind::InvalidRef, "invalid_ref"),
            (TransformErrorKind::InvalidTarget, "invalid_target"),
            (TransformErrorKind::MissingRequired, "missing_required"),
            (TransformErrorKind::TypeCastFailed, "type_cast_failed"),
            (TransformErrorKind::ExprError, "expr_error"),
            (TransformErrorKind::AssertionFailed, "assertion_failed"),
        ];

        for (kind, expected) in cases {
            let err = TransformError::new(kind, "fixed message");
            assert_eq!(err.kind_name(), expected);
        }
    }

    #[test]
    fn transform_error_display_keeps_path_suffix() {
        let err = TransformError::new(TransformErrorKind::ExprError, "bad value");
        assert_eq!(err.to_string(), "bad value");

        let err = err.with_path("$.items[0]");
        assert_eq!(err.to_string(), "bad value (path: $.items[0])");
    }

    #[test]
    fn transform_warning_from_error_preserves_fields() {
        let err = TransformError::new(TransformErrorKind::InvalidTarget, "target failed")
            .with_path("$.out");

        let warning = TransformWarning::from(err);

        assert_eq!(warning.kind, TransformErrorKind::InvalidTarget);
        assert_eq!(warning.message, "target failed");
        assert_eq!(warning.path.as_deref(), Some("$.out"));
    }
}
