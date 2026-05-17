use super::*;

mod conflict;
mod flatten;
mod mutation;

pub(super) use conflict::{has_duplicate_path, has_path_conflict};
pub(super) use flatten::flatten_object;
pub(super) use mutation::{
    merge_object, remove_path, set_path, set_path_object_only, set_path_with_indexes,
};

pub(super) fn parse_source(source: &str) -> Result<(Namespace, &str), TransformError> {
    if let Some((prefix, path)) = source.split_once('.') {
        if path.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "reference path is empty",
            ));
        }
        let namespace = match prefix {
            "input" => Namespace::Input,
            "context" => Namespace::Context,
            "out" => Namespace::Out,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidRef,
                    "ref namespace must be input|context|out",
                ));
            }
        };
        Ok((namespace, path))
    } else {
        if source.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "reference path is empty",
            ));
        }
        Ok((Namespace::Input, source))
    }
}

pub(super) fn parse_ref(value: &str) -> Result<(Namespace, &str), TransformError> {
    let (prefix, path) = value.split_once('.').ok_or_else(|| {
        TransformError::new(TransformErrorKind::InvalidRef, "ref must include namespace")
    })?;

    if path.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidRef,
            "ref path is empty",
        ));
    }

    let namespace = match prefix {
        "input" => Namespace::Input,
        "context" => Namespace::Context,
        "out" => Namespace::Out,
        "item" => Namespace::Item,
        "acc" => Namespace::Acc,
        "pipe" => Namespace::Pipe,
        "local" => Namespace::Local,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "ref namespace must be input|context|out|item|acc|pipe|local",
            ));
        }
    };

    Ok((namespace, path))
}

pub(super) fn parse_path_tokens(
    path: &str,
    kind: TransformErrorKind,
    error_path: impl Into<String>,
) -> Result<Vec<PathToken>, TransformError> {
    parse_path(path)
        .map_err(|err| TransformError::new(kind, err.message()).with_path(error_path.into()))
}
