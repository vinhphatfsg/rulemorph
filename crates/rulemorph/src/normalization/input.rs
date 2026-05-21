use crate::error::{TransformError, TransformErrorKind};

use super::NormalizationOptions;

#[derive(Clone, Copy)]
pub enum InputData<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
}

pub(super) fn text_input<'a>(
    input: InputData<'a>,
    options: &NormalizationOptions,
) -> Result<&'a str, TransformError> {
    let bytes = match input {
        InputData::Text(value) => value.as_bytes(),
        InputData::Bytes(bytes) => bytes,
    };
    if bytes.len() > options.max_input_bytes {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "input exceeds max_input_bytes",
        ));
    }
    let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
    std::str::from_utf8(bytes).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("input must be valid UTF-8: {}", err),
        )
    })
}
