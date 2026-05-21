use std::io::Read;

use crate::error::{TransformError, TransformErrorKind};

mod calamine;
mod preflight;

pub(super) use calamine::xlsx_bytes_for_calamine;
pub(super) use preflight::preflight_xlsx_package;

fn read_zip_text<R: Read>(reader: &mut R) -> Result<String, TransformError> {
    let mut text = String::new();
    reader.read_to_string(&mut text).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to read Excel ZIP XML: {}", err),
        )
    })?;
    Ok(text)
}
