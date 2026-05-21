use std::borrow::Cow;
use std::io::{Cursor, Write};

use zip::{ZipArchive, ZipWriter, write::FileOptions};

use crate::error::{TransformError, TransformErrorKind};

use super::super::workbook::rewrite_workbook_for_calamine;
use super::read_zip_text;

pub(in crate::normalization::excel) fn xlsx_bytes_for_calamine(
    bytes: &[u8],
) -> Result<Cow<'_, [u8]>, TransformError> {
    let mut archive = ZipArchive::new(Cursor::new(bytes)).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("invalid Excel ZIP package: {}", err),
        )
    })?;
    let mut workbook = archive.by_name("xl/workbook.xml").map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to inspect Excel workbook XML: {}", err),
        )
    })?;
    let workbook_xml = read_zip_text(&mut workbook)?;
    drop(workbook);
    let Some(rewritten_workbook) = rewrite_workbook_for_calamine(&workbook_xml)? else {
        return Ok(Cow::Borrowed(bytes));
    };

    let mut output = ZipWriter::new(Cursor::new(Vec::new()));
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to rewrite Excel ZIP entry: {}", err),
            )
        })?;
        let name = entry.name().to_string();
        let options = FileOptions::default().compression_method(entry.compression());
        if entry.is_dir() {
            output.add_directory(&name, options).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to rewrite Excel ZIP directory: {}", err),
                )
            })?;
            continue;
        }
        output.start_file(&name, options).map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to rewrite Excel ZIP entry: {}", err),
            )
        })?;
        if name.eq_ignore_ascii_case("xl/workbook.xml") {
            output
                .write_all(rewritten_workbook.as_bytes())
                .map_err(|err| {
                    TransformError::new(
                        TransformErrorKind::InvalidInput,
                        format!("failed to rewrite Excel workbook XML: {}", err),
                    )
                })?;
        } else {
            std::io::copy(&mut entry, &mut output).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to copy Excel ZIP entry: {}", err),
                )
            })?;
        }
    }
    let rewritten = output.finish().map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to finish rewritten Excel ZIP package: {}", err),
        )
    })?;
    Ok(Cow::Owned(rewritten.into_inner()))
}
