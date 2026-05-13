use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read, Write};

use quick_xml::Writer as XmlWriter;
use quick_xml::events::Event;
use quick_xml::name::ResolveResult;
use quick_xml::reader::{NsReader, Reader as XmlReader};
use zip::{ZipArchive, ZipWriter, write::FileOptions};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{ExcelFormulaPolicy, ExcelInput, ExcelSheetRef};
use crate::normalization::NormalizationOptions;

use super::invalid;
use super::range::parse_cell_ref;

const OFFICE_RELATIONSHIPS_NS: &[u8] =
    b"http://schemas.openxmlformats.org/officeDocument/2006/relationships";

pub(super) fn preflight_xlsx_package(
    bytes: &[u8],
    excel: &ExcelInput,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    let mut archive = ZipArchive::new(Cursor::new(bytes)).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("invalid Excel ZIP package: {}", err),
        )
    })?;
    if archive.len() > options.max_excel_zip_entries {
        return Err(invalid("input exceeds max_excel_zip_entries"));
    }

    let mut total_uncompressed = 0usize;
    let mut content_types = None;
    let mut workbook_xml = None;
    let mut workbook_rels = None;
    let mut worksheet_count = 0usize;
    let mut seen_entry_names = HashSet::new();
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to inspect Excel ZIP entry: {}", err),
            )
        })?;
        let name = entry.name().to_string();
        let lower_name = name.to_ascii_lowercase();
        if !seen_entry_names.insert(lower_name.clone()) {
            return Err(invalid("Excel ZIP entry names must be unique"));
        }
        let size =
            usize::try_from(entry.size()).map_err(|_| invalid("Excel ZIP entry too large"))?;
        if size > options.max_excel_entry_uncompressed_bytes {
            return Err(invalid("input exceeds max_excel_entry_uncompressed_bytes"));
        }
        total_uncompressed = total_uncompressed
            .checked_add(size)
            .ok_or_else(|| invalid("input exceeds max_excel_uncompressed_bytes"))?;
        if total_uncompressed > options.max_excel_uncompressed_bytes {
            return Err(invalid("input exceeds max_excel_uncompressed_bytes"));
        }
        if lower_name.ends_with("vbaproject.bin") {
            return Err(invalid("Excel macros are not supported"));
        }
        if lower_name == "[content_types].xml" {
            content_types = Some(read_zip_text(&mut entry)?);
        } else if lower_name == "xl/workbook.xml" {
            workbook_xml = Some(read_zip_text(&mut entry)?);
        } else if lower_name == "xl/_rels/workbook.xml.rels" {
            let rels = read_zip_text(&mut entry)?;
            reject_external_relationships(&rels)?;
            workbook_rels = Some(rels);
        } else if lower_name.starts_with("xl/worksheets/") && lower_name.ends_with(".xml") {
            worksheet_count = worksheet_count.saturating_add(1);
            if worksheet_count > options.max_excel_sheets {
                return Err(invalid("input exceeds max_excel_sheets"));
            }
        } else if lower_name.ends_with(".rels") {
            let rels = read_zip_text(&mut entry)?;
            reject_external_relationships(&rels)?;
        } else if lower_name == "xl/sharedstrings.xml" {
            if size > options.max_excel_shared_string_bytes {
                return Err(invalid("input exceeds max_excel_shared_string_bytes"));
            }
            let shared_strings = read_zip_text(&mut entry)?;
            if count_xml_elements(&shared_strings, b"si")? > options.max_excel_shared_strings {
                return Err(invalid("input exceeds max_excel_shared_strings"));
            }
        } else if lower_name == "xl/styles.xml" {
            let styles = read_zip_text(&mut entry)?;
            if count_xml_elements(&styles, b"xf")? > options.max_excel_styles {
                return Err(invalid("input exceeds max_excel_styles"));
            }
        }
    }

    let content_types =
        content_types.ok_or_else(|| invalid("Excel package is missing [Content_Types].xml"))?;
    let lower_content_types = content_types.to_ascii_lowercase();
    if lower_content_types.contains("macroenabled")
        || lower_content_types.contains("application/vnd.ms-excel.sheet.binary.macroenabled")
    {
        return Err(invalid("Excel macros are not supported"));
    }
    if !lower_content_types
        .contains("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml")
    {
        return Err(invalid("only .xlsx workbooks are supported"));
    }
    let workbook_xml =
        workbook_xml.ok_or_else(|| invalid("Excel package is missing workbook.xml"))?;
    let workbook_rels =
        workbook_rels.ok_or_else(|| invalid("Excel package is missing workbook relationships"))?;
    let selected_worksheet_path = selected_worksheet_path(&workbook_xml, &workbook_rels, excel)?;
    let mut selected_sheet = archive.by_name(&selected_worksheet_path).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to inspect selected Excel worksheet: {}", err),
        )
    })?;
    let selected_sheet = read_zip_text(&mut selected_sheet)?;
    let counts = inspect_worksheet_xml(&selected_sheet, excel.formula)?;
    if counts.rows > options.max_excel_rows || counts.max_row > options.max_excel_rows {
        return Err(invalid("input exceeds max_excel_rows"));
    }
    if counts.cells > options.max_excel_cells {
        return Err(invalid("input exceeds max_excel_cells"));
    }
    let dense_cells = counts
        .max_row
        .checked_mul(counts.max_col)
        .ok_or_else(|| invalid("input exceeds max_excel_cells"))?;
    if dense_cells > options.max_excel_cells {
        return Err(invalid("input exceeds max_excel_cells"));
    }
    Ok(())
}

pub(super) fn xlsx_bytes_for_calamine(bytes: &[u8]) -> Result<Cow<'_, [u8]>, TransformError> {
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

enum CalamineRelationshipPrefix {
    R,
    Relationships,
}

impl CalamineRelationshipPrefix {
    fn attr_name(&self) -> &'static str {
        match self {
            Self::R => "r:id",
            Self::Relationships => "relationships:id",
        }
    }

    fn namespace_attr(&self) -> &'static str {
        match self {
            Self::R => "xmlns:r",
            Self::Relationships => "xmlns:relationships",
        }
    }
}

struct WorkbookRewritePlan {
    prefix: CalamineRelationshipPrefix,
    add_namespace_attr: bool,
}

fn rewrite_workbook_for_calamine(workbook_xml: &str) -> Result<Option<String>, TransformError> {
    let Some(plan) = workbook_rewrite_plan(workbook_xml)? else {
        return Ok(None);
    };

    let mut reader = NsReader::from_str(workbook_xml);
    reader.trim_text(false);
    let mut output = Vec::with_capacity(workbook_xml.len() + 128);
    let mut writer = XmlWriter::new(&mut output);
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) => {
                let rewritten = rewrite_workbook_event(event.to_owned(), &reader, &plan)?;
                writer.write_event(Event::Start(rewritten)).map_err(|err| {
                    TransformError::new(
                        TransformErrorKind::InvalidInput,
                        format!("failed to rewrite Excel workbook XML: {}", err),
                    )
                })?;
            }
            Ok(Event::Empty(event)) => {
                let rewritten = rewrite_workbook_event(event.to_owned(), &reader, &plan)?;
                writer.write_event(Event::Empty(rewritten)).map_err(|err| {
                    TransformError::new(
                        TransformErrorKind::InvalidInput,
                        format!("failed to rewrite Excel workbook XML: {}", err),
                    )
                })?;
            }
            Ok(Event::Eof) => break,
            Ok(event) => {
                writer.write_event(event).map_err(|err| {
                    TransformError::new(
                        TransformErrorKind::InvalidInput,
                        format!("failed to rewrite Excel workbook XML: {}", err),
                    )
                })?;
            }
            Err(err) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to parse Excel workbook XML: {}", err),
                ));
            }
        }
    }
    String::from_utf8(output).map(Some).map_err(|err| {
        invalid(format!(
            "failed to encode rewritten Excel workbook XML: {}",
            err
        ))
    })
}

fn workbook_rewrite_plan(
    workbook_xml: &str,
) -> Result<Option<WorkbookRewritePlan>, TransformError> {
    let mut reader = NsReader::from_str(workbook_xml);
    reader.trim_text(false);
    let mut needs_rewrite = false;
    let mut r_namespace = None;
    let mut relationships_namespace = None;
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) | Ok(Event::Empty(event))
                if event.local_name().as_ref() == b"workbook" =>
            {
                for attr in event.attributes() {
                    let attr = attr.map_err(|err| {
                        TransformError::new(
                            TransformErrorKind::InvalidInput,
                            format!("failed to parse Excel workbook XML attribute: {}", err),
                        )
                    })?;
                    match attr.key.as_ref() {
                        b"xmlns:r" => r_namespace = Some(attr.value.as_ref().to_vec()),
                        b"xmlns:relationships" => {
                            relationships_namespace = Some(attr.value.as_ref().to_vec())
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::Start(event)) | Ok(Event::Empty(event))
                if event.local_name().as_ref() == b"sheet" =>
            {
                for attr in event.attributes() {
                    let attr = attr.map_err(|err| {
                        TransformError::new(
                            TransformErrorKind::InvalidInput,
                            format!("failed to parse Excel workbook XML attribute: {}", err),
                        )
                    })?;
                    let (namespace, local_name) = reader.resolve_attribute(attr.key);
                    if local_name.as_ref() == b"id"
                        && matches!(
                            namespace,
                            ResolveResult::Bound(namespace)
                                if namespace.as_ref() == OFFICE_RELATIONSHIPS_NS
                        )
                        && !is_calamine_relationship_attr(attr.key.as_ref())
                    {
                        needs_rewrite = true;
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to parse Excel workbook XML: {}", err),
                ));
            }
        }
    }
    if !needs_rewrite {
        return Ok(None);
    }

    if r_namespace
        .as_deref()
        .is_none_or(|namespace| namespace == OFFICE_RELATIONSHIPS_NS)
    {
        return Ok(Some(WorkbookRewritePlan {
            prefix: CalamineRelationshipPrefix::R,
            add_namespace_attr: r_namespace.is_none(),
        }));
    }
    if relationships_namespace
        .as_deref()
        .is_none_or(|namespace| namespace == OFFICE_RELATIONSHIPS_NS)
    {
        return Ok(Some(WorkbookRewritePlan {
            prefix: CalamineRelationshipPrefix::Relationships,
            add_namespace_attr: relationships_namespace.is_none(),
        }));
    }
    Err(invalid(
        "Excel workbook relationship namespace conflicts with supported prefixes",
    ))
}

fn rewrite_workbook_event(
    mut event: quick_xml::events::BytesStart<'static>,
    reader: &NsReader<&[u8]>,
    plan: &WorkbookRewritePlan,
) -> Result<quick_xml::events::BytesStart<'static>, TransformError> {
    if event.local_name().as_ref() == b"workbook" && plan.add_namespace_attr {
        event.push_attribute((
            plan.prefix.namespace_attr().as_bytes(),
            OFFICE_RELATIONSHIPS_NS,
        ));
    }
    if event.local_name().as_ref() != b"sheet" {
        return Ok(event);
    }

    let mut relationship_value = None;
    let mut has_calamine_relationship_attr = false;
    for attr in event.attributes() {
        let attr = attr.map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to parse Excel workbook XML attribute: {}", err),
            )
        })?;
        let (namespace, local_name) = reader.resolve_attribute(attr.key);
        let is_office_relationship = matches!(
            namespace,
            ResolveResult::Bound(namespace) if namespace.as_ref() == OFFICE_RELATIONSHIPS_NS
        );
        if is_calamine_relationship_attr(attr.key.as_ref()) && !is_office_relationship {
            return Err(invalid(
                "Excel workbook sheet relationship uses an invalid namespace",
            ));
        }
        if local_name.as_ref() == b"id" && is_office_relationship {
            if is_calamine_relationship_attr(attr.key.as_ref()) {
                has_calamine_relationship_attr = true;
            }
            relationship_value = Some(attr.value.as_ref().to_vec());
        }
    }
    if !has_calamine_relationship_attr && let Some(value) = relationship_value {
        event.push_attribute((plan.prefix.attr_name().as_bytes(), value.as_slice()));
    }
    Ok(event)
}

fn is_calamine_relationship_attr(name: &[u8]) -> bool {
    matches!(name, b"r:id" | b"relationships:id")
}

struct WorkbookSheet {
    name: String,
    relationship_id: String,
}

fn selected_worksheet_path(
    workbook_xml: &str,
    workbook_rels: &str,
    excel: &ExcelInput,
) -> Result<String, TransformError> {
    let sheets = parse_workbook_sheets(workbook_xml)?;
    let relationships = parse_workbook_relationships(workbook_rels)?;
    let selected = match &excel.sheet {
        Some(ExcelSheetRef::Name(name)) => sheets
            .iter()
            .find(|sheet| sheet.name == *name)
            .ok_or_else(|| invalid("Excel sheet was not found"))?,
        Some(ExcelSheetRef::Index(index)) => sheets
            .get(*index)
            .ok_or_else(|| invalid("Excel sheet index is out of range"))?,
        None => sheets
            .first()
            .ok_or_else(|| invalid("Excel workbook has no sheets"))?,
    };
    relationships
        .get(&selected.relationship_id)
        .cloned()
        .ok_or_else(|| invalid("Excel selected sheet relationship was not found"))
}

fn parse_workbook_sheets(workbook_xml: &str) -> Result<Vec<WorkbookSheet>, TransformError> {
    let mut reader = NsReader::from_str(workbook_xml);
    reader.trim_text(false);
    let mut sheets = Vec::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) | Ok(Event::Empty(event))
                if event.local_name().as_ref() == b"sheet" =>
            {
                let mut name = None;
                let mut relationship_id = None;
                for attr in event.attributes() {
                    let attr = attr.map_err(|err| {
                        TransformError::new(
                            TransformErrorKind::InvalidInput,
                            format!("failed to parse Excel workbook XML attribute: {}", err),
                        )
                    })?;
                    match attr.key.as_ref() {
                        b"name" => {
                            name = Some(
                                attr.decode_and_unescape_value(&reader)
                                    .map_err(|err| {
                                        TransformError::new(
                                            TransformErrorKind::InvalidInput,
                                            format!(
                                                "failed to decode Excel workbook sheet name: {}",
                                                err
                                            ),
                                        )
                                    })?
                                    .into_owned(),
                            )
                        }
                        _ => {
                            let (namespace, local_name) = reader.resolve_attribute(attr.key);
                            if local_name.as_ref() == b"id"
                                && matches!(
                                    namespace,
                                    ResolveResult::Bound(namespace)
                                        if namespace.as_ref() == OFFICE_RELATIONSHIPS_NS
                                )
                            {
                                if relationship_id.is_some() {
                                    return Err(invalid(
                                        "Excel workbook sheet has multiple relationships",
                                    ));
                                }
                                relationship_id = Some(
                                    attr.decode_and_unescape_value(&reader)
                                        .map_err(|err| {
                                            TransformError::new(
                                                TransformErrorKind::InvalidInput,
                                                format!(
                                                    "failed to decode Excel workbook sheet relationship: {}",
                                                    err
                                                ),
                                            )
                                        })?
                                        .into_owned(),
                                )
                            }
                        }
                    }
                }
                let name = name.ok_or_else(|| invalid("Excel workbook sheet is missing name"))?;
                let relationship_id = relationship_id
                    .ok_or_else(|| invalid("Excel workbook sheet is missing relationship"))?;
                sheets.push(WorkbookSheet {
                    name,
                    relationship_id,
                });
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to parse Excel workbook XML: {}", err),
                ));
            }
        }
    }
    Ok(sheets)
}

fn parse_workbook_relationships(
    workbook_rels: &str,
) -> Result<HashMap<String, String>, TransformError> {
    let mut reader = XmlReader::from_str(workbook_rels);
    reader.trim_text(false);
    let mut relationships = HashMap::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) | Ok(Event::Empty(event))
                if local_name(event.name().as_ref()) == b"Relationship" =>
            {
                let mut id = None;
                let mut target = None;
                let mut relationship_type = None;
                for attr in event.attributes() {
                    let attr = attr.map_err(|err| {
                        TransformError::new(
                            TransformErrorKind::InvalidInput,
                            format!("failed to parse Excel workbook relationship: {}", err),
                        )
                    })?;
                    match local_name(attr.key.as_ref()) {
                        b"Id" => {
                            id = Some(String::from_utf8_lossy(attr.value.as_ref()).to_string())
                        }
                        b"Target" => {
                            target = Some(String::from_utf8_lossy(attr.value.as_ref()).to_string())
                        }
                        b"Type" => {
                            relationship_type =
                                Some(String::from_utf8_lossy(attr.value.as_ref()).to_string())
                        }
                        _ => {}
                    }
                }
                if relationship_type
                    .as_deref()
                    .is_some_and(|value| value.ends_with("/worksheet"))
                {
                    let id =
                        id.ok_or_else(|| invalid("Excel worksheet relationship is missing id"))?;
                    let target = target
                        .ok_or_else(|| invalid("Excel worksheet relationship is missing target"))?;
                    relationships.insert(id, resolve_workbook_relationship_target(&target)?);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to parse Excel workbook relationships: {}", err),
                ));
            }
        }
    }
    Ok(relationships)
}

fn resolve_workbook_relationship_target(target: &str) -> Result<String, TransformError> {
    if target.contains("..") || target.contains('\\') {
        return Err(invalid("Excel worksheet relationship target is invalid"));
    }
    let target = target.trim_start_matches('/');
    if target.is_empty() {
        return Err(invalid("Excel worksheet relationship target is invalid"));
    }
    if target.starts_with("xl/") {
        if target.starts_with("xl/worksheets/") && target.ends_with(".xml") {
            Ok(target.to_string())
        } else {
            Err(invalid("Excel worksheet relationship target is invalid"))
        }
    } else if target.starts_with("worksheets/") && target.ends_with(".xml") {
        Ok(format!("xl/{target}"))
    } else {
        Err(invalid("Excel worksheet relationship target is invalid"))
    }
}

#[derive(Default)]
struct WorksheetCounts {
    rows: usize,
    cells: usize,
    max_row: usize,
    max_col: usize,
}

fn inspect_worksheet_xml(
    worksheet: &str,
    formula_policy: ExcelFormulaPolicy,
) -> Result<WorksheetCounts, TransformError> {
    let mut reader = XmlReader::from_str(worksheet);
    reader.trim_text(false);
    let mut counts = WorksheetCounts::default();
    let mut in_cell = false;
    let mut cell_has_formula = false;
    let mut cell_has_value = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) => match local_name(event.name().as_ref()) {
                b"row" => {
                    counts.rows = counts.rows.saturating_add(1);
                    update_row_extent(&mut counts, event.attributes())?;
                }
                b"c" => {
                    counts.cells = counts.cells.saturating_add(1);
                    update_cell_extent(&mut counts, event.attributes())?;
                    in_cell = true;
                    cell_has_formula = false;
                    cell_has_value = false;
                }
                b"f" if in_cell => {
                    reject_shared_formula(event.attributes())?;
                    cell_has_formula = true;
                }
                b"v" if in_cell => cell_has_value = true,
                _ => {}
            },
            Ok(Event::Empty(event)) => match local_name(event.name().as_ref()) {
                b"row" => {
                    counts.rows = counts.rows.saturating_add(1);
                    update_row_extent(&mut counts, event.attributes())?;
                }
                b"c" => {
                    counts.cells = counts.cells.saturating_add(1);
                    update_cell_extent(&mut counts, event.attributes())?;
                }
                b"f" if in_cell => {
                    reject_shared_formula(event.attributes())?;
                    cell_has_formula = true;
                }
                b"v" if in_cell => cell_has_value = true,
                _ => {}
            },
            Ok(Event::End(event)) if local_name(event.name().as_ref()) == b"c" => {
                if formula_policy == ExcelFormulaPolicy::Cached
                    && cell_has_formula
                    && !cell_has_value
                {
                    return Err(invalid("Excel formula cell is missing a cached value"));
                }
                in_cell = false;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to parse Excel worksheet XML: {}", err),
                ));
            }
        }
    }
    Ok(counts)
}

fn update_row_extent(
    counts: &mut WorksheetCounts,
    attributes: quick_xml::events::attributes::Attributes<'_>,
) -> Result<(), TransformError> {
    if let Some(value) = find_attr_value(attributes, b"r")? {
        let row = parse_positive_usize_attr(&value, "Excel row reference is invalid")?;
        counts.max_row = counts.max_row.max(row);
    } else {
        counts.max_row = counts.max_row.max(counts.rows);
    }
    Ok(())
}

fn update_cell_extent(
    counts: &mut WorksheetCounts,
    attributes: quick_xml::events::attributes::Attributes<'_>,
) -> Result<(), TransformError> {
    if let Some(value) = find_attr_value(attributes, b"r")? {
        let parsed = parse_cell_ref(&value)?;
        counts.max_col = counts.max_col.max(parsed.col + 1);
        if let Some(row) = parsed.row {
            counts.max_row = counts.max_row.max(row + 1);
        }
    }
    Ok(())
}

fn reject_shared_formula(
    attributes: quick_xml::events::attributes::Attributes<'_>,
) -> Result<(), TransformError> {
    let mut formula_type = None;
    let mut has_shared_index = false;
    let mut has_ref = false;
    for attr in attributes {
        let attr = attr.map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to parse Excel XML attribute: {}", err),
            )
        })?;
        match local_name(attr.key.as_ref()) {
            b"t" => formula_type = Some(String::from_utf8_lossy(attr.value.as_ref()).to_string()),
            b"si" => has_shared_index = true,
            b"ref" => has_ref = true,
            _ => {}
        }
    }
    if formula_type
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("shared"))
        || has_shared_index
        || has_ref
    {
        return Err(invalid("Excel shared formulas are not supported"));
    }
    Ok(())
}

fn find_attr_value(
    attributes: quick_xml::events::attributes::Attributes<'_>,
    name: &[u8],
) -> Result<Option<String>, TransformError> {
    for attr in attributes {
        let attr = attr.map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to parse Excel XML attribute: {}", err),
            )
        })?;
        if local_name(attr.key.as_ref()) == name {
            return Ok(Some(
                String::from_utf8_lossy(attr.value.as_ref()).to_string(),
            ));
        }
    }
    Ok(None)
}

fn parse_positive_usize_attr(value: &str, message: &str) -> Result<usize, TransformError> {
    let value = value.parse::<usize>().map_err(|_| invalid(message))?;
    if value == 0 {
        return Err(invalid(message));
    }
    Ok(value)
}

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

fn reject_external_relationships(rels: &str) -> Result<(), TransformError> {
    let lower = rels.to_ascii_lowercase();
    let compact = lower
        .chars()
        .filter(|value| !value.is_ascii_whitespace())
        .collect::<String>();
    if compact.contains("targetmode=\"external\"")
        || compact.contains("targetmode='external'")
        || compact.contains("target=\"http:")
        || compact.contains("target=\"https:")
        || compact.contains("target=\"file:")
        || compact.contains("target='http:")
        || compact.contains("target='https:")
        || compact.contains("target='file:")
    {
        return Err(invalid("Excel external relationships are not supported"));
    }
    Ok(())
}

fn count_xml_elements(xml: &str, name: &[u8]) -> Result<usize, TransformError> {
    let mut reader = XmlReader::from_str(xml);
    reader.trim_text(false);
    let mut count = 0usize;
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) | Ok(Event::Empty(event)) => {
                if local_name(event.name().as_ref()) == name {
                    count = count.saturating_add(1);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to parse Excel XML: {}", err),
                ));
            }
        }
    }
    Ok(count)
}

fn local_name(name: &[u8]) -> &[u8] {
    name.iter()
        .rposition(|value| *value == b':')
        .map(|index| &name[index + 1..])
        .unwrap_or(name)
}
