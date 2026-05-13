use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;

use rulemorph::{
    InputData, NormalizationOptions, RuleFormat, TransformErrorKind,
    normalize_records_with_options, parse_rule_file, preflight_validate_input, transform,
    transform_input,
};
use zip::{CompressionMethod, ZipWriter, write::FileOptions};

mod common;

use common::golden::{
    assert_text_fixture, assert_xlsx_fixture, fixtures_dir, load_json, load_optional_json,
    load_rule, load_rule_with_format,
};

#[derive(Default)]
struct XlsxFixtureOptions {
    duplicate_header: bool,
    empty_sheet: bool,
    formula_without_cache: bool,
    far_formula_without_cache: bool,
    shared_formula: bool,
    sparse_far_cell: bool,
    macro_enabled: bool,
    external_relationship: bool,
    extra_sheet: bool,
    conflicting_sheet_relationship: bool,
    case_variant_duplicate_sheet: bool,
    escaped_sheet_name: bool,
    custom_relationship_prefix: bool,
    unqualified_sheet_relationship_only: bool,
    wrong_relationship_namespace: bool,
    duplicate_qualified_sheet_relationships: bool,
    conflicting_wrong_literal_relationship: bool,
}

fn build_test_xlsx(options: XlsxFixtureOptions) -> Vec<u8> {
    let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
    let file_options = FileOptions::default().compression_method(CompressionMethod::Deflated);
    let workbook_content_type = if options.macro_enabled {
        "application/vnd.ms-excel.sheet.macroEnabled.main+xml"
    } else {
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"
    };
    let include_second_sheet = options.extra_sheet || options.conflicting_sheet_relationship;
    let sheet2_content_type = if include_second_sheet {
        r#"<Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>"#
    } else {
        ""
    };
    let content_types = format!(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="{workbook_content_type}"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  {sheet2_content_type}
  <Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>"#
    );
    write_zip_file(
        &mut zip,
        "[Content_Types].xml",
        &content_types,
        file_options,
    );
    write_zip_file(
        &mut zip,
        "_rels/.rels",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"#,
        file_options,
    );
    let relationship_prefix =
        if options.custom_relationship_prefix || options.wrong_relationship_namespace {
            "rel"
        } else {
            "r"
        };
    let users_relationship_attrs = if options.conflicting_wrong_literal_relationship {
        r#"r:id="rId4" rel:id="rId1""#.to_string()
    } else if options.duplicate_qualified_sheet_relationships {
        r#"r:id="rId4" relationships:id="rId1""#.to_string()
    } else if options.unqualified_sheet_relationship_only {
        r#"id="rId1""#.to_string()
    } else if options.conflicting_sheet_relationship {
        format!(r#"{relationship_prefix}:id="rId4" id="rId1""#)
    } else {
        format!(r#"{relationship_prefix}:id="rId1""#)
    };
    let relationship_namespace = if options.wrong_relationship_namespace {
        "urn:wrong"
    } else {
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    };
    let workbook_namespace_attrs = if options.conflicting_wrong_literal_relationship {
        format!(r#"xmlns:r="urn:wrong" xmlns:rel="{relationship_namespace}""#)
    } else if options.duplicate_qualified_sheet_relationships {
        format!(
            r#"xmlns:r="{relationship_namespace}" xmlns:relationships="{relationship_namespace}""#
        )
    } else {
        format!(r#"xmlns:{relationship_prefix}="{relationship_namespace}""#)
    };
    let sheet_name = if options.escaped_sheet_name {
        "Users &amp; Billing"
    } else {
        "Users"
    };
    let sheet2_workbook_entry = if options.extra_sheet {
        format!(r#"<sheet name="Archive" sheetId="2" {relationship_prefix}:id="rId4"/>"#)
    } else {
        String::new()
    };
    let workbook = format!(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" {workbook_namespace_attrs}>
  <sheets><sheet name="{sheet_name}" sheetId="1" {users_relationship_attrs}/>{sheet2_workbook_entry}</sheets>
</workbook>"#
    );
    write_zip_file(&mut zip, "xl/workbook.xml", &workbook, file_options);
    let external_relationship = if options.external_relationship {
        r#"<Relationship Id="rId99" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink" Target="https://example.test/" TargetMode="External"/>"#
    } else {
        ""
    };
    let sheet2_relationship = if include_second_sheet {
        r#"<Relationship Id="rId4" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>"#
    } else {
        ""
    };
    let workbook_rels = format!(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
  {sheet2_relationship}
  {external_relationship}
</Relationships>"#
    );
    write_zip_file(
        &mut zip,
        "xl/_rels/workbook.xml.rels",
        &workbook_rels,
        file_options,
    );
    write_zip_file(
        &mut zip,
        "xl/sharedStrings.xml",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="4" uniqueCount="4">
  <si><t>id</t></si><si><t>name</t></si><si><t>Alice</t></si><si><t>Bob</t></si>
</sst>"#,
        file_options,
    );
    write_zip_file(
        &mut zip,
        "xl/styles.xml",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs></styleSheet>"#,
        file_options,
    );
    let sheet = if options.empty_sheet {
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData></sheetData>
</worksheet>"#
            .to_string()
    } else if options.formula_without_cache {
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1"><c r="A1" t="s"><v>0</v></c></row>
    <row r="2"><c r="A2"><f>1+1</f></c></row>
  </sheetData>
</worksheet>"#
            .to_string()
    } else if options.far_formula_without_cache {
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="200"><c r="A200"><f>1+1</f></c></row>
  </sheetData>
</worksheet>"#
            .to_string()
    } else if options.shared_formula {
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1"><c r="A1" t="s"><v>0</v></c></row>
    <row r="2"><c r="A2"><f t="shared" si="1000000000" ref="A2:A1048576">1+1</f><v>2</v></c></row>
  </sheetData>
</worksheet>"#
            .to_string()
    } else if options.sparse_far_cell {
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1"><c r="A1" t="s"><v>0</v></c></row>
    <row r="1048576"><c r="XFD1048576"><v>1</v></c></row>
  </sheetData>
</worksheet>"#
            .to_string()
    } else {
        let second_header = if options.duplicate_header { 0 } else { 1 };
        format!(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>{second_header}</v></c></row>
    <row r="2"><c r="A2"><v>1</v></c><c r="B2" t="s"><v>2</v></c></row>
  </sheetData>
</worksheet>"#
        )
    };
    write_zip_file(&mut zip, "xl/worksheets/sheet1.xml", &sheet, file_options);
    if options.case_variant_duplicate_sheet {
        write_zip_file(
            &mut zip,
            "xl/worksheets/SHEET1.XML",
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1"><c r="A1"><v>1</v></c></row>
    <row r="1048576"><c r="XFD1048576"><v>1</v></c></row>
  </sheetData>
</worksheet>"#,
            file_options,
        );
    }
    if include_second_sheet {
        let sheet2 = if options.conflicting_sheet_relationship {
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1"><c r="A1"><v>1</v></c></row>
    <row r="1048576"><c r="XFD1048576"><v>1</v></c></row>
  </sheetData>
</worksheet>"#
        } else {
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1"><c r="A1"><v>1</v></c><c r="B1"><v>2</v></c></row>
    <row r="2"><c r="A2"><v>3</v></c><c r="B2"><v>4</v></c></row>
    <row r="3"><c r="A3"><v>5</v></c><c r="B3"><v>6</v></c></row>
  </sheetData>
</worksheet>"#
        };
        write_zip_file(&mut zip, "xl/worksheets/sheet2.xml", sheet2, file_options);
    }
    if options.macro_enabled {
        write_zip_file(
            &mut zip,
            "xl/vbaProject.bin",
            "not-a-real-vba-project",
            file_options,
        );
    }
    zip.finish().expect("finish xlsx").into_inner()
}

fn build_dynamodb_users_xlsx() -> Vec<u8> {
    let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
    let file_options = FileOptions::default().compression_method(CompressionMethod::Deflated);
    write_zip_file(
        &mut zip,
        "[Content_Types].xml",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>"#,
        file_options,
    );
    write_zip_file(
        &mut zip,
        "_rels/.rels",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"#,
        file_options,
    );
    write_zip_file(
        &mut zip,
        "xl/workbook.xml",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Users" sheetId="1" r:id="rId1"/></sheets>
</workbook>"#,
        file_options,
    );
    write_zip_file(
        &mut zip,
        "xl/_rels/workbook.xml.rels",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>"#,
        file_options,
    );
    write_zip_file(
        &mut zip,
        "xl/sharedStrings.xml",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="8" uniqueCount="8">
  <si><t>user_id</t></si><si><t>email</t></si><si><t>age</t></si><si><t>active</t></si>
  <si><t>u001</t></si><si><t>alice@example.com</t></si><si><t>u002</t></si><si><t>bob@example.com</t></si>
</sst>"#,
        file_options,
    );
    write_zip_file(
        &mut zip,
        "xl/styles.xml",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs></styleSheet>"#,
        file_options,
    );
    write_zip_file(
        &mut zip,
        "xl/worksheets/sheet1.xml",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c><c r="C1" t="s"><v>2</v></c><c r="D1" t="s"><v>3</v></c></row>
    <row r="2"><c r="A2" t="s"><v>4</v></c><c r="B2" t="s"><v>5</v></c><c r="C2"><v>31</v></c><c r="D2" t="b"><v>1</v></c></row>
    <row r="3"><c r="A3" t="s"><v>6</v></c><c r="B3" t="s"><v>7</v></c><c r="C3"><v>28</v></c><c r="D3" t="b"><v>0</v></c></row>
  </sheetData>
</worksheet>"#,
        file_options,
    );
    zip.finish().expect("finish dynamodb xlsx").into_inner()
}

fn build_string_table_xlsx(sheet_name: &str, headers: &[&str], rows: &[Vec<&str>]) -> Vec<u8> {
    let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
    let file_options = FileOptions::default().compression_method(CompressionMethod::Deflated);

    write_zip_file(
        &mut zip,
        "[Content_Types].xml",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>"#,
        file_options,
    );
    write_zip_file(
        &mut zip,
        "_rels/.rels",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"#,
        file_options,
    );
    write_zip_file(
        &mut zip,
        "xl/workbook.xml",
        &format!(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="{}" sheetId="1" r:id="rId1"/></sheets>
</workbook>"#,
            escape_xml_text(sheet_name)
        ),
        file_options,
    );
    write_zip_file(
        &mut zip,
        "xl/_rels/workbook.xml.rels",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>"#,
        file_options,
    );

    let mut shared_strings = Vec::new();
    shared_strings.extend(headers.iter().copied());
    for row in rows {
        shared_strings.extend(row.iter().copied());
    }
    let shared_string_items = shared_strings
        .iter()
        .map(|value| format!("<si><t>{}</t></si>", escape_xml_text(value)))
        .collect::<String>();
    write_zip_file(
        &mut zip,
        "xl/sharedStrings.xml",
        &format!(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="{count}" uniqueCount="{count}">
  {shared_string_items}
</sst>"#,
            count = shared_strings.len()
        ),
        file_options,
    );
    write_zip_file(
        &mut zip,
        "xl/styles.xml",
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs></styleSheet>"#,
        file_options,
    );

    let mut next_shared_index = 0usize;
    let mut sheet_rows = String::new();
    sheet_rows.push_str(&xlsx_row(1, headers.len(), &mut next_shared_index));
    for (row_index, row) in rows.iter().enumerate() {
        assert_eq!(row.len(), headers.len());
        sheet_rows.push_str(&xlsx_row(row_index + 2, row.len(), &mut next_shared_index));
    }
    write_zip_file(
        &mut zip,
        "xl/worksheets/sheet1.xml",
        &format!(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>{sheet_rows}</sheetData>
</worksheet>"#
        ),
        file_options,
    );

    zip.finish().expect("finish string table xlsx").into_inner()
}

fn xlsx_row(row_number: usize, width: usize, next_shared_index: &mut usize) -> String {
    let mut row = format!(r#"<row r="{row_number}">"#);
    for col_index in 0..width {
        let cell = format!("{}{}", excel_col_name(col_index), row_number);
        row.push_str(&format!(
            r#"<c r="{cell}" t="s"><v>{}</v></c>"#,
            *next_shared_index
        ));
        *next_shared_index += 1;
    }
    row.push_str("</row>");
    row
}

fn excel_col_name(mut index: usize) -> String {
    let mut chars = Vec::new();
    loop {
        let rem = index % 26;
        chars.push((b'A' + rem as u8) as char);
        index /= 26;
        if index == 0 {
            break;
        }
        index -= 1;
    }
    chars.iter().rev().collect()
}

fn escape_xml_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn write_zip_file(
    zip: &mut ZipWriter<Cursor<Vec<u8>>>,
    name: &str,
    contents: &str,
    options: FileOptions,
) {
    zip.start_file(name, options).expect("start xlsx part");
    zip.write_all(contents.as_bytes()).expect("write xlsx part");
}

fn load_expected_error(path: &Path) -> ExpectedTransformError {
    let value = load_json(path);
    serde_json::from_value(value)
        .unwrap_or_else(|err| panic!("invalid expected error: {} ({})", path.display(), err))
}

fn transform_kind_to_str(kind: &TransformErrorKind) -> &'static str {
    match kind {
        TransformErrorKind::InvalidInput => "InvalidInput",
        TransformErrorKind::InvalidRecordsPath => "InvalidRecordsPath",
        TransformErrorKind::InvalidRef => "InvalidRef",
        TransformErrorKind::InvalidTarget => "InvalidTarget",
        TransformErrorKind::MissingRequired => "MissingRequired",
        TransformErrorKind::TypeCastFailed => "TypeCastFailed",
        TransformErrorKind::ExprError => "ExprError",
        TransformErrorKind::AssertionFailed => "AssertionFailed",
    }
}

#[test]
fn t01_csv_basic() {
    let base = fixtures_dir().join("t01_csv_basic");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.csv"))
        .unwrap_or_else(|_| panic!("failed to read input.csv"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t02_csv_no_header() {
    let base = fixtures_dir().join("t02_csv_no_header");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.csv"))
        .unwrap_or_else(|_| panic!("failed to read input.csv"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn csv_trailing_missing_field_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
    default: "missing-name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "id,name\n1\n", None).expect_err("short csv row should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("CSV error"), "{}", err.message);
}

#[test]
fn csv_extra_field_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "id\n1,extra\n", None).expect_err("wide csv row should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("CSV error"), "{}", err.message);
}

#[test]
fn csv_no_header_short_row_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: false
    columns:
      - name: id
      - name: name
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "1\n", None).expect_err("short csv row should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(
        err.message.contains("expected 2")
            || err.message.contains("expected 2 fields")
            || err.message.contains("expected 2"),
        "{}",
        err.message
    );
}

#[test]
fn csv_no_header_extra_field_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: false
    columns:
      - name: id
      - name: name
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "1,Ada,extra\n", None).expect_err("wide csv row should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("expected 2"), "{}", err.message);
}

#[test]
fn csv_duplicate_header_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "id,id\n1,2\n", None).expect_err("duplicate header should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn json_input_duplicate_key_is_invalid() {
    let yaml = r#"
version: 2
input:
  format: json
  json:
    records_path: items
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, r#"{ "items": [{ "id": 1, "id": 2 }] }"#, None)
        .expect_err("duplicate key should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn normalization_rejects_input_over_byte_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_input_bytes: 4,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Text(r#"{ "id": 1 }"#), &options)
        .expect_err("limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn transform_input_rejects_invalid_utf8_bytes() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = transform_input(&rule, InputData::Bytes(&[0xff, 0xfe, b'\n']), None)
        .expect_err("invalid UTF-8 bytes should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("UTF-8"));
}

#[test]
fn normalization_rejects_byte_input_over_byte_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_input_bytes: 4,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(br#"{ "id": 1 }"#), &options)
        .expect_err("byte input limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn t30_json_rule_file_transform_golden() {
    let base = fixtures_dir().join("t30_json_rule_file");
    let rule = load_rule_with_format(&base.join("rules.json"), RuleFormat::Json);
    let input = fs::read_to_string(base.join("input.json")).expect("read input.json");
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t31_yaml_input_transform_golden() {
    assert_text_fixture("t31_yaml_input", "input.yaml");
}

#[test]
fn t32_toml_input_transform_golden() {
    assert_text_fixture("t32_toml_input", "input.toml");
}

#[test]
fn t33_xml_input_transform_golden() {
    assert_text_fixture("t33_xml_input", "input.xml");
}

#[test]
fn excel_input_with_header_normalizes_rows() {
    let base = fixtures_dir().join("t34_excel_input");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read(base.join("input.xlsx")).expect("read xlsx");
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    let expected = load_json(&base.join("expected.json"));
    assert_eq!(output, expected);
}

#[test]
fn excel_rows_transform_to_dynamodb_attribute_values() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
mappings:
  - target: "Item.PK.S"
    expr:
      op: "concat"
      args: ["USER#", { ref: "input.user_id" }]
  - target: "Item.SK.S"
    value: "PROFILE"
  - target: "Item.email.S"
    source: "email"
    type: "string"
  - target: "Item.age.N"
    source: "age"
    type: "string"
  - target: "Item.active.BOOL"
    source: "active"
    type: "bool"
"#,
    )
    .expect("parse rule");
    let input = build_dynamodb_users_xlsx();
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!([
            {
                "Item": {
                    "PK": { "S": "USER#u001" },
                    "SK": { "S": "PROFILE" },
                    "email": { "S": "alice@example.com" },
                    "age": { "N": "31" },
                    "active": { "BOOL": true }
                }
            },
            {
                "Item": {
                    "PK": { "S": "USER#u002" },
                    "SK": { "S": "PROFILE" },
                    "email": { "S": "bob@example.com" },
                    "age": { "N": "28" },
                    "active": { "BOOL": false }
                }
            }
        ])
    );
}

#[test]
fn excel_rows_transform_to_dynamodb_batch_write_item_payload() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
mappings:
  - target: "PutRequest.Item.PK.S"
    expr:
      op: "concat"
      args: ["USER#", { ref: "input.user_id" }]
  - target: "PutRequest.Item.SK.S"
    value: "PROFILE"
  - target: "PutRequest.Item.email.S"
    source: "email"
    type: "string"
  - target: "PutRequest.Item.age.N"
    source: "age"
    type: "string"
  - target: "PutRequest.Item.active.BOOL"
    source: "active"
    type: "bool"
finalize:
  wrap:
    RequestItems:
      UsersTable: "@out"
"#,
    )
    .expect("parse rule");
    let input = build_dynamodb_users_xlsx();
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!({
            "RequestItems": {
                "UsersTable": [
                    {
                        "PutRequest": {
                            "Item": {
                                "PK": { "S": "USER#u001" },
                                "SK": { "S": "PROFILE" },
                                "email": { "S": "alice@example.com" },
                                "age": { "N": "31" },
                                "active": { "BOOL": true }
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "PK": { "S": "USER#u002" },
                                "SK": { "S": "PROFILE" },
                                "email": { "S": "bob@example.com" },
                                "age": { "N": "28" },
                                "active": { "BOOL": false }
                            }
                        }
                    }
                ]
            }
        })
    );
}

#[test]
fn excel_rows_transform_to_dynamodb_extended_attribute_values() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Products
mappings:
  - target: "Item.PK.S"
    expr:
      - "PRODUCT#"
      - concat: ["@input.sku"]
  - target: "Item.SK.S"
    value: "METADATA"
  - target: "Item.name.S"
    source: "name"
    type: "string"
  - target: "Item.price.N"
    source: "price"
    type: "string"
  - target: "Item.active.BOOL"
    source: "active"
    type: "bool"
  - target: "Item.tags.SS"
    expr:
      - "@input.tags"
      - split: [","]
  - target: "Item.dimensions.M.width.N"
    source: 'input.["dimensions.width"]'
    type: "string"
  - target: "Item.dimensions.M.height.N"
    source: 'input.["dimensions.height"]'
    type: "string"
  - target: "Item.archived.NULL"
    value: false
"#,
    )
    .expect("parse rule");
    let input = build_string_table_xlsx(
        "Products",
        &[
            "sku",
            "name",
            "price",
            "active",
            "tags",
            "dimensions.width",
            "dimensions.height",
        ],
        &[
            vec![
                "p001",
                "Notebook",
                "1299",
                "true",
                "stationery,paper",
                "148",
                "210",
            ],
            vec!["p002", "Pen", "199", "false", "stationery,ink", "10", "140"],
        ],
    );
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!([
            {
                "Item": {
                    "PK": { "S": "PRODUCT#p001" },
                    "SK": { "S": "METADATA" },
                    "name": { "S": "Notebook" },
                    "price": { "N": "1299" },
                    "active": { "BOOL": true },
                    "tags": { "SS": ["stationery", "paper"] },
                    "dimensions": { "M": { "width": { "N": "148" }, "height": { "N": "210" } } },
                    "archived": { "NULL": false }
                }
            },
            {
                "Item": {
                    "PK": { "S": "PRODUCT#p002" },
                    "SK": { "S": "METADATA" },
                    "name": { "S": "Pen" },
                    "price": { "N": "199" },
                    "active": { "BOOL": false },
                    "tags": { "SS": ["stationery", "ink"] },
                    "dimensions": { "M": { "width": { "N": "10" }, "height": { "N": "140" } } },
                    "archived": { "NULL": false }
                }
            }
        ])
    );
}

#[test]
fn excel_flat_spreadsheet_columns_transform_to_nested_json_document() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Orders
mappings:
  - target: "document"
    expr:
      - "@input"
      - object_unflatten: []
"#,
    )
    .expect("parse rule");
    let input = build_string_table_xlsx(
        "Orders",
        &[
            "order.id",
            "buyer.name",
            "buyer.email",
            "shipping.address.city",
            "items.primary.sku",
            "items.primary.qty",
            "items.secondary.sku",
            "items.secondary.qty",
        ],
        &[
            vec![
                "o001",
                "Alice",
                "alice@example.com",
                "Tokyo",
                "p001",
                "2",
                "p002",
                "1",
            ],
            vec![
                "o002",
                "Bob",
                "bob@example.com",
                "Osaka",
                "p003",
                "4",
                "p004",
                "3",
            ],
        ],
    );
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!([
            {
                "document": {
                    "order": { "id": "o001" },
                    "buyer": { "name": "Alice", "email": "alice@example.com" },
                    "shipping": { "address": { "city": "Tokyo" } },
                    "items": {
                        "primary": { "sku": "p001", "qty": "2" },
                        "secondary": { "sku": "p002", "qty": "1" }
                    }
                }
            },
            {
                "document": {
                    "order": { "id": "o002" },
                    "buyer": { "name": "Bob", "email": "bob@example.com" },
                    "shipping": { "address": { "city": "Osaka" } },
                    "items": {
                        "primary": { "sku": "p003", "qty": "4" },
                        "secondary": { "sku": "p004", "qty": "3" }
                    }
                }
            }
        ])
    );
}

#[test]
fn xlsform_survey_sheet_transforms_to_question_schema() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: survey
mappings:
  - target: "name"
    source: "name"
  - target: "kind"
    source: "type"
  - target: "label"
    source: "label"
  - target: "required"
    source: "required"
    type: "bool"
  - target: "relevance"
    source: "relevant"
"#,
    )
    .expect("parse rule");
    let input = build_string_table_xlsx(
        "survey",
        &["type", "name", "label", "required", "relevant"],
        &[
            vec!["text", "respondent_name", "Respondent name", "true", ""],
            vec!["integer", "age", "Age", "false", "${respondent_name} != ''"],
        ],
    );
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!([
            {
                "name": "respondent_name",
                "kind": "text",
                "label": "Respondent name",
                "required": true,
                "relevance": ""
            },
            {
                "name": "age",
                "kind": "integer",
                "label": "Age",
                "required": false,
                "relevance": "${respondent_name} != ''"
            }
        ])
    );
}

#[test]
fn t35_html_input_transform_golden() {
    assert_text_fixture("t35_html_input", "input.html");
}

#[test]
fn t36_spreadsheets_plugin_products() {
    assert_xlsx_fixture("t36_spreadsheets_plugin_products");
}

#[test]
fn t37_spreadsheets_plugin_orders() {
    assert_xlsx_fixture("t37_spreadsheets_plugin_orders");
}

#[test]
fn t38_spreadsheets_plugin_survey() {
    assert_xlsx_fixture("t38_spreadsheets_plugin_survey");
}

#[test]
fn t39_pyproject_dependency_inventory() {
    assert_text_fixture("t39_pyproject_dependency_inventory", "input.toml");
}

#[test]
fn t40_cargo_dependency_feature_inventory() {
    assert_text_fixture("t40_cargo_dependency_feature_inventory", "input.toml");
}

#[test]
fn t41_github_actions_matrix() {
    assert_text_fixture("t41_github_actions_matrix", "input.yaml");
}

#[test]
fn t42_openapi_endpoint_catalog() {
    assert_text_fixture("t42_openapi_endpoint_catalog", "input.yaml");
}

#[test]
fn t43_mongodb_schema_summary() {
    assert_text_fixture("t43_mongodb_schema_summary", "input.json");
}

#[test]
fn excel_rejects_sheet_limit_exceeded() {
    let base = fixtures_dir().join("t34_excel_input");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read(base.join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_sheets: 0,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("sheet limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_text_input() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("not xlsx"),
        &NormalizationOptions::default(),
    )
    .expect_err("excel text input should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_input_over_byte_limit() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_input_bytes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("input byte limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_preflight_accepts_byte_input() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    preflight_validate_input(&rule, InputData::Bytes(&input), None)
        .expect("excel preflight should accept byte input");
}

#[test]
fn excel_selects_sheet_with_escaped_name() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: "Users & Billing"
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let input = build_test_xlsx(XlsxFixtureOptions {
        escaped_sheet_name: true,
        ..XlsxFixtureOptions::default()
    });
    let output = transform_input(&rule, InputData::Bytes(&input), None)
        .expect("escaped sheet name should resolve");
    assert_eq!(output, serde_json::json!([{ "id": 1, "name": "Alice" }]));
}

#[test]
fn excel_accepts_sheet_relationship_with_custom_namespace_prefix() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        custom_relationship_prefix: true,
        ..XlsxFixtureOptions::default()
    });
    let output = transform_input(&rule, InputData::Bytes(&input), None)
        .expect("custom relationship namespace prefix should resolve");
    assert_eq!(output, serde_json::json!([{ "id": 1, "name": "Alice" }]));
}

#[test]
fn excel_rejects_unqualified_sheet_relationship_id() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        unqualified_sheet_relationship_only: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("unqualified sheet id must not be accepted as a relationship");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "Excel workbook sheet is missing relationship");
}

#[test]
fn excel_rejects_relationship_id_bound_to_wrong_namespace() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        wrong_relationship_namespace: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("relationship id with the wrong namespace must not be accepted");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "Excel workbook sheet is missing relationship");
}

#[test]
fn excel_rejects_multiple_qualified_sheet_relationship_ids() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        duplicate_qualified_sheet_relationships: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("multiple qualified sheet relationship ids should be rejected");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(
        err.message,
        "Excel workbook sheet has multiple relationships"
    );
}

#[test]
fn excel_rejects_literal_relationship_id_bound_to_wrong_namespace() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        conflicting_wrong_literal_relationship: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("literal relationship id with wrong namespace should be rejected");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(
        err.message,
        "Excel workbook sheet relationship uses an invalid namespace"
    );
}

#[test]
fn excel_applies_row_and_cell_limits_to_selected_sheet_only() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        extra_sheet: true,
        ..XlsxFixtureOptions::default()
    });
    let options = NormalizationOptions {
        max_excel_rows: 2,
        max_excel_cells: 4,
        ..NormalizationOptions::default()
    };
    let records = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect("unselected sheet should not count toward row/cell limits")
        .collect::<Result<Vec<_>, _>>()
        .expect("records should normalize");
    assert_eq!(
        records,
        vec![serde_json::json!({ "id": 1, "name": "Alice" })]
    );
}

#[test]
fn excel_rejects_empty_selected_range_with_clear_error() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        empty_sheet: true,
        ..XlsxFixtureOptions::default()
    });
    let err = transform_input(&rule, InputData::Bytes(&input), None)
        .expect_err("empty sheet should fail before header lookup");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("selected range has no columns"));
}

#[test]
fn excel_rejects_duplicate_header() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let input = build_test_xlsx(XlsxFixtureOptions {
        duplicate_header: true,
        ..XlsxFixtureOptions::default()
    });
    let err = transform_input(&rule, InputData::Bytes(&input), None)
        .expect_err("duplicate header should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_formula_without_cache() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let input = build_test_xlsx(XlsxFixtureOptions {
        formula_without_cache: true,
        ..XlsxFixtureOptions::default()
    });
    let err = transform_input(&rule, InputData::Bytes(&input), None)
        .expect_err("formula without cache should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_formula_policy_returns_formula_without_cache() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
    formula: formula
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let input = build_test_xlsx(XlsxFixtureOptions {
        formula_without_cache: true,
        ..XlsxFixtureOptions::default()
    });
    let output = transform_input(&rule, InputData::Bytes(&input), None).expect("formula transform");
    assert_eq!(output, serde_json::json!([{ "id": "1+1" }]));
}

#[test]
fn excel_formula_error_policy_rejects_formula_without_cache() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
    formula: error
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let input = build_test_xlsx(XlsxFixtureOptions {
        formula_without_cache: true,
        ..XlsxFixtureOptions::default()
    });
    let err = transform_input(&rule, InputData::Bytes(&input), None)
        .expect_err("formula error policy should fail on formulas");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_formula_extent_over_cell_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
    has_header: false
    formula: formula
    columns:
      - name: "a"
        column: "A"
      - name: "b"
        column: "B"
mappings:
  - target: "a"
    source: "a"
"#,
    )
    .expect("parse rule");
    let input = build_test_xlsx(XlsxFixtureOptions {
        far_formula_without_cache: true,
        ..XlsxFixtureOptions::default()
    });
    let options = NormalizationOptions {
        max_excel_rows: 200,
        max_excel_cells: 250,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("formula extent should count toward effective cells");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "input exceeds max_excel_cells");
}

#[test]
fn excel_rejects_shared_formula_metadata() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        shared_formula: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("shared formula should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_sparse_far_cell_dense_range_limit() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        sparse_far_cell: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("sparse far cell should fail before calamine range allocation");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_preflight_ignores_unqualified_sheet_id_attribute() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        conflicting_sheet_relationship: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("preflight must inspect the r:id worksheet, not an unqualified id");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "input exceeds max_excel_rows");
}

#[test]
fn excel_preflight_rejects_case_variant_duplicate_sheet_part() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        case_variant_duplicate_sheet: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("case-variant duplicate sheet parts should be rejected before parsing");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "Excel ZIP entry names must be unique");
}

#[test]
fn xml_input_normalizes_attributes_text_and_repeated_children() {
    let yaml = r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    attr_prefix: "@"
    text_key: "#text"
    child_policy: array
mappings:
  - target: "id"
    source: 'input.["@id"]'
  - target: "name"
    source: 'input.name[0]["#text"]'
  - target: "first_role"
    source: 'input.role[0]["#text"]'
"##;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"<users><user id="1"><name>Alice</name><role>admin</role><role>editor</role></user></users>"#;
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "id": "1", "name": "Alice", "first_role": "admin" }])
    );
}

#[test]
fn xml_mixed_content_preserves_token_separators_before_normalization() {
    let yaml = r##"
version: 2
input:
  format: xml
  xml:
    records_path: root
    text_key: "#text"
mappings:
  - target: "text"
    source: 'input.["#text"]'
"##;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"<root>hello <b>ignored</b> world</root>"#;
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "text": "hello world" }]));
}

#[test]
fn xml_dtd_is_rejected() {
    let yaml = r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, r#"<!DOCTYPE users><users><user /></users>"#, None)
        .expect_err("DTD should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_external_entity_dtd_is_rejected() {
    let yaml = r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(
        &rule,
        r#"<!DOCTYPE users [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><users><user>&xxe;</user></users>"#,
        None,
    )
    .expect_err("external entity DTD should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_node_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_xml_nodes: 1,
        ..NormalizationOptions::default()
    };
    let err =
        normalize_records_with_options(&rule, InputData::Text("<users><user /></users>"), &options)
            .expect_err("node limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_namespace_strip_collision() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    namespaces: strip
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = transform(
        &rule,
        r#"<users xmlns:a="urn:a" xmlns:b="urn:b"><user><a:name>Alice</a:name><b:name>Bob</b:name></user></users>"#,
        None,
    )
    .expect_err("namespace strip collision should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_allows_scoped_namespace_prefix_shadowing() {
    let rule = parse_rule_file(
        r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "outer_name"
    source: 'input.["a:name"][0]["#text"]'
  - target: "inner_name"
    source: 'input.group[0]["a:name"][0]["#text"]'
"##,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<users xmlns:a="urn:outer"><user><a:name>Outer</a:name><group xmlns:a="urn:inner"><a:name>Inner</a:name></group></user></users>"#,
        None,
    )
    .expect("scoped namespace shadowing should be valid");
    assert_eq!(
        output,
        serde_json::json!([{ "outer_name": "Outer", "inner_name": "Inner" }])
    );
}

#[test]
fn xml_allows_scoped_default_namespace_shadowing() {
    let rule = parse_rule_file(
        r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "outer_name"
    source: 'input.name[0]["#text"]'
  - target: "inner_name"
    source: 'input.group[0].name[0]["#text"]'
"##,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<users xmlns="urn:outer"><user><name>Outer</name><group xmlns="urn:inner"><name>Inner</name></group></user></users>"#,
        None,
    )
    .expect("scoped default namespace shadowing should be valid");
    assert_eq!(
        output,
        serde_json::json!([{ "outer_name": "Outer", "inner_name": "Inner" }])
    );
}

#[test]
fn xml_rejects_invalid_records_path_at_runtime() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users[0].user
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = transform(&rule, "<users><user /></users>", None)
        .expect_err("invalid XML records_path should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidRecordsPath);
}

#[test]
fn xml_rejects_records_path_that_matches_no_elements() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.usr
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = transform(&rule, "<users><user /></users>", None)
        .expect_err("missing XML records_path should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidRecordsPath);
    assert_eq!(err.path.as_deref(), Some("input.xml.records_path"));
}

#[test]
fn xml_processing_instruction_is_rejected() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = transform(
        &rule,
        r#"<?xml-stylesheet href="file:///tmp/x" type="text/xsl"?><users><user /></users>"#,
        None,
    )
    .expect_err("processing instruction should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_undefined_entity_is_rejected() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let err = transform(&rule, "<users><user>&xxe;</user></users>", None)
        .expect_err("undefined entity should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_depth_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("<users><user><name>Alice</name></user></users>"),
        &options,
    )
    .expect_err("depth limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_self_closing_element_over_depth_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 2,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("<users><user><name /></user></users>"),
        &options,
    )
    .expect_err("self-closing element over depth limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_text_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_text_bytes: 3,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("<users><user>Alice</user></users>"),
        &options,
    )
    .expect_err("text limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_array_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "roles"
    source: "role"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("<users><user><role>a</role><role>b</role></user></users>"),
        &options,
    )
    .expect_err("array limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_records_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("<users><user /><user /></users>"),
        &options,
    )
    .expect_err("record limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_attribute_namespace_strip_collision() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    namespaces: strip
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = transform(
        &rule,
        r#"<users xmlns:a="urn:a" xmlns:b="urn:b"><user a:id="1" b:id="2" /></users>"#,
        None,
    )
    .expect_err("attribute namespace strip collision should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_records_path_accepts_non_ascii_element_names() {
    let yaml = r##"
version: 2
input:
  format: xml
  xml:
    records_path: 利用者.名前
mappings:
  - target: "name"
    source: "#text"
"##;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"<利用者><名前>太郎</名前></利用者>"#;
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "name": "太郎" }]));
}

#[test]
fn xml_rejects_attr_text_key_collision() {
    let rule = parse_rule_file(
        r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    attr_prefix: "#"
    text_key: "#text"
mappings:
  - target: "id"
    source: "id"
"##,
    )
    .expect("parse rule");
    let err = transform(
        &rule,
        r#"<users><user text="attr">body</user></users>"#,
        None,
    )
    .expect_err("attr/text key collision should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn html_input_extracts_selector_fields() {
    let yaml = r#"
version: 2
input:
  format: html
  html:
    records_selector: "table#users tbody tr"
    fields:
      id:
        selector: "td:nth-child(1)"
        value: text
      name:
        selector: "td:nth-child(2)"
        value: text
      profile_url:
        selector: "a.profile"
        value: attr
        attr: href
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
  - target: "profile_url"
    source: "profile_url"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"<table id="users"><tbody><tr><td>1</td><td>Alice</td><td><a class="profile" href="/users/1">Profile</a></td></tr></tbody></table>"#;
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "id": "1", "name": "Alice", "profile_url": "/users/1" }])
    );
}

#[test]
fn html_multiple_no_match_returns_empty_array() {
    let yaml = r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      tags:
        selector: ".tag"
        value: text
        multiple: true
mappings:
  - target: "tags"
    source: "tags"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let output =
        transform(&rule, r#"<article class="article"></article>"#, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "tags": [] }]));
}

#[test]
fn html_field_without_selector_uses_record_element() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      name:
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let output =
        transform(&rule, r#"<p class="item"> Alice <b>Smith</b> </p>"#, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "name": "Alice Smith" }]));
}

#[test]
fn html_inner_html_is_extracted_without_execution() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      body:
        selector: ".body"
        value: html
mappings:
  - target: "body"
    source: "body"
"#,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<article class="article"><div class="body"><b>Alice</b><script>fetch("/x")</script></div></article>"#,
        None,
    )
    .expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "body": "<b>Alice</b><script>fetch(\"/x\")</script>" }])
    );
}

#[test]
fn html_inner_html_preserves_raw_spacing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      body:
        selector: ".body"
        value: html
mappings:
  - target: "body"
    source: "body"
"#,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<article class="article"><div class="body"><span> Alice   Smith </span></div></article>"#,
        None,
    )
    .expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "body": "<span> Alice   Smith </span>" }])
    );
}

#[test]
fn html_multiple_missing_attrs_are_excluded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      urls:
        selector: "a"
        value: attr
        attr: href
        multiple: true
mappings:
  - target: "urls"
    source: "urls"
"#,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<article class="article"><a>missing</a><a href="/ok">ok</a></article>"#,
        None,
    )
    .expect("transform");
    assert_eq!(output, serde_json::json!([{ "urls": ["/ok"] }]));
}

#[test]
fn html_rejects_array_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      tags:
        selector: ".tag"
        value: text
        multiple: true
mappings:
  - target: "tags"
    source: "tags"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(
            r#"<article class="article"><span class="tag">a</span><span class="tag">b</span></article>"#,
        ),
        &options,
    )
    .expect_err("array limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn html_rejects_node_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      name:
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_html_nodes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"<div><p class="item">Alice</p></div>"#),
        &options,
    )
    .expect_err("node limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn html_rejects_parser_created_node_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      name:
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_html_nodes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"<p class="item">Alice</p>"#),
        &options,
    )
    .expect_err("parsed DOM node limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn html_allows_literal_less_than_sequences_when_dom_nodes_within_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      name:
        selector: ".name"
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_html_nodes: 20,
        ..NormalizationOptions::default()
    };
    let literal_tags = "<article><aside><a><address><abbr><area><audio><bdi><bdo><base><button>";
    let input = format!(
        r#"<article class="item"><script>const sample = "{literal_tags}";</script><span class="name">Alice</span></article>"#
    );
    let output = normalize_records_with_options(&rule, InputData::Text(&input), &options)
        .expect("literal less-than sequences should not count as parsed DOM nodes")
        .collect::<Result<Vec<_>, _>>()
        .expect("records should normalize");
    assert_eq!(output, vec![serde_json::json!({ "name": "Alice" })]);
}

#[test]
fn html_rejects_invalid_selector() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item["
    fields:
      name:
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let err = transform(&rule, r#"<p class="item">Alice</p>"#, None)
        .expect_err("selector parse should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_zip_entry_count_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_zip_entries: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("zip entry count limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_zip_uncompressed_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_uncompressed_bytes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("zip total uncompressed limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_zip_entry_uncompressed_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_entry_uncompressed_bytes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("zip entry uncompressed limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_shared_strings_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_shared_strings: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("shared strings limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_styles_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_styles: 0,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("styles limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_macro_enabled_workbook() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        macro_enabled: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("macro workbook should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_external_relationships() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        external_relationship: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("external relationship should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn csv_normalization_rejects_too_many_records_while_iterating() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let mut records =
        normalize_records_with_options(&rule, InputData::Text("id\n1\n2\n"), &options)
            .expect("CSV iterator should be created before the second record is read");
    let first = records
        .next()
        .expect("first record should exist")
        .expect("first record should parse");
    assert_eq!(first, serde_json::json!({ "id": "1" }));
    let err = records
        .next()
        .expect("second record should report the record limit")
        .expect_err("record limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn json_records_path_rejects_too_many_records_before_materializing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json:
    records_path: users
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"{ "users": [{ "id": 1 }, { "id": 2 }] }"#),
        &options,
    )
    .expect_err("record limit should fail before records are materialized");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn json_records_path_rejects_single_object_when_record_limit_is_zero() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json:
    records_path: user
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 0,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"{ "user": { "id": 1 } }"#),
        &options,
    )
    .expect_err("single object should still honor max_records");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_records_path_rejects_too_many_records_before_materializing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("users:\n  - id: 1\n  - id: 2\n"),
        &options,
    )
    .expect_err("record limit should fail before YAML records are materialized");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_records_path_rejects_too_many_records_before_materializing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("[[users]]\nid = 1\n[[users]]\nid = 2\n"),
        &options,
    )
    .expect_err("record limit should fail before TOML records are materialized");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn csv_rejects_non_byte_delimiter() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
    delimiter: "，"
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("id，name\n1，Alice\n"),
        &NormalizationOptions::default(),
    )
    .expect_err("non-byte delimiter should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_input_uses_records_path() {
    let yaml = r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = "users:\n  - id: 1\n    name: Alice\n";
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "id": 1, "name": "Alice" }]));
}

#[test]
fn yaml_rejects_non_string_mapping_key() {
    let yaml = r#"
version: 2
input:
  format: yaml
  yaml: {}
mappings:
  - target: "value"
    source: "value"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "1: value\n", None).expect_err("non-string key should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_rejects_trailing_document() {
    let yaml = r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "users:\n  - id: 1\n---\nusers:\n  - id: 2\n", None)
        .expect_err("multi-document YAML should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_alias_limit_ignores_asterisks_in_scalars() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "name"
    source: "name"
  - target: "note"
    source: "note"
"#,
    )
    .expect("parse rule");
    let input = r#"
users:
  - name: Alice
    note: "**********"
    block: |
      **********
"#;
    let options = NormalizationOptions {
        max_yaml_aliases: 1,
        ..NormalizationOptions::default()
    };
    let records = normalize_records_with_options(&rule, InputData::Text(input), &options)
        .expect("asterisks in scalar values should not count as aliases")
        .collect::<Result<Vec<_>, _>>()
        .expect("normalized records");
    assert_eq!(
        records,
        vec![serde_json::json!({ "name": "Alice", "note": "**********", "block": "**********\n" })]
    );
}

#[test]
fn yaml_rejects_alias_expansion_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let input = "base: &base { name: Alice }\nusers: [*base, *base]\n";
    let options = NormalizationOptions {
        max_yaml_aliases: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Text(input), &options)
        .expect_err("alias limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_datetime_is_string() {
    let yaml = r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "created_at"
    source: "created_at"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = "[[users]]\ncreated_at = 2026-05-08T12:00:00Z\n";
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "created_at": "2026-05-08T12:00:00Z" }])
    );
}

#[test]
fn toml_quoted_private_datetime_key_stays_object() {
    let yaml = r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "metadata"
    source: "metadata"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = "[[users]]\n[users.metadata]\n'$__toml_private_datetime' = 2026-05-08T12:00:00Z\n";
    let records = normalize_records_with_options(
        &rule,
        InputData::Text(input),
        &NormalizationOptions::default(),
    )
    .expect("normalize toml")
    .collect::<Result<Vec<_>, _>>()
    .expect("normalized records");
    assert_eq!(
        records,
        vec![serde_json::json!({
            "metadata": { "$__toml_private_datetime": "2026-05-08T12:00:00Z" }
        })]
    );
}

#[test]
fn toml_rejects_depth_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "value"
    source: "a.b.c.value"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 2,
        ..NormalizationOptions::default()
    };
    let err =
        normalize_records_with_options(&rule, InputData::Text("[a.b.c]\nvalue = 1\n"), &options)
            .expect_err("depth limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_allows_nested_table_at_equivalent_depth_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "value"
    source: "a.b.c.value"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 4,
        ..NormalizationOptions::default()
    };
    let mut records =
        normalize_records_with_options(&rule, InputData::Text("[a.b.c]\nvalue = 1\n"), &options)
            .expect("nested TOML table should fit within equivalent JSON depth");
    let record = records.next().expect("record").expect("record ok");
    assert_eq!(
        record,
        serde_json::json!({ "a": { "b": { "c": { "value": 1 } } } })
    );
}

#[test]
fn toml_allows_inline_table_at_equivalent_depth_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "value"
    source: "record.value"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 2,
        ..NormalizationOptions::default()
    };
    let mut records = normalize_records_with_options(
        &rule,
        InputData::Text("record = { value = 1 }\n"),
        &options,
    )
    .expect("inline TOML table should fit within equivalent JSON depth");
    let record = records.next().expect("record").expect("record ok");
    assert_eq!(record, serde_json::json!({ "record": { "value": 1 } }));
}

#[test]
fn toml_rejects_nested_inline_table_over_depth_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "value"
    source: "record.inner.value"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 2,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("record = { inner = { value = 1 } }\n"),
        &options,
    )
    .expect_err("nested inline table should exceed depth limit");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_rejects_array_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "values"
    source: "values"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Text("values = [1, 2]\n"), &options)
        .expect_err("array limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_allows_array_at_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "values"
    source: "values"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let mut records =
        normalize_records_with_options(&rule, InputData::Text("values = [1]\n"), &options)
            .expect("array at limit should pass");
    let record = records.next().expect("record").expect("record ok");
    assert_eq!(record, serde_json::json!({ "values": [1] }));
}

#[test]
fn yaml_rejects_text_limit_during_parse() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml: {}
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_text_bytes: 3,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Text("name: Alice\n"), &options)
        .expect_err("text limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_allows_array_at_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml: {}
mappings:
  - target: "values"
    source: "values"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let mut records =
        normalize_records_with_options(&rule, InputData::Text("values: [1]\n"), &options)
            .expect("array at limit should pass");
    let record = records.next().expect("record").expect("record ok");
    assert_eq!(record, serde_json::json!({ "values": [1] }));
}

#[test]
fn t03_json_out_context() {
    let base = fixtures_dir().join("t03_json_out_context");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t04_json_root_coalesce_default() {
    let base = fixtures_dir().join("t04_json_root_coalesce_default");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t05_expr_transforms() {
    let base = fixtures_dir().join("t05_expr_transforms");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t06_lookup_context() {
    let base = fixtures_dir().join("t06_lookup_context");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t07_array_index_paths() {
    let base = fixtures_dir().join("t07_array_index_paths");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t08_escaped_keys() {
    let base = fixtures_dir().join("t08_escaped_keys");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t09_when_mapping() {
    let base = fixtures_dir().join("t09_when_mapping");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t10_when_compare() {
    let base = fixtures_dir().join("t10_when_compare");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t11_when_logical_ops() {
    let base = fixtures_dir().join("t11_when_logical_ops");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t13_expr_extended() {
    let base = fixtures_dir().join("t13_expr_extended");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t14_expr_chain() {
    let base = fixtures_dir().join("t14_expr_chain");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t15_record_when() {
    let base = fixtures_dir().join("t15_record_when");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t16_array_ops() {
    let base = fixtures_dir().join("t16_array_ops");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t17_json_ops_merge() {
    let base = fixtures_dir().join("t17_json_ops_merge");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t18_json_ops_deep_merge() {
    let base = fixtures_dir().join("t18_json_ops_deep_merge");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t19_json_ops_pick() {
    let base = fixtures_dir().join("t19_json_ops_pick");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t20_json_ops_omit() {
    let base = fixtures_dir().join("t20_json_ops_omit");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t21_json_ops_keys_values_entries() {
    let base = fixtures_dir().join("t21_json_ops_keys_values_entries");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t22_json_ops_object_flatten() {
    let base = fixtures_dir().join("t22_json_ops_object_flatten");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t23_json_ops_object_unflatten() {
    let base = fixtures_dir().join("t23_json_ops_object_unflatten");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t24_json_ops_missing() {
    let base = fixtures_dir().join("t24_json_ops_missing");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t25_json_ops_get_chain() {
    let base = fixtures_dir().join("t25_json_ops_get_chain");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t26_chain_all_ops() {
    let base = fixtures_dir().join("t26_chain_all_ops");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t27_json_ops_from_entries() {
    let base = fixtures_dir().join("t27_json_ops_from_entries");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t28_expr_chain_nested() {
    let base = fixtures_dir().join("t28_expr_chain_nested");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t29_json_ops_len() {
    let base = fixtures_dir().join("t29_json_ops_len");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[derive(Debug, serde::Deserialize)]
struct ExpectedTransformError {
    kind: String,
    path: Option<String>,
}

#[test]
fn r01_float_non_finite() {
    let base = fixtures_dir().join("r01_float_non_finite");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r02_json_ops_invalid_path_pick() {
    let base = fixtures_dir().join("r02_json_ops_invalid_path_pick");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r03_json_ops_non_object() {
    let base = fixtures_dir().join("r03_json_ops_non_object");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r04_json_ops_null_arg() {
    let base = fixtures_dir().join("r04_json_ops_null_arg");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r05_json_ops_unflatten_array_index() {
    let base = fixtures_dir().join("r05_json_ops_unflatten_array_index");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r06_json_ops_flatten_brackets() {
    let base = fixtures_dir().join("r06_json_ops_flatten_brackets");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r07_json_ops_flatten_empty_key() {
    let base = fixtures_dir().join("r07_json_ops_flatten_empty_key");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r08_json_ops_from_entries_single_pair() {
    let base = fixtures_dir().join("r08_json_ops_from_entries_single_pair");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r09_asserts_failed() {
    let base = fixtures_dir().join("r09_asserts_failed");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

// =============================================================================
// v2 Golden Tests (T22-T27)
// =============================================================================

#[test]
fn tv22_basic() {
    let base = fixtures_dir().join("tv22_basic");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv23_steps() {
    let base = fixtures_dir().join("tv23_steps");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv24_conditions() {
    let base = fixtures_dir().join("tv24_conditions");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv25_lookup() {
    let base = fixtures_dir().join("tv25_lookup");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv32_steps_finalize() {
    let base = fixtures_dir().join("tv32_steps_finalize");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv33_branch_return() {
    let base = fixtures_dir().join("tv33_branch_return");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv34_branch_return_true() {
    let base = fixtures_dir().join("tv34_branch_return_true");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv35_finalize_wrap() {
    let base = fixtures_dir().join("tv35_finalize_wrap");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv36_branch_uses_out() {
    let base = fixtures_dir().join("tv36_branch_uses_out");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv37_root_refs() {
    let base = fixtures_dir().join("tv37_root_refs");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv38_finalize_filter_offset() {
    let base = fixtures_dir().join("tv38_finalize_filter_offset");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv39_finalize_filter_index() {
    let base = fixtures_dir().join("tv39_finalize_filter_index");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv40_branch_return_filter() {
    let base = fixtures_dir().join("tv40_branch_return_filter");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv41_branch_finalize_wrap() {
    let base = fixtures_dir().join("tv41_branch_finalize_wrap");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv42_branch_deep_merge() {
    let base = fixtures_dir().join("tv42_branch_deep_merge");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv26_unknown_op_error() {
    let base = fixtures_dir().join("tv26_v01_unknown_op");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = r#"[{"name": "test"}]"#;
    let result = transform(&rule, &input, None);
    assert!(result.is_err(), "expected error for unknown op");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("unknown op")
            || err.to_string().contains("nonexistent_op")
            || err.to_string().contains("expr.op is not supported"),
        "expected unknown op error, got: {}",
        err
    );
}

#[test]
fn tv26_forward_out_ref_returns_null() {
    // Note: Forward out reference (@out.b before b is computed)
    // does not cause a runtime error - it returns null/missing.
    // This is valid behavior for v2 expressions.
    let base = fixtures_dir().join("tv26_v02_forward_out_ref");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = r#"[{"x": 1}]"#;
    let result = transform(&rule, &input, None).expect("transform should succeed");
    // When @out.b is not yet computed, it should result in null/missing for "a"
    // The output should have "b" = 1 (from @input.x)
    assert!(result.is_array());
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    let obj = arr[0].as_object().unwrap();
    // "a" should be missing (not in output) or null since @out.b wasn't computed yet
    // "b" should be 1
    assert_eq!(obj.get("b"), Some(&serde_json::json!(1)));
}

#[test]
fn tv27_v1_compat() {
    let base = fixtures_dir().join("tv27_v1_compat");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv28_map_let_binding() {
    let base = fixtures_dir().join("tv28_map_let_binding");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv30_literal_escape() {
    let base = fixtures_dir().join("tv30_literal_escape");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv31_v2_json_ops_pick_omit_reduce_fold() {
    let base = fixtures_dir().join("tv31_v2_json_ops_pick_omit_reduce_fold");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}
