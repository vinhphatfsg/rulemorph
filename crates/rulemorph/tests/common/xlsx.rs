use std::io::{Cursor, Write};

use zip::{CompressionMethod, ZipWriter, write::FileOptions};

#[derive(Default)]
pub(crate) struct XlsxFixtureOptions {
    pub(crate) duplicate_header: bool,
    pub(crate) empty_sheet: bool,
    pub(crate) formula_without_cache: bool,
    pub(crate) far_formula_without_cache: bool,
    pub(crate) shared_formula: bool,
    pub(crate) sparse_far_cell: bool,
    pub(crate) macro_enabled: bool,
    pub(crate) external_relationship: bool,
    pub(crate) extra_sheet: bool,
    pub(crate) conflicting_sheet_relationship: bool,
    pub(crate) case_variant_duplicate_sheet: bool,
    pub(crate) escaped_sheet_name: bool,
    pub(crate) custom_relationship_prefix: bool,
    pub(crate) unqualified_sheet_relationship_only: bool,
    pub(crate) wrong_relationship_namespace: bool,
    pub(crate) duplicate_qualified_sheet_relationships: bool,
    pub(crate) conflicting_wrong_literal_relationship: bool,
}

pub(crate) fn build_test_xlsx(options: XlsxFixtureOptions) -> Vec<u8> {
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

pub(crate) fn build_dynamodb_users_xlsx() -> Vec<u8> {
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

pub(crate) fn build_string_table_xlsx(
    sheet_name: &str,
    headers: &[&str],
    rows: &[Vec<&str>],
) -> Vec<u8> {
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
