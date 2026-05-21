use std::io::Cursor;

use zip::{CompressionMethod, ZipWriter, write::FileOptions};

use super::write_zip_file;

mod worksheet;

use self::worksheet::{build_sheet1_xml, build_sheet2_xml};

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
    let sheet = build_sheet1_xml(&options);
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
        let sheet2 = build_sheet2_xml(&options);
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
