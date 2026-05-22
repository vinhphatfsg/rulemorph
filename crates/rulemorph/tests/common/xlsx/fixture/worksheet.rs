use super::XlsxFixtureOptions;

pub(super) fn build_sheet1_xml(options: &XlsxFixtureOptions) -> String {
    if options.empty_sheet {
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
    }
}

pub(super) fn build_sheet2_xml(options: &XlsxFixtureOptions) -> &'static str {
    if options.conflicting_sheet_relationship {
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
    }
}
