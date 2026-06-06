use crate::error::{TransformError, TransformErrorKind};

#[derive(Clone, Copy)]
pub(crate) struct CellWindow {
    pub(crate) start_row: usize,
    pub(crate) end_row: Option<usize>,
    pub(crate) start_col: usize,
    pub(crate) end_col: Option<usize>,
}

pub(crate) struct ParsedCellRef {
    pub(crate) col: usize,
    pub(crate) row: Option<usize>,
}

pub(crate) fn parse_cell_window(range: Option<&str>) -> Result<CellWindow, TransformError> {
    let Some(range) = range else {
        return Ok(CellWindow {
            start_row: 0,
            end_row: None,
            start_col: 0,
            end_col: None,
        });
    };
    let (start, end) = range
        .split_once(':')
        .ok_or_else(|| invalid("Excel range must use A:D or A1:D100 form"))?;
    let start = parse_cell_ref(start)?;
    let end = parse_cell_ref(end)?;
    if start.col > end.col {
        return Err(invalid("Excel range start column is after end column"));
    }
    if let (Some(start_row), Some(end_row)) = (start.row, end.row)
        && start_row > end_row
    {
        return Err(invalid("Excel range start row is after end row"));
    }
    Ok(CellWindow {
        start_row: start.row.unwrap_or(0),
        end_row: end.row,
        start_col: start.col,
        end_col: Some(end.col),
    })
}

pub(crate) fn parse_cell_ref(value: &str) -> Result<ParsedCellRef, TransformError> {
    let mut letters = String::new();
    let mut digits = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphabetic() && digits.is_empty() {
            letters.push(ch.to_ascii_uppercase());
        } else if ch.is_ascii_digit() {
            digits.push(ch);
        } else {
            return Err(invalid("Excel cell reference is invalid"));
        }
    }
    if letters.is_empty() {
        return Err(invalid("Excel cell reference requires a column"));
    }
    let col = column_letters_to_index(&letters)?;
    let row = if digits.is_empty() {
        None
    } else {
        let row = digits
            .parse::<usize>()
            .map_err(|_| invalid("Excel cell reference row is invalid"))?;
        if row == 0 {
            return Err(invalid("Excel cell reference row must be 1-based"));
        }
        Some(row - 1)
    };
    Ok(ParsedCellRef { col, row })
}

pub(crate) fn column_letters_to_index(value: &str) -> Result<usize, TransformError> {
    let mut index = 0usize;
    for ch in value.chars() {
        if !ch.is_ascii_alphabetic() {
            return Err(invalid("Excel column reference is invalid"));
        }
        let value = (ch.to_ascii_uppercase() as u8 - b'A' + 1) as usize;
        index = index
            .checked_mul(26)
            .and_then(|index| index.checked_add(value))
            .ok_or_else(|| invalid("Excel column reference is too large"))?;
    }
    if index == 0 {
        return Err(invalid("Excel column reference is required"));
    }
    Ok(index - 1)
}

fn invalid(message: &str) -> TransformError {
    TransformError::new(TransformErrorKind::InvalidInput, message)
}
