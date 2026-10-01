use crate::{OutputError, OutputErrorKind, RepairEdit, limits};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct RepairCandidate {
    pub text: String,
    pub repairs: Vec<RepairEdit>,
}

/// Propose comma insertions only. This is not a semantic preservation guarantee.
pub fn repair_json_candidate(raw: &str) -> Result<RepairCandidate, OutputError> {
    limits::check_text(raw)?;
    let mut parser = Parser {
        raw,
        pos: 0,
        nodes: 0,
        edits: vec![],
    };
    let scanned = parser.value(0).and_then(|_| {
        parser.whitespace();
        if parser.pos != raw.len() {
            Err(parser.error(
                OutputErrorKind::AmbiguousRepair,
                "trailing text or ambiguous quote boundary",
            ))
        } else {
            Ok(())
        }
    });
    if let Err(mut error) = scanned {
        error.repairs = parser.edits;
        return Err(error);
    }
    let mut text = String::with_capacity(raw.len() + parser.edits.len());
    let mut start = 0;
    for edit in &parser.edits {
        text.push_str(&raw[start..edit.byte_offset]);
        text.push(',');
        start = edit.byte_offset;
    }
    text.push_str(&raw[start..]);
    let value = rulemorph::serde_guard::parse_json_value_strict(&text).map_err(|message| {
        let mut error = parser.error(
            if message.contains("duplicate") {
                OutputErrorKind::DuplicateKey
            } else {
                OutputErrorKind::InputSyntax
            },
            "candidate is not strict JSON",
        );
        error.repairs = parser.edits.clone();
        error
    })?;
    limits::check_text(&text)
        .and_then(|_| limits::check_value(&value, limits::MAX_NODES, limits::MAX_BYTES).map(|_| ()))
        .map_err(|mut error| {
            error.repairs = parser.edits.clone();
            error
        })?;
    Ok(RepairCandidate {
        text,
        repairs: parser.edits,
    })
}

struct Parser<'a> {
    raw: &'a str,
    pos: usize,
    nodes: usize,
    edits: Vec<RepairEdit>,
}
impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.raw.as_bytes().get(self.pos).copied()
    }
    fn whitespace(&mut self) {
        while self
            .peek()
            .is_some_and(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.pos += 1;
        }
    }
    fn error(&self, kind: OutputErrorKind, message: &str) -> OutputError {
        let mut error = OutputError::new(kind, message);
        error.byte_offset = Some(self.pos);
        error
    }
    fn value(&mut self, depth: usize) -> Result<(), OutputError> {
        self.whitespace();
        self.nodes += 1;
        if depth > limits::MAX_DEPTH || self.nodes > limits::MAX_NODES {
            return Err(limits::limit("repair depth/node limit exceeded"));
        }
        match self.peek() {
            Some(b'"') => self.string(),
            Some(b'[') => self.container(depth, false),
            Some(b'{') => self.container(depth, true),
            None => Err(self.error(
                OutputErrorKind::IncompleteInput,
                "missing value or closing delimiter",
            )),
            Some(b'-' | b'0'..=b'9' | b't' | b'f' | b'n') => self.primitive(),
            _ => Err(self.error(
                OutputErrorKind::AmbiguousRepair,
                "unsupported syntax or ambiguous quote boundary",
            )),
        }
    }
    fn string(&mut self) -> Result<(), OutputError> {
        let start = self.pos;
        self.pos += 1;
        while let Some(byte) = self.peek() {
            self.pos += 1;
            if byte == b'\\' {
                if self.peek().is_none() {
                    break;
                }
                self.pos += 1;
            } else if byte == b'"' {
                return serde_json::from_str::<String>(&self.raw[start..self.pos])
                    .map(|_| ())
                    .map_err(|_| {
                        self.error(
                            OutputErrorKind::InputSyntax,
                            "invalid JSON string escape/control character",
                        )
                    });
            }
        }
        Err(self.error(
            OutputErrorKind::IncompleteInput,
            "unclosed quote; repair requires confirmation or regeneration",
        ))
    }
    fn primitive(&mut self) -> Result<(), OutputError> {
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|b| !matches!(b, b' ' | b'\n' | b'\r' | b'\t' | b',' | b']' | b'}'))
        {
            self.pos += 1;
        }
        serde_json::from_str::<Value>(&self.raw[start..self.pos])
            .map(|_| ())
            .map_err(|_| {
                self.error(
                    OutputErrorKind::InputSyntax,
                    "invalid primitive; no guessed token repair",
                )
            })
    }
    fn container(&mut self, depth: usize, object: bool) -> Result<(), OutputError> {
        let close = if object { b'}' } else { b']' };
        self.pos += 1;
        self.whitespace();
        if self.peek() == Some(close) {
            self.pos += 1;
            return Ok(());
        }
        let mut entries = 0;
        loop {
            entries += 1;
            if entries > 256 {
                return Err(limits::limit("repair container entries exceed 256"));
            }
            if object {
                if self.peek() != Some(b'"') {
                    return Err(self.error(
                        if self.peek().is_none() {
                            OutputErrorKind::IncompleteInput
                        } else {
                            OutputErrorKind::AmbiguousRepair
                        },
                        "object key must be an explicitly quoted string",
                    ));
                }
                self.string()?;
                self.whitespace();
                if self.peek() != Some(b':') {
                    return Err(self.error(
                        OutputErrorKind::AmbiguousRepair,
                        "missing colon or ambiguous quote boundary",
                    ));
                }
                self.pos += 1;
            }
            self.value(depth + 1)?;
            let previous_end = self.pos;
            self.whitespace();
            match self.peek() {
                Some(byte) if byte == close => {
                    self.pos += 1;
                    return Ok(());
                }
                Some(b',') => {
                    self.pos += 1;
                    self.whitespace();
                }
                None => {
                    return Err(self.error(
                        OutputErrorKind::IncompleteInput,
                        "unclosed container; do not infer generated content is complete",
                    ));
                }
                Some(byte)
                    if self.pos > previous_end
                        && (if object {
                            byte == b'"'
                        } else {
                            matches!(
                                byte,
                                b'"' | b'{' | b'[' | b'-' | b'0'..=b'9' | b't' | b'f' | b'n'
                            )
                        }) =>
                {
                    if self.edits.len() >= 128 {
                        return Err(limits::limit("repair edit limit is 128"));
                    }
                    self.edits.push(RepairEdit {
                        byte_offset: self.pos,
                        inserted: ",".into(),
                    });
                }
                _ => {
                    return Err(self.error(
                        OutputErrorKind::AmbiguousRepair,
                        "mismatched delimiter or ambiguous token boundary",
                    ));
                }
            }
        }
    }
}
