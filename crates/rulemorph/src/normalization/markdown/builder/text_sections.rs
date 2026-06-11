use super::*;

impl<'a> DocumentBuilder<'a> {
    pub(super) fn normalized_text(&self, value: &str) -> Result<String, TransformError> {
        self.checked_text(normalize_text(value, self.markdown))
    }

    pub(super) fn normalized_container_text(
        &self,
        node: Node<'_>,
    ) -> Result<String, TransformError> {
        let text = normalize_text(&plain_text(node), self.markdown);
        if self.container_text_needs_limit() {
            self.check_text_bytes(&text)?;
        }
        Ok(text)
    }

    pub(super) fn container_text_needs_limit(&self) -> bool {
        (self.markdown.include.blocks && self.markdown.records != MarkdownRecordsMode::TableRows)
            || (self.markdown.records == MarkdownRecordsMode::Sections
                && self.markdown.include.body_text)
    }

    pub(super) fn raw_html(&self, value: &str) -> Result<String, TransformError> {
        self.checked_text(normalize_raw_html(value, self.markdown))
    }

    pub(super) fn checked_text(&self, value: String) -> Result<String, TransformError> {
        self.check_text_bytes(&value)?;
        Ok(value)
    }

    pub(super) fn check_text_bytes(&self, value: &str) -> Result<(), TransformError> {
        if value.len() > self.options.max_text_bytes {
            Err(invalid("input exceeds max_text_bytes"))
        } else {
            Ok(())
        }
    }

    pub(super) fn collects_document_body_text(&self) -> bool {
        self.markdown.records == MarkdownRecordsMode::Document && self.markdown.include.body_text
    }

    pub(super) fn push_body_text(&mut self, value: &str) -> Result<(), TransformError> {
        if !self.collects_document_body_text() {
            return Ok(());
        }
        let value = normalize_text(value, self.markdown);
        if value.is_empty() {
            return Ok(());
        }
        let needs_space = !self.body_text.is_empty()
            && !self.body_text.ends_with(char::is_whitespace)
            && !value.starts_with(no_space_before);
        let next_len = self
            .body_text
            .len()
            .saturating_add(usize::from(needs_space))
            .saturating_add(value.len());
        if next_len > self.options.max_text_bytes {
            return Err(invalid("input exceeds max_text_bytes"));
        }
        if needs_space {
            self.body_text.push(' ');
        }
        self.body_text.push_str(&value);
        Ok(())
    }

    pub(super) fn common_block(
        &self,
        id: String,
        block_type: &str,
        section_id: String,
        parent_block_id: Option<String>,
        text: String,
        inlines: Vec<JsonValue>,
    ) -> Map<String, JsonValue> {
        let mut block = Map::new();
        block.insert("id".to_string(), JsonValue::String(id));
        block.insert(
            "type".to_string(),
            JsonValue::String(block_type.to_string()),
        );
        block.insert("section_id".to_string(), JsonValue::String(section_id));
        block.insert(
            "parent_block_id".to_string(),
            parent_block_id.map_or(JsonValue::Null, JsonValue::String),
        );
        block.insert("text".to_string(), JsonValue::String(text));
        block.insert("inlines".to_string(), JsonValue::Array(inlines));
        block
    }

    pub(super) fn push_block(
        &mut self,
        block: Map<String, JsonValue>,
        section_content: bool,
    ) -> usize {
        let index = self.blocks.len();
        let id = block
            .get("id")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_string();
        if section_content {
            self.push_content_block_id(&id);
        }
        self.block_index_by_id.insert(id, index);
        self.blocks.push(block);
        index
    }

    pub(super) fn is_section_content_block(
        &self,
        section_id: &str,
        parent_block_id: Option<&str>,
        top_level_content: bool,
    ) -> bool {
        if top_level_content {
            return true;
        }
        parent_block_id
            .and_then(|block_id| self.block_section_id(block_id))
            .is_some_and(|parent_section_id| parent_section_id != section_id)
    }

    pub(super) fn block_section_id(&self, block_id: &str) -> Option<&str> {
        let index = self.block_index_by_id.get(block_id).copied()?;
        self.blocks
            .get(index)?
            .get("section_id")
            .and_then(JsonValue::as_str)
    }

    pub(super) fn next_block_id(&mut self) -> String {
        let id = format!("b{}", self.next_block_number);
        self.next_block_number += 1;
        id
    }

    pub(super) fn current_section_id(&mut self) -> String {
        if self.active_section_id.is_none() {
            self.ensure_preamble();
        }
        self.active_section_id.clone().unwrap_or_default()
    }

    pub(super) fn ensure_preamble(&mut self) {
        if self.section_by_id.contains_key("preamble") {
            self.active_section_id = Some("preamble".to_string());
            return;
        }
        let section = FlatSection {
            id: "preamble".to_string(),
            level: 0,
            heading: String::new(),
            heading_block_id: None,
            path: Vec::new(),
            ordinal_path: Vec::new(),
            content_block_ids: Vec::new(),
            child_ids: Vec::new(),
            parent_id: None,
        };
        self.section_by_id
            .insert(section.id.clone(), self.flat_sections.len());
        self.flat_sections.push(section);
        self.active_section_id = Some("preamble".to_string());
    }

    pub(super) fn open_heading_section(
        &mut self,
        level: u8,
        heading: String,
        heading_block_id: String,
    ) -> String {
        let level_index = usize::from(level);
        self.counters[level_index] += 1;
        for counter in self.counters.iter_mut().skip(level_index + 1) {
            *counter = 0;
        }
        self.section_stack.retain(|item| item.level < level);
        let mut path = self
            .section_stack
            .iter()
            .map(|item| item.heading.clone())
            .collect::<Vec<_>>();
        path.push(heading.clone());
        let mut ordinal_path = self
            .section_stack
            .iter()
            .map(|item| item.ordinal)
            .collect::<Vec<_>>();
        ordinal_path.push(self.counters[level_index]);
        let id = self.section_id(level, self.counters[level_index]);
        let parent_id = self.section_stack.last().map(|item| item.id.clone());
        if let Some(parent_id) = &parent_id
            && let Some(parent) = self.section_mut(parent_id)
        {
            parent.child_ids.push(id.clone());
        }
        let section = FlatSection {
            id: id.clone(),
            level,
            heading: heading.clone(),
            heading_block_id: Some(heading_block_id),
            path,
            ordinal_path,
            content_block_ids: Vec::new(),
            child_ids: Vec::new(),
            parent_id,
        };
        self.section_by_id
            .insert(section.id.clone(), self.flat_sections.len());
        self.flat_sections.push(section);
        self.section_stack.push(StackSection {
            id: id.clone(),
            level,
            heading,
            ordinal: self.counters[level_index],
        });
        self.active_section_id = Some(id.clone());
        id
    }

    pub(super) fn section_id(&self, level: u8, ordinal: usize) -> String {
        let mut parts = self
            .section_stack
            .iter()
            .map(|item| format!("s{}-{}", item.level, item.ordinal))
            .collect::<Vec<_>>();
        parts.push(format!("s{}-{}", level, ordinal));
        parts.join(".")
    }

    pub(super) fn push_content_block_id(&mut self, block_id: &str) {
        let Some(section_id) = self.active_section_id.clone() else {
            return;
        };
        if let Some(section) = self.section_mut(&section_id) {
            section.content_block_ids.push(block_id.to_string());
        }
    }

    pub(super) fn section_mut(&mut self, id: &str) -> Option<&mut FlatSection> {
        self.section_by_id
            .get(id)
            .copied()
            .and_then(|index| self.flat_sections.get_mut(index))
    }

    pub(super) fn nested_sections(&self) -> Vec<Section> {
        self.flat_sections
            .iter()
            .filter(|section| section.parent_id.is_none())
            .map(|section| self.nested_section(section))
            .collect()
    }

    pub(super) fn nested_section(&self, section: &FlatSection) -> Section {
        let children = section
            .child_ids
            .iter()
            .filter_map(|child_id| {
                self.section_by_id
                    .get(child_id)
                    .and_then(|index| self.flat_sections.get(*index))
            })
            .map(|child| self.nested_section(child))
            .collect();
        Section {
            id: section.id.clone(),
            level: section.level,
            heading: section.heading.clone(),
            heading_block_id: section.heading_block_id.clone(),
            path: section.path.clone(),
            ordinal_path: section.ordinal_path.clone(),
            content_block_ids: section.content_block_ids.clone(),
            child_ids: section.child_ids.clone(),
            children,
        }
    }
}
