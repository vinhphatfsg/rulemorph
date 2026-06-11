use super::*;

impl<'a> DocumentBuilder<'a> {
    pub(super) fn add_heading(
        &mut self,
        node: Node<'_>,
        level: u8,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> Result<String, TransformError> {
        let id = self.next_block_id();
        let text = self.normalized_text(&plain_text(node))?;
        let section_id = self.open_heading_section(level, text.clone(), id.clone());
        if top_level_content && level == 1 && self.title.is_none() && !text.is_empty() {
            self.title = Some(text.clone());
        }
        self.push_body_text(&text)?;
        let inlines = self.collect_inlines(node, &id)?;
        let mut block = self.common_block(
            id.clone(),
            "heading",
            section_id,
            parent_block_id,
            text,
            inlines,
        );
        block.insert("level".to_string(), json!(level));
        self.push_block(block, false);
        Ok(id)
    }

    pub(super) fn add_paragraph(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> Result<String, TransformError> {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let text = self.normalized_text(&plain_text(node))?;
        self.push_body_text(&text)?;
        let inlines = self.collect_inlines(node, &id)?;
        let section_content = self.is_section_content_block(
            &section_id,
            parent_block_id.as_deref(),
            top_level_content,
        );
        let block = self.common_block(
            id.clone(),
            "paragraph",
            section_id,
            parent_block_id,
            text,
            inlines,
        );
        self.push_block(block, section_content);
        Ok(id)
    }

    pub(super) fn add_list(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> Result<Option<String>, TransformError> {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let (ordered, start, tight) = {
            let data = node.data.borrow();
            match &data.value {
                NodeValue::List(list) => {
                    (list.list_type == ListType::Ordered, list.start, list.tight)
                }
                _ => (false, 0, false),
            }
        };
        let text = self.normalized_container_text(node)?;
        let section_content = self.is_section_content_block(
            &section_id,
            parent_block_id.as_deref(),
            top_level_content,
        );
        let mut block = self.common_block(
            id.clone(),
            "list",
            section_id,
            parent_block_id,
            text,
            Vec::new(),
        );
        block.insert("ordered".to_string(), json!(ordered));
        block.insert(
            "start".to_string(),
            if ordered {
                json!(start)
            } else {
                JsonValue::Null
            },
        );
        block.insert("tight".to_string(), json!(tight));
        block.insert("item_ids".to_string(), JsonValue::Array(Vec::new()));
        let block_index = self.push_block(block, section_content);

        let mut item_ids = Vec::new();
        for (offset, child) in node.children().enumerate() {
            let ordinal = if ordered { Some(start + offset) } else { None };
            if let Some(item_id) = self.process_list_child(child, id.clone(), ordinal)? {
                item_ids.push(JsonValue::String(item_id));
            }
        }
        self.blocks[block_index].insert("item_ids".to_string(), JsonValue::Array(item_ids));
        Ok(Some(id))
    }

    pub(super) fn process_list_child(
        &mut self,
        node: Node<'_>,
        parent_block_id: String,
        ordinal: Option<usize>,
    ) -> Result<Option<String>, TransformError> {
        enum ListChild {
            Item,
            Task(Option<char>),
            Other,
        }
        let child = {
            let data = node.data.borrow();
            match &data.value {
                NodeValue::Item(_) => ListChild::Item,
                NodeValue::TaskItem(task) => ListChild::Task(task.symbol),
                _ => ListChild::Other,
            }
        };
        match child {
            ListChild::Item => self.add_list_item(node, Some(parent_block_id), ordinal, None),
            ListChild::Task(symbol) => {
                self.add_list_item(node, Some(parent_block_id), ordinal, Some(symbol.is_some()))
            }
            ListChild::Other => self.process_block(node, Some(parent_block_id), false),
        }
    }

    pub(super) fn add_list_item(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        ordinal: Option<usize>,
        checked: Option<bool>,
    ) -> Result<Option<String>, TransformError> {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let text = self.normalized_container_text(node)?;
        let mut block = self.common_block(
            id.clone(),
            "list_item",
            section_id,
            parent_block_id,
            text,
            Vec::new(),
        );
        block.insert(
            "ordinal".to_string(),
            ordinal.map_or(JsonValue::Null, |value| json!(value)),
        );
        block.insert(
            "checked".to_string(),
            checked.map_or(JsonValue::Null, JsonValue::Bool),
        );
        block.insert("child_block_ids".to_string(), JsonValue::Array(Vec::new()));
        let block_index = self.push_block(block, false);

        let mut child_ids = Vec::new();
        for child in node.children() {
            if let Some(child_id) = self.process_block(child, Some(id.clone()), false)? {
                child_ids.push(JsonValue::String(child_id));
            }
        }
        self.blocks[block_index].insert("child_block_ids".to_string(), JsonValue::Array(child_ids));
        Ok(Some(id))
    }

    pub(super) fn add_blockquote(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> Result<Option<String>, TransformError> {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let text = self.normalized_container_text(node)?;
        let section_content = self.is_section_content_block(
            &section_id,
            parent_block_id.as_deref(),
            top_level_content,
        );
        let mut block = self.common_block(
            id.clone(),
            "blockquote",
            section_id,
            parent_block_id,
            text,
            Vec::new(),
        );
        block.insert("child_block_ids".to_string(), JsonValue::Array(Vec::new()));
        let block_index = self.push_block(block, section_content);

        let mut child_ids = Vec::new();
        for child in node.children() {
            if let Some(child_id) = self.process_block(child, Some(id.clone()), false)? {
                child_ids.push(JsonValue::String(child_id));
            }
        }
        self.blocks[block_index].insert("child_block_ids".to_string(), JsonValue::Array(child_ids));
        Ok(Some(id))
    }

    pub(super) fn add_code_block(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> Result<String, TransformError> {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let (info, literal) = {
            let data = node.data.borrow();
            match &data.value {
                NodeValue::CodeBlock(code) => (code.info.clone(), code.literal.clone()),
                _ => (String::new(), String::new()),
            }
        };
        self.check_text_bytes(&info)?;
        let language = info.split_whitespace().next().unwrap_or("").to_string();
        let text = self.checked_text(literal.trim_end_matches('\n').to_string())?;
        self.push_body_text(&text)?;
        let section_content = self.is_section_content_block(
            &section_id,
            parent_block_id.as_deref(),
            top_level_content,
        );
        let mut block = self.common_block(
            id.clone(),
            "code_block",
            section_id.clone(),
            parent_block_id,
            text.clone(),
            Vec::new(),
        );
        block.insert("language".to_string(), JsonValue::String(language.clone()));
        block.insert("info".to_string(), JsonValue::String(info.clone()));
        self.push_block(block, section_content);
        if self.markdown.include.code_blocks {
            self.code_blocks.push(json!({
                "block_id": id,
                "section_id": section_id,
                "language": language,
                "info": info,
                "text": text,
            }));
        }
        Ok(id)
    }

    pub(super) fn add_html_block(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> Result<Option<String>, TransformError> {
        let literal = {
            let data = node.data.borrow();
            match &data.value {
                NodeValue::HtmlBlock(html) => html.literal.clone(),
                _ => String::new(),
            }
        };
        let text = self.normalized_text(&html_to_text(&literal))?;
        let html = if self.markdown.include.raw_html {
            Some(self.raw_html(&literal)?)
        } else {
            None
        };
        self.push_body_text(&text)?;
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let section_content = self.is_section_content_block(
            &section_id,
            parent_block_id.as_deref(),
            top_level_content,
        );
        let mut block = self.common_block(
            id.clone(),
            "html_block",
            section_id,
            parent_block_id,
            text,
            Vec::new(),
        );
        if let Some(html) = html {
            block.insert("html".to_string(), JsonValue::String(html.clone()));
            self.push_block(block, section_content);
            self.raw_html.push(json!({
                "block_id": id,
                "kind": "block",
                "html": html,
            }));
        } else {
            self.push_block(block, section_content);
        }
        Ok(Some(id))
    }

    pub(super) fn add_thematic_break(
        &mut self,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> String {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let section_content = self.is_section_content_block(
            &section_id,
            parent_block_id.as_deref(),
            top_level_content,
        );
        let block = self.common_block(
            id.clone(),
            "thematic_break",
            section_id,
            parent_block_id,
            String::new(),
            Vec::new(),
        );
        self.push_block(block, section_content);
        id
    }
}
