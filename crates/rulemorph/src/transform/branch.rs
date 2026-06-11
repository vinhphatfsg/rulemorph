use super::*;

pub(super) fn merge_branch_output(
    out: &mut JsonValue,
    other: &JsonValue,
    path: &str,
) -> Result<(), TransformError> {
    let out_map = out.as_object_mut().ok_or_else(|| {
        TransformError::new(TransformErrorKind::InvalidTarget, "output must be object")
            .with_path(path)
    })?;
    let other_map = other.as_object().ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::InvalidTarget,
            "branch output must be object",
        )
        .with_path(path)
    })?;
    merge_object_maps(out_map, other_map);
    Ok(())
}

#[derive(Default)]
pub(super) struct BranchContext {
    stack: Vec<PathBuf>,
    allowed_root: Option<PathBuf>,
}

impl BranchContext {
    pub(super) fn enter(
        &mut self,
        base_dir: Option<&Path>,
        target: &str,
    ) -> Result<BranchPathGuard, TransformError> {
        if self.stack.len() >= BRANCH_MAX_DEPTH {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "branch rule depth limit exceeded",
            ));
        }
        let resolved = resolve_rule_path(base_dir, target);
        let canonical = resolved.canonicalize().map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to resolve branch rule: {}", err),
            )
        })?;
        let allowed_root = match (&self.allowed_root, base_dir) {
            (Some(root), _) => Some(root.clone()),
            (None, Some(base_dir)) => Some(base_dir.canonicalize().map_err(|err| {
                TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to resolve branch base directory: {}", err),
                )
            })?),
            (None, None) => None,
        };
        if let Some(root) = &allowed_root
            && !canonical.starts_with(root)
        {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "branch rule path must stay under the base directory",
            ));
        }
        if self.stack.iter().any(|path| path == &canonical) {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "branch rule cycle detected",
            ));
        }
        if self.allowed_root.is_none() {
            self.allowed_root = allowed_root;
        }
        self.stack.push(canonical);
        Ok(BranchPathGuard)
    }

    pub(super) fn allowed_root(&self) -> Option<&Path> {
        self.allowed_root.as_deref()
    }

    pub(super) fn exit(&mut self, _guard: BranchPathGuard) {
        self.stack.pop();
    }
}

pub(super) struct BranchPathGuard;

fn merge_object_maps(out_map: &mut Map<String, JsonValue>, other_map: &Map<String, JsonValue>) {
    for (key, other_value) in other_map {
        match (out_map.get_mut(key), other_value) {
            (Some(JsonValue::Object(out_obj)), JsonValue::Object(other_obj)) => {
                merge_object_maps(out_obj, other_obj);
            }
            _ => {
                out_map.insert(key.clone(), other_value.clone());
            }
        }
    }
}

pub(super) fn load_rule_from_path(
    base_dir: Option<&Path>,
    path: &str,
    allowed_root: Option<&Path>,
) -> Result<(RuleFile, PathBuf), TransformError> {
    let resolved = resolve_rule_path(base_dir, path);
    if let Some(allowed_root) = allowed_root {
        let canonical_resolved = resolved.canonicalize().map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to resolve branch rule: {}", err),
            )
            .with_path(path)
        })?;
        if !canonical_resolved.starts_with(allowed_root) {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "branch rule path must stay under the base directory",
            )
            .with_path(path));
        }
    }
    let yaml = std::fs::read_to_string(&resolved).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to read rule: {}", err),
        )
        .with_path(path)
    })?;
    let format = crate::RuleFormat::from_path(&resolved);
    let rule = crate::parse_rule_file_with_format(&yaml, format).map_err(|err| {
        TransformError::new(TransformErrorKind::InvalidInput, err.to_string()).with_path(path)
    })?;
    let resolved_base = resolved
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    Ok((rule, resolved_base))
}

fn resolve_rule_path(base_dir: Option<&Path>, path: &str) -> PathBuf {
    let rule_path = PathBuf::from(path);
    if rule_path.is_absolute() {
        rule_path
    } else if let Some(base_dir) = base_dir {
        base_dir.join(rule_path)
    } else {
        rule_path
    }
}
