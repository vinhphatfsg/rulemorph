#[cfg(test)]
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use serde_json::Value as JsonValue;

use crate::error::{ErrorCode, RuleError};
use crate::model::{Mapping, RuleFile, V2Branch, V2RuleStep};
use crate::path::{PathToken, parse_path};
use crate::transform::BRANCH_MAX_DEPTH;
use crate::{RuleFormat, parse_rule_file_with_format};

use super::ValidationCtx;

#[derive(Clone, Default)]
pub(super) struct OutputContract {
    pub(super) possible_outputs: HashSet<Vec<PathToken>>,
    pub(super) guaranteed_outputs: HashSet<Vec<PathToken>>,
    pub(super) mergeable_object: bool,
}

#[derive(Clone)]
pub(super) struct BranchGraphState {
    allowed_root: Option<PathBuf>,
    base_dir_error: Option<String>,
    stack: Vec<PathBuf>,
    contract_cache: Rc<RefCell<HashMap<BranchContractCacheKey, OutputContract>>>,
    #[cfg(test)]
    read_count: Rc<Cell<usize>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BranchContractCacheKey {
    rule_path: PathBuf,
    base_dir: PathBuf,
}

impl BranchGraphState {
    pub(super) fn new(base_dir: &Path) -> Self {
        let (allowed_root, base_dir_error) = match base_dir.canonicalize() {
            Ok(root) => (Some(root), None),
            Err(err) => (
                None,
                Some(format!("failed to resolve branch base directory: {err}")),
            ),
        };
        Self {
            allowed_root,
            base_dir_error,
            stack: Vec::new(),
            contract_cache: Rc::new(RefCell::new(HashMap::new())),
            #[cfg(test)]
            read_count: Rc::new(Cell::new(0)),
        }
    }

    #[cfg(test)]
    fn read_count(&self) -> usize {
        self.read_count.get()
    }
}

pub(super) fn collect_branch_contract(
    ctx: &mut ValidationCtx<'_>,
    branch: &V2Branch,
    branch_path: &str,
) -> Option<OutputContract> {
    let base_dir = ctx.base_dir.clone()?;
    let mut graph = ctx.branch_graph.take()?;

    let then_contract = if branch.then.trim().is_empty() {
        None
    } else {
        collect_target_contract(
            ctx,
            &mut graph,
            &base_dir,
            &branch.then,
            &format!("{}.then", branch_path),
        )
    };
    let else_contract = branch.r#else.as_deref().and_then(|target| {
        if target.trim().is_empty() {
            None
        } else {
            collect_target_contract(
                ctx,
                &mut graph,
                &base_dir,
                target,
                &format!("{}.else", branch_path),
            )
        }
    });

    ctx.branch_graph = Some(graph);

    if !branch.return_ {
        if let Some(contract) = &then_contract
            && !contract.mergeable_object
        {
            ctx.push(
                ErrorCode::InvalidStep,
                "return:false branch output must be an object",
                format!("{}.then", branch_path),
            );
        }
        if let Some(contract) = &else_contract
            && !contract.mergeable_object
        {
            ctx.push(
                ErrorCode::InvalidStep,
                "return:false branch output must be an object",
                format!("{}.else", branch_path),
            );
        }
    }

    Some(combine_branch_contracts(then_contract, else_contract))
}

fn collect_target_contract(
    ctx: &mut ValidationCtx<'_>,
    graph: &mut BranchGraphState,
    base_dir: &Path,
    target: &str,
    target_path: &str,
) -> Option<OutputContract> {
    let (rule, yaml, child_base_dir, cache_key) =
        match load_branch_rule(ctx, graph, base_dir, target, target_path)? {
            BranchRuleLoad::Cached(contract) => {
                graph.stack.pop();
                return Some(contract);
            }
            BranchRuleLoad::Loaded {
                rule,
                yaml,
                child_base_dir,
                cache_key,
            } => (rule, yaml, child_base_dir, cache_key),
        };
    if let Err(errors) =
        super::validate_branch_rule_file_with_source_and_graph(&rule, &yaml, &child_base_dir, graph)
    {
        ctx.errors
            .extend(prefix_branch_child_errors(errors, target_path));
        ctx.push(
            ErrorCode::InvalidStep,
            "branch rule failed validation",
            target_path,
        );
        graph.stack.pop();
        return None;
    }
    let contract = collect_rule_contract(ctx, graph, &rule, &child_base_dir, target_path);
    graph
        .contract_cache
        .borrow_mut()
        .insert(cache_key, contract.clone());
    graph.stack.pop();
    Some(contract)
}

fn prefix_branch_child_errors(errors: Vec<RuleError>, target_path: &str) -> Vec<RuleError> {
    errors
        .into_iter()
        .map(|mut err| {
            err.path = Some(match err.path {
                Some(path) if !path.is_empty() => format!("{target_path}.{path}"),
                _ => target_path.to_string(),
            });
            err
        })
        .collect()
}

enum BranchRuleLoad {
    Cached(OutputContract),
    Loaded {
        rule: RuleFile,
        yaml: String,
        child_base_dir: PathBuf,
        cache_key: BranchContractCacheKey,
    },
}

fn load_branch_rule(
    ctx: &mut ValidationCtx<'_>,
    graph: &mut BranchGraphState,
    base_dir: &Path,
    target: &str,
    target_path: &str,
) -> Option<BranchRuleLoad> {
    if graph.stack.len() >= BRANCH_MAX_DEPTH {
        ctx.push(
            ErrorCode::InvalidStep,
            "branch rule depth limit exceeded",
            target_path,
        );
        return None;
    }

    if let Some(message) = &graph.base_dir_error {
        ctx.push(ErrorCode::InvalidStep, message, target_path);
        return None;
    }

    let resolved = resolve_rule_path(base_dir, target);
    let canonical = match resolved.canonicalize() {
        Ok(path) => path,
        Err(err) => {
            ctx.push(
                ErrorCode::InvalidStep,
                &format!("failed to resolve branch rule: {err}"),
                target_path,
            );
            return None;
        }
    };
    if let Some(root) = &graph.allowed_root
        && !canonical.starts_with(root)
    {
        ctx.push(
            ErrorCode::InvalidStep,
            "branch rule path must stay under the base directory",
            target_path,
        );
        return None;
    }
    if graph.stack.iter().any(|path| path == &canonical) {
        ctx.push(
            ErrorCode::InvalidStep,
            "branch rule cycle detected",
            target_path,
        );
        return None;
    }
    graph.stack.push(canonical.clone());
    let child_base_dir = resolved
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let cache_key = BranchContractCacheKey {
        rule_path: canonical,
        base_dir: child_base_dir.clone(),
    };

    if let Some(contract) = graph.contract_cache.borrow().get(&cache_key).cloned() {
        return Some(BranchRuleLoad::Cached(contract));
    }

    #[cfg(test)]
    graph.read_count.set(graph.read_count.get() + 1);
    let yaml = match fs::read_to_string(&resolved) {
        Ok(yaml) => yaml,
        Err(err) => {
            ctx.push(
                ErrorCode::InvalidStep,
                &format!("failed to read branch rule: {err}"),
                target_path,
            );
            graph.stack.pop();
            return None;
        }
    };
    let format = RuleFormat::from_path(&resolved);
    let rule = match parse_rule_file_with_format(&yaml, format) {
        Ok(rule) => rule,
        Err(err) => {
            ctx.push(
                ErrorCode::InvalidStep,
                &format!("failed to parse branch rule: {err}"),
                target_path,
            );
            graph.stack.pop();
            return None;
        }
    };
    Some(BranchRuleLoad::Loaded {
        rule,
        yaml,
        child_base_dir,
        cache_key,
    })
}

fn collect_rule_contract(
    ctx: &mut ValidationCtx<'_>,
    graph: &mut BranchGraphState,
    rule: &RuleFile,
    base_dir: &Path,
    branch_error_path: &str,
) -> OutputContract {
    let mut contract = OutputContract {
        mergeable_object: true,
        ..OutputContract::default()
    };

    if let Some(steps) = rule.steps.as_deref() {
        collect_steps_contract(
            ctx,
            graph,
            steps,
            base_dir,
            branch_error_path,
            &mut contract,
        );
    } else {
        add_mapping_targets(&rule.mappings, &mut contract);
    }

    if let Some(finalize) = rule.finalize.as_ref() {
        contract = if let Some(wrap) = finalize.wrap.as_ref() {
            finalize_wrap_contract(wrap)
        } else {
            OutputContract {
                mergeable_object: false,
                ..OutputContract::default()
            }
        };
    }

    contract
}

fn collect_steps_contract(
    ctx: &mut ValidationCtx<'_>,
    graph: &mut BranchGraphState,
    steps: &[V2RuleStep],
    base_dir: &Path,
    branch_error_path: &str,
    contract: &mut OutputContract,
) {
    for (index, step) in steps.iter().enumerate() {
        if let Some(mappings) = step.mappings.as_deref() {
            add_mapping_targets(mappings, contract);
        }
        if let Some(branch) = step.branch.as_ref() {
            let child_path = format!("{}.steps[{}].branch", branch_error_path, index);
            let child_contract =
                collect_child_branch_contract(ctx, graph, base_dir, branch, &child_path);
            if !branch.return_
                && let Some(child_contract) = child_contract
            {
                if !child_contract.mergeable_object {
                    ctx.push(
                        ErrorCode::InvalidStep,
                        "return:false branch output must be an object",
                        format!("{}.then", child_path),
                    );
                    contract.mergeable_object = false;
                }
                contract
                    .possible_outputs
                    .extend(child_contract.possible_outputs.iter().cloned());
                contract
                    .guaranteed_outputs
                    .extend(child_contract.guaranteed_outputs.iter().cloned());
            } else if let Some(child_contract) = child_contract {
                contract
                    .possible_outputs
                    .extend(child_contract.possible_outputs.iter().cloned());
                contract.mergeable_object &= child_contract.mergeable_object;
            }
        }
    }
}

fn collect_child_branch_contract(
    ctx: &mut ValidationCtx<'_>,
    graph: &mut BranchGraphState,
    base_dir: &Path,
    branch: &V2Branch,
    branch_path: &str,
) -> Option<OutputContract> {
    let then_contract = collect_target_contract(
        ctx,
        graph,
        base_dir,
        &branch.then,
        &format!("{}.then", branch_path),
    );
    let else_contract = branch.r#else.as_deref().and_then(|target| {
        collect_target_contract(
            ctx,
            graph,
            base_dir,
            target,
            &format!("{}.else", branch_path),
        )
    });
    Some(combine_branch_contracts(then_contract, else_contract))
}

fn combine_branch_contracts(
    then_contract: Option<OutputContract>,
    else_contract: Option<OutputContract>,
) -> OutputContract {
    let mut output = OutputContract {
        mergeable_object: true,
        ..OutputContract::default()
    };
    if let Some(contract) = then_contract {
        output
            .possible_outputs
            .extend(contract.possible_outputs.iter().cloned());
        output
            .guaranteed_outputs
            .extend(contract.guaranteed_outputs.iter().cloned());
        output.mergeable_object &= contract.mergeable_object;
    }
    if let Some(contract) = else_contract {
        output
            .possible_outputs
            .extend(contract.possible_outputs.iter().cloned());
        output.guaranteed_outputs = output
            .guaranteed_outputs
            .intersection(&contract.guaranteed_outputs)
            .cloned()
            .collect();
        output.mergeable_object &= contract.mergeable_object;
    } else {
        output.guaranteed_outputs.clear();
    }
    output
}

fn add_mapping_targets(mappings: &[Mapping], contract: &mut OutputContract) {
    for mapping in mappings {
        let Ok(tokens) = parse_path(&mapping.target) else {
            continue;
        };
        if tokens
            .iter()
            .any(|token| matches!(token, PathToken::Index(_)))
        {
            continue;
        }
        contract.possible_outputs.insert(tokens.clone());
        if mapping.when.is_none() {
            contract.guaranteed_outputs.insert(tokens);
        }
    }
}

fn finalize_wrap_contract(wrap: &JsonValue) -> OutputContract {
    let Some(map) = wrap.as_object() else {
        return OutputContract {
            mergeable_object: false,
            ..OutputContract::default()
        };
    };
    let mut contract = OutputContract {
        mergeable_object: true,
        ..OutputContract::default()
    };
    for key in map.keys() {
        let tokens = vec![PathToken::Key(key.clone())];
        contract.possible_outputs.insert(tokens.clone());
        contract.guaranteed_outputs.insert(tokens);
    }
    contract
}

fn resolve_rule_path(base_dir: &Path, path: &str) -> PathBuf {
    let rule_path = PathBuf::from(path);
    if rule_path.is_absolute() {
        rule_path
    } else {
        base_dir.join(rule_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::locator::YamlLocator;

    #[test]
    fn branch_graph_reads_deep_branch_chain_linearly() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rulemorph-branch-cache-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).expect("create temp dir");

        let depth = 40;
        for index in 0..depth {
            let next = index + 1;
            let yaml = format!(
                r#"version: 2
input:
  format: json
  json: {{}}
steps:
  - branch:
      when: {{ eq: [1, 1] }}
      then: r{next}.yaml
      return: false
  - mappings:
      - target: own{index}
        expr: "@input.value"
"#
            );
            fs::write(temp_dir.join(format!("r{index}.yaml")), yaml).expect("write branch rule");
        }
        fs::write(
            temp_dir.join(format!("r{depth}.yaml")),
            r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: leaf
    expr: "@input.value"
"#,
        )
        .expect("write leaf rule");

        let main_yaml = r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: r0.yaml
      return: false
  - mappings:
      - target: observed
        expr: "@out.leaf"
"#;
        let rule =
            parse_rule_file_with_format(main_yaml, RuleFormat::Yaml).expect("parse main rule");
        let locator = YamlLocator::from_str(main_yaml);
        let mut ctx = super::super::ValidationCtx::new(
            Some(&locator),
            rule.defs.keys().cloned().collect(),
            rule.codecs.clone(),
            Some(&temp_dir),
        );
        let graph = ctx.branch_graph.as_ref().expect("branch graph").clone();

        super::super::validate_rule_file_with_ctx(&rule, &mut ctx);
        let result = ctx.finish();
        assert!(
            result.is_ok(),
            "generated branch chain should validate: {result:?}"
        );
        assert!(
            graph.read_count() <= depth + 1,
            "branch graph should read each branch rule once, got {} reads for depth {depth}",
            graph.read_count()
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
