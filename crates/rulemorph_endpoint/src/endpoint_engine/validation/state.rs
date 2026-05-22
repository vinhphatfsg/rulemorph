use std::collections::BTreeSet;
use std::path::PathBuf;

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct RuleRefUsage {
    pub(super) step: bool,
    pub(super) body_rule: bool,
    pub(super) catch_rule: bool,
    pub(super) branch_rule: bool,
}

impl RuleRefUsage {
    pub(super) fn step() -> Self {
        RuleRefUsage {
            step: true,
            ..RuleRefUsage::default()
        }
    }

    pub(super) fn body_rule() -> Self {
        RuleRefUsage {
            body_rule: true,
            ..RuleRefUsage::default()
        }
    }

    pub(super) fn catch_rule() -> Self {
        RuleRefUsage {
            catch_rule: true,
            ..RuleRefUsage::default()
        }
    }

    pub(super) fn branch_rule() -> Self {
        RuleRefUsage {
            branch_rule: true,
            ..RuleRefUsage::default()
        }
    }

    pub(super) fn merge(&mut self, other: RuleRefUsage) {
        self.step |= other.step;
        self.body_rule |= other.body_rule;
        self.catch_rule |= other.catch_rule;
        self.branch_rule |= other.branch_rule;
    }
}

#[derive(Debug, Default)]
pub(super) struct ValidationState {
    pub(super) validated_content: BTreeSet<PathBuf>,
}
