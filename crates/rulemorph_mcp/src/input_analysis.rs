mod candidates;
mod stats;

pub(crate) use candidates::{build_input_paths, select_candidates};
pub(crate) use stats::{analyze_records, stats_to_json};
