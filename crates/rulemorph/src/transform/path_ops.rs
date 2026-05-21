use super::*;

mod conflict;
mod flatten;
mod merge;
mod mutation;
mod refs;
mod target;

pub(super) use conflict::{has_duplicate_path, has_path_conflict};
pub(super) use flatten::flatten_object;
pub(super) use merge::merge_object;
pub(super) use mutation::{remove_path, set_path_object_only, set_path_with_indexes};
pub(super) use refs::{parse_path_tokens, parse_ref, parse_source};
pub(super) use target::set_path;
