use super::*;

mod conflict;
mod flatten;
mod mutation;
mod refs;

pub(super) use conflict::{has_duplicate_path, has_path_conflict};
pub(super) use flatten::flatten_object;
pub(super) use mutation::{
    merge_object, remove_path, set_path, set_path_object_only, set_path_with_indexes,
};
pub(super) use refs::{parse_path_tokens, parse_ref, parse_source};
