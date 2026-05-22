use crate::v2_operator::types::V2OperatorMetadata;

mod aggregate;
mod scoped;
mod sequence;

pub(super) const ARRAY_OPERATORS: &[V2OperatorMetadata] = &[
    scoped::map(),
    scoped::filter(),
    scoped::flat_map(),
    sequence::flatten(),
    sequence::take(),
    sequence::drop(),
    sequence::slice(),
    sequence::chunk(),
    sequence::zip(),
    sequence::zip_with(),
    sequence::unzip(),
    scoped::group_by(),
    scoped::key_by(),
    scoped::partition(),
    sequence::unique(),
    scoped::distinct_by(),
    scoped::sort_by(),
    scoped::find(),
    scoped::find_index(),
    sequence::index_of(),
    sequence::contains(),
    aggregate::sum(),
    aggregate::avg(),
    aggregate::min(),
    aggregate::max(),
    aggregate::reduce(),
    aggregate::fold(),
    aggregate::first(),
    aggregate::last(),
];
