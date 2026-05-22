pub(super) fn local_name(name: &[u8]) -> &[u8] {
    name.iter()
        .rposition(|value| *value == b':')
        .map(|index| &name[index + 1..])
        .unwrap_or(name)
}
