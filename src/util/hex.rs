pub(crate) fn hex(src: &[u8]) -> String {
    src.into_iter()
        .map(|x| format!("{:02x}", x))
        .collect::<Vec<_>>()
        .concat()
}
