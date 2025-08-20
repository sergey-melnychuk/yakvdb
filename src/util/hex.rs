pub fn hex(src: &[u8]) -> String {
    src.iter()
        .map(|x| format!("{x:02x}"))
        .collect::<Vec<_>>()
        .concat()
}
