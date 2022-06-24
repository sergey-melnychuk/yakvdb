pub fn hex(src: &[u8]) -> String {
    src.iter()
        .map(|x| format!("{:02x}", x))
        .collect::<Vec<_>>()
        .concat()
}
