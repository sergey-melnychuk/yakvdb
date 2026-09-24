pub mod block;
pub mod file;
// Not wired into `File` yet: the next steps of doc/wal.md do that.
#[allow(dead_code)]
pub(crate) mod wal;
