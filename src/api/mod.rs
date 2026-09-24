pub mod error;
pub mod page;
pub(crate) mod tree;

use std::time::Duration;

use crate::{
    api::error::Result,
    disk::{block::Block, file::File},
};

pub use crate::disk::file::DEFAULT_CACHE_BYTES;
pub use crate::disk::wal::Op;

pub type KV = File<Block>;

/// When a committed write survives what.
///
/// Every write goes to the write-ahead log first, and the tree file is only
/// rewritten by checkpoints, which always fsync. So the file is never left
/// half-updated, whatever the mode: the mode only decides how many of the
/// latest writes a power loss can take away.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// A write is handed to the OS before it returns, and fsynced only by the
    /// next checkpoint. Survives a process crash; a power loss or kernel panic
    /// loses the writes since the last checkpoint.
    #[default]
    Os,
    /// As `Os`, and a background thread also fsyncs the log every interval,
    /// so a power loss loses at most about that much.
    Periodic(Duration),
    /// A write returns only once it is fsynced. Writers that commit at the
    /// same time share one fsync (group commit), but a single thread writing
    /// one key at a time pays a full fsync per write: batch with `write_batch`.
    Sync,
}

/// How long committed records may wait in memory before they are written to
/// the OS, under `Durability::Os` and `Durability::Periodic`: whichever limit
/// is reached first triggers the write. `Durability::Sync` ignores it.
///
/// A record waiting in memory is lost if the process crashes, so this trades
/// the last few writes for speed: writing each record costs a system call,
/// which is most of what an insert costs. Checkpoints, `sync` and closing the
/// database write out everything waiting, whatever the limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteBuffer {
    /// Write once this many records are waiting. With 1, the default, every
    /// write reaches the OS before it returns: a process crash loses nothing.
    pub records: usize,
    /// ... or once the waiting records take this many bytes.
    pub bytes: usize,
    /// ... or once this much time has passed, checked by a background thread,
    /// so that records do not wait indefinitely when writes stop coming.
    pub interval: Option<Duration>,
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self {
            records: 1,
            bytes: 1024 * 1024,
            interval: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Options {
    /// Upper limit on the page cache; see `DEFAULT_CACHE_BYTES`.
    pub cache_bytes: usize,
    pub durability: Durability,
    pub write_buffer: WriteBuffer,
    /// Checkpoint once this much of the cache holds modified pages. Capped at
    /// half the cache: modified pages stay in memory until a checkpoint.
    ///
    /// The default is that cap. A checkpoint writes every modified page twice
    /// (once to the log, once in place), so the more of the tree's working set
    /// it can hold, the more writes each of those absorbs.
    pub checkpoint_dirty_bytes: usize,
    /// Checkpoint once the log has grown this much since the last one.
    pub checkpoint_wal_bytes: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cache_bytes: DEFAULT_CACHE_BYTES,
            durability: Durability::default(),
            write_buffer: WriteBuffer::default(),
            checkpoint_dirty_bytes: DEFAULT_CACHE_BYTES / 2,
            checkpoint_wal_bytes: 64 * 1024 * 1024,
        }
    }
}

pub trait Store {
    fn lookup(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn insert(&self, key: &[u8], val: &[u8]) -> Result<()>;
    fn remove(&self, key: &[u8]) -> Result<()>;

    /// Apply all `ops` in order, atomically: after a crash, either all of
    /// them are there or none is. One log record, one commit wait.
    fn write_batch(&self, ops: &[Op<'_>]) -> Result<()>;

    fn is_empty(&self) -> bool;

    /// Get lowest/smallest key stored in the tree, or none if tree is empty.
    fn min(&self) -> Result<Option<Vec<u8>>>;

    /// Get highest/biggest key stored in the tree, or none if tree is empty.
    fn max(&self) -> Result<Option<Vec<u8>>>;

    /// Get smallest key that is strictly greater than given one, if any.
    fn above(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Get biggest key that is strictly lesser than given one, if any.
    fn below(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
}
