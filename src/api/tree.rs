use crate::api::error::Result;
use crate::api::page::Page;
use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard};

pub trait Tree<P: Page> {
    /// Get an immutable reference to a root page.
    fn root(&self) -> MappedRwLockReadGuard<'_, P>;

    /// Get an immutable reference to a page having given id, if such page exists.
    fn page(&self, id: u32) -> Option<MappedRwLockReadGuard<'_, P>>;

    /// Get a mutable reference to a root page.
    fn root_mut(&self) -> MappedRwLockWriteGuard<'_, P>;

    /// Get a mutable reference to a page having given id, if such page exists.
    fn page_mut(&self, id: u32) -> Option<MappedRwLockWriteGuard<'_, P>>;

    /// Check that the page of given id is cached (if not then load it from disk)
    fn cache(&self, id: u32) -> std::io::Result<()>;

    /// Mark page with given id as dirty and thus eligible for flushing to the disk.
    fn mark(&self, id: u32);

    /// Flush all pages marked as dirty to the disk.
    fn flush(&self) -> Result<()>;

    /// Reserve the provided page id - such id will never be returned by `next_id` until freed.
    fn next_id(&self) -> Result<u32>;

    /// Un-reserve the provided page id making it available for future via `next_id`.
    fn free_id(&self, id: u32);

    /// Split given page into two subpages containing ~equal number of entries.
    fn split(&self, id: u32, parent_id: u32) -> Result<()>;

    /// Check reference consistency a between parent page and a child page.
    fn check(&self, parent_id: u32, page_id: u32) -> Result<()>;

    /// Merge page `src_id` into page `dst_id`, effectively removing page `src_id`.
    fn merge(&self, src_id: u32, dst_id: u32) -> Result<()>;

    /// Dump tree representation to a string where keys values are presented as hex strings.
    /// Intended to use for debugging purposes only.
    fn dump(&self) -> String;
}
