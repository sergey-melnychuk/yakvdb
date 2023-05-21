use crate::api::error::Result;
use crate::api::page::Page;
use std::cell::{Ref, RefMut};

pub trait Tree<P: Page> {
    fn lookup(&self, key: &[u8]) -> Result<Option<Ref<[u8]>>>;
    fn insert(&self, key: &[u8], val: &[u8]) -> Result<()>;
    fn remove(&self, key: &[u8]) -> Result<()>;

    fn is_empty(&self) -> bool;

    /// Get lowest/smallest key stored in the tree, or none if tree is empty.
    fn min(&self) -> Result<Option<Ref<[u8]>>>;

    /// Get highest/biggest key stored in the tree, or none if tree is empty.
    fn max(&self) -> Result<Option<Ref<[u8]>>>;

    /// Get smallest key that is strictly greater than given one, if any.
    fn above(&self, key: &[u8]) -> Result<Option<Ref<[u8]>>>;

    /// Get biggest key that is strictly lesser than given one, if any.
    fn below(&self, key: &[u8]) -> Result<Option<Ref<[u8]>>>;

    /// Get an immutable reference to a root page.
    fn root(&self) -> Ref<P>;

    /// Get an immutable reference to a page having given id, if such page exists.
    fn page(&self, id: u32) -> Option<Ref<P>>;

    /// Get a mutable reference to a root page.
    fn root_mut(&self) -> RefMut<P>;

    /// Get a mutable reference to a page having given id, if such page exists.
    fn page_mut(&self, id: u32) -> Option<RefMut<P>>;

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

    /// Merge page `src_id` into page `dst_id`, effectively removing page `src_id`.
    fn merge(&self, src_id: u32, dst_id: u32) -> Result<()>;

    /// Dump tree representation to a string where keys values are presented as hex strings.
    /// Intended to use for debugging purposes only.
    fn dump(&self) -> String;
}
