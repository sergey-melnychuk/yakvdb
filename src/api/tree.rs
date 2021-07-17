use crate::api::error::Result;
use crate::api::page::Page;
use std::cell::{Ref, RefMut};

pub(crate) trait Tree<P: Page> {
    fn lookup(&self, key: &[u8]) -> Result<Option<Ref<[u8]>>>;
    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<()>;
    fn remove(&mut self, key: &[u8]) -> Result<()>;

    fn root(&self) -> Ref<P>;
    fn page(&self, id: u32) -> Option<Ref<P>>;

    fn root_mut(&self) -> RefMut<P>;
    fn page_mut(&self, id: u32) -> Option<RefMut<P>>;

    fn mark(&self, id: u32);
    fn flush(&self) -> Result<()>;

    /// Reserve the provided page id - such id will never be returned by `next_id` until freed.
    fn next_id(&self) -> Result<u32>;

    /// Un-reserve the provided page id making it available for future via `next_id`.
    fn free_id(&self, id: u32);

    /// Split given page into two subpages containing ~equal number of entries.
    fn split(&self, id: u32, parent_id: u32) -> Result<()>;

    /// Merge page `src_id` into page `dst_id`, effectively removing page `src_id`.
    fn merge(&self, src_id: u32, dst_id: u32) -> Result<()>;

    fn dump(&self) -> String;
}
