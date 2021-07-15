use crate::api::error::Result;
use crate::api::page::Page;
use std::cell::{Ref, RefMut};

// TODO consider wrapping result of lookup/insert/remove/page*/root* in api::error::Result<>?
pub(crate) trait Tree<P: Page> {
    fn lookup(&self, key: &[u8]) -> Option<Ref<[u8]>>;
    fn insert(&mut self, key: &[u8], val: &[u8]);
    fn remove(&mut self, key: &[u8]);

    fn root(&self) -> Ref<P>;
    fn page(&self, id: u32) -> Option<Ref<P>>;

    fn root_mut(&self) -> RefMut<P>;
    fn page_mut(&self, id: u32) -> Option<RefMut<P>>;

    fn flush(&mut self, page: &P) -> Result<()>;

    /// Reserve the provided page id - such id will never be returned by `next_id` until freed.
    fn next_id(&mut self) -> u32;

    /// Un-reserve the provided page id making it available for future via `next_id`.
    fn free_id(&mut self, id: u32);

    /// Split given page into two subpages containing ~equal number of entries.
    fn split(&mut self, page: &P) -> (u32, u32);

    /// Merge page `that` into page `this`, effectively removing page `that`.
    /// Return 'freed' id that previously identified `that` page.
    fn merge(&mut self, this: &mut P, that: &P) -> u32;
}
