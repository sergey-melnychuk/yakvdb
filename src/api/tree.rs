use crate::api::page::Page;

pub(crate) trait Tree {
    fn lookup(&self, key: &[u8]) -> Option<&[u8]>;
    fn insert(&mut self, key: &[u8], val: &[u8]);
    fn remove(&mut self, key: &[u8]);

    fn root(&self) -> &dyn Page;                // TODO wrap in api::error::Result<>?
    fn root_mut(&mut self) -> &mut dyn Page;    // TODO wrap in api::error::Result<>?

    fn page(&self, id: u32) -> Option<&dyn Page>;               // TODO wrap in api::error::Result<>?
    fn page_mut(&mut self, id: u32) -> Option<&mut dyn Page>;   // TODO wrap in api::error::Result<>?

    fn flush<P: Page>(&mut self, page: &P);      // TODO return api::error::Result<()>?

    /// Return next available page id.
    fn next_id(&mut self) -> u32;

    /// Reserve the provided page id - such id will never be returned by `next_id` until freed.
    fn take_id(&mut self, id: u32);

    /// Un-reserve the provided page id making it available for future via `next_id`.
    fn free_id(&mut self, id: u32);

    /// Split given page into two subpages containing ~equal number of entries.
    fn split<P: Page>(&mut self, page: &P) -> (u32, u32);

    /// Merge page `that` into page `this`, effectively removing page `that`.
    /// Return 'freed' id that previously identified `that` page.
    fn merge<P: Page>(&mut self, this: &mut P, that: &P) -> u32;
}
