#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct Slot {
    pub(crate) offset: u32,
    pub(crate) klen: u32,
    pub(crate) vlen: u32, // if >0 value is stored in the same page as a key (leaf page)
    pub(crate) page: u32, // if >0 key holds a reference to another page (node page)
}

impl Slot {
    pub(crate) fn new(offset: u32, klen: u32, vlen: u32, page: u32) -> Self {
        Self {
            offset,
            klen,
            vlen,
            page,
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            offset: 0,
            klen: 0,
            vlen: 0,
            page: 0,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.offset == 0 && self.klen == 0 && self.vlen == 0 && self.page == 0
    }

    pub(crate) fn clear(&mut self) {
        self.offset = 0;
        self.klen = 0;
        self.vlen = 0;
        self.page = 0;
    }
}

pub(crate) trait Page {
    fn id(&self) -> u32;

    /// Current page's length in bytes.
    fn len(&self) -> u32;

    /// Number of entries (and slots) stored in the page.
    fn size(&self) -> u32;

    fn slot(&self, idx: u32) -> Option<Slot>;

    fn min(&self) -> &[u8];
    fn max(&self) -> &[u8];
    fn key(&self, idx: u32) -> &[u8];
    fn val(&self, idx: u32) -> &[u8];

    /// Get total number of unoccupied bytes in the page.
    /// Use `fits` to check if page really has enough free space to store a key-value pair.
    fn free(&self) -> u32;

    /// Get integer percent value (0..=100) of how full the page is.
    /// Effectively this is equal to `((len() - HEAD) - free()) * 100 / (len() - HEAD)`.
    fn full(&self) -> u8;

    /// Check if payload (key and value) of given size can fit the page,
    /// taking into account necessary housekeeping overhead.
    fn fits(&self, len: u32) -> bool;

    /// Find a slot with exact match to a given key (if any).
    fn find(&self, key: &[u8]) -> Option<u32>;

    /// Find a slot with the smallest key greater or equal to a given key.
    fn ceil(&self, key: &[u8]) -> Option<u32>;

    /// Put a key-value pair into the page.
    /// Returns slot index if operation was successful.
    fn put_val(&mut self, key: &[u8], val: &[u8]) -> Option<u32>;

    /// Put a key-page-reference pair into the page.
    /// Returns slot index if operation was successful.
    fn put_ref(&mut self, key: &[u8], page: u32) -> Option<u32>;

    /// Remove the slot of a given index and return key-value stored there.
    /// Automatic defragmentation is performed to maximize available capacity.
    fn remove(&mut self, idx: u32);

    /// Make an owned copy of all entries in the page: (key, val, page).
    fn copy(&self) -> Vec<(Vec<u8>, Vec<u8>, u32)>;
}
