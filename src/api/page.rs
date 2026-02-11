/// High bit of `vlen` marks an overflow entry.
/// When set, the lower 31 bits encode the page multiplier M (number of contiguous
/// pages in the overflow region) and `page` holds the starting overflow page id.
pub const OVERFLOW_FLAG: u32 = 0x80000000;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Slot {
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
        Self::new(0, 0, 0, 0)
    }

    /// Returns true if this slot is an overflow entry (value stored in external pages).
    pub fn is_overflow(&self) -> bool {
        self.vlen & OVERFLOW_FLAG != 0
    }

    /// Returns the overflow region size multiplier (number of contiguous pages).
    /// Only meaningful when `is_overflow()` is true.
    pub fn multiplier(&self) -> u32 {
        self.vlen & !OVERFLOW_FLAG
    }

    /// Returns the number of inline value bytes stored in this page.
    /// For overflow entries this is 0 (value lives externally).
    pub fn inline_vlen(&self) -> u32 {
        if self.is_overflow() {
            0
        } else {
            self.vlen
        }
    }

    /// Returns true if this is a leaf entry (inline value or overflow value).
    /// Internal node entries have `page > 0` and are NOT overflow.
    pub fn is_leaf(&self) -> bool {
        self.page == 0 || self.is_overflow()
    }
}

pub trait Page: AsRef<[u8]> + AsMut<[u8]> {
    fn reserve(capacity: u32) -> Self;
    fn create(id: u32, cap: u32) -> Self;

    fn id(&self) -> u32;

    /// Current page's capacity in bytes.
    fn cap(&self) -> u32;

    /// Number of slots stored in the page.
    fn len(&self) -> u32;
    fn is_empty(&self) -> bool;

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

    /// Put a key with an overflow reference into the page.
    /// `overflow_page` is the starting page id of the contiguous overflow region.
    /// `raw_vlen` should include the OVERFLOW_FLAG and the multiplier.
    /// No inline value bytes are stored -- the full key+value lives in overflow.
    /// Returns slot index if operation was successful.
    fn put_overflow(&mut self, key: &[u8], overflow_page: u32, raw_vlen: u32) -> Option<u32>;

    /// Remove the slot of a given index.
    /// Automatic defragmentation is performed to maximize available capacity.
    fn remove(&mut self, idx: u32);

    /// Make an owned copy of all entries in the page: (key, val, page, raw_vlen).
    /// The 4th element carries the raw vlen field to distinguish overflow from internal nodes.
    fn copy(&self) -> Vec<(Vec<u8>, Vec<u8>, u32, u32)>;

    /// Fill whole page (but header) with zeroes.
    fn clear(&mut self);
}
