use crate::api::Store;
use crate::api::error::{Error, Result};
use crate::api::page::{OVERFLOW_FLAG, Page};
use crate::api::tree::Tree;
use crate::api::{Durability, Options};
use crate::disk::wal::Commit;
use crate::disk::wal::{self, Flusher, Op, Record, Wal};
use crate::util::cache::{Cache, ClockCache};
use crate::util::hex::hex;
use bytes::{Buf, BufMut, BytesMut};
use log::{debug, error, trace};
use rustc_hash::FxHashSet;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use parking_lot::{
    MappedRwLockReadGuard, MappedRwLockWriteGuard, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
};

pub struct File<P: Page> {
    /// Underlying file reference where all data is physically stored.
    file: Arc<RwLock<fs::File>>,
    head: Head,

    /// In-memory page cache. All page access happens only through cached page representation.
    cache: Arc<RwLock<ClockCache<u32, P>>>,
    dirty: Arc<RwLock<FxHashSet<u32>>>,

    /// Min-heap of available page identifiers (this helps avoid "gaps": empty pages inside file).
    empty: Arc<RwLock<BinaryHeap<Reverse<u32>>>>,

    /// Serializes whole tree operations against each other.
    ///
    /// The locks above guard individual page accesses only, while a tree operation
    /// spans many of them: `split` snapshots a page, releases the lock to allocate a
    /// peer page, then re-acquires it and expects the snapshot to still describe the
    /// page. `insert` and `remove` take this lock exclusively, so no other operation
    /// ever observes the tree mid-rewrite.
    ///
    /// The read-only operations share it: the tree cannot change underneath them,
    /// and everything else they touch tolerates concurrency -- `page()` retries when
    /// a page is evicted out from under it, and page reads are positional, so they
    /// only need a shared lock on the file handle.
    ops: Arc<RwLock<()>>,

    /// The write-ahead log: every write is logged here before it returns, and
    /// modified pages reach the file only through `checkpoint`. See `doc/wal.md`.
    wal: Arc<Wal>,
    /// Fsyncs the log in the background, for `Durability::Periodic`.
    _flusher: Option<Flusher>,
    /// Pages of overflow regions freed since the last checkpoint. They go back
    /// to the free list only once a checkpoint has written a tree that no
    /// longer refers to them: until then the file on disk still does.
    pending_free: Mutex<Vec<u32>>,
    /// Pages in the file, the root and every page of every overflow region
    /// included: the next page allocated is `page_count + 1`.
    page_count: AtomicU32,
    /// The header's page count and the free-list trailer on disk describe the
    /// file as it is, so that the next open need not scan it.
    layout_saved: AtomicBool,
    /// Test hook: set when `open` had to scan the file for free pages.
    #[cfg(test)]
    scanned: AtomicBool,
    /// Pages were written straight to the file (new pages, overflow regions)
    /// since the last checkpoint, which has to make them durable first.
    file_dirty: AtomicBool,
    /// LSN of the last record the last checkpoint covered: with nothing
    /// logged since, nothing dirty and nothing freed, a checkpoint has nothing
    /// to do and is skipped.
    checkpointed_lsn: AtomicU64,
    /// Checkpoint when this many pages are dirty, or the log segment this big.
    max_dirty: usize,
    checkpoint_wal_bytes: u64,
    /// A write or a checkpoint failed half-way. The cache may hold part of a
    /// change the log does not, so nothing more may be written or checkpointed:
    /// reopening recovers the last committed state from the file and the log.
    poisoned: AtomicBool,
    /// Test hook: fail a checkpoint after writing this many pages in place
    /// (`usize::MAX`: never).
    #[cfg(test)]
    crash_after_pages: std::sync::atomic::AtomicUsize,
}

const MAGIC: &[u8] = b"YAKVDB42";

/// Upper limit on the page cache used by `make` and `open`: 256 MiB, i.e.
/// 65536 pages of 4 KiB.
///
/// The cache is the only thing standing between a page access and a `pread`,
/// and every operation touches a page per tree level, so the more of the tree
/// it holds the better. The limit is not allocated up front: a page takes
/// memory only once it is read or created, so the cache follows the size of
/// the tree -- a 50 MiB tree costs 50 MiB -- and stops growing here. Use
/// `Options::cache_bytes` to pick another limit.
pub const DEFAULT_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// Upper bound on the tree depth, to report a reference cycle in a corrupted
/// file instead of following it forever. A B-tree with a fan-out of at least 2
/// would need more than 2^64 entries to get this deep.
const MAX_DEPTH: usize = 64;

const HEAD: usize = MAGIC.len() + size_of::<Head>();
/// Where the header keeps the page count: after the magic and the page size.
const PAGE_COUNT_OFFSET: usize = MAGIC.len() + 4;

/// Starts the free-list trailer a checkpoint writes after the last page:
/// `[magic][count: u32][count page ids: u32][crc: u32]`, the CRC over all before it.
const FREE_MAGIC: &[u8] = b"YAKFREE1";
const ROOT: u32 = 1;

/// How many times `page`/`page_mut` reload a page that was evicted between
/// `cache()` putting it there and the borrow that hands it to the caller.
///
/// A page is the most-recently-used entry the moment it lands in the cache, so
/// losing it this many times in a row does not happen in practice. The bound is
/// there so that a pathological workload fails the operation instead of spinning
/// forever.
const CACHE_RETRIES: usize = 32;

// Percentage threshold for splitting (on insert) and merging (on delete) pages
const SPLIT_THRESHOLD: u8 = 80;
const MERGE_THRESHOLD: u8 = 20;

/// Magic value at offset 4 of an overflow region leader page.
/// Regular pages have `cap` (= page_bytes <= u16::MAX = 65535) at this offset,
/// so any value above 65535 is unambiguous.
const OVERFLOW_MAGIC: u32 = 0xF100DA7A;

/// Size of the overflow region header in bytes: [id, MAGIC, key_len, val_len].
const OVERFLOW_HEAD: usize = 16;

/// Size of a single slot descriptor in bytes (matches size_of::<Slot>()).
const SLOT_BYTES: usize = 16;

#[derive(Debug)]
#[repr(C)]
struct Head {
    page_bytes: u32,
    page_count: u32,
}

impl<P: Page> File<P> {
    /// Create a new database file, with default `Options`.
    pub fn make(path: &Path, page_bytes: u32) -> io::Result<Self> {
        Self::make_with(path, page_bytes, Options::default())
    }

    /// Open an existing database file, with default `Options`.
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_with(path, Options::default())
    }

    fn new(file: fs::File, head: Head, options: &Options, wal: Wal) -> Self {
        let page_count = head.page_count;
        let page_bytes = head.page_bytes as usize;
        let pages = (options.cache_bytes / page_bytes).max(1);
        // Dirty pages are pinned in the cache until a checkpoint: keep them to
        // half of it, so that reads always have room.
        let max_dirty = (options.checkpoint_dirty_bytes / page_bytes)
            .min(pages / 2)
            .max(1);
        let checkpointed_lsn = AtomicU64::new(wal.last_lsn());
        let wal = Arc::new(wal);
        let (write_every, sync_every) = match options.durability {
            Durability::Os => (options.write_buffer.interval, None),
            Durability::Periodic(every) => (options.write_buffer.interval, Some(every)),
            Durability::Sync => (None, None),
        };
        let flusher = (write_every.is_some() || sync_every.is_some())
            .then(|| Flusher::start(wal.clone(), write_every, sync_every));
        Self {
            file: Arc::new(RwLock::new(file)),
            head,
            cache: Arc::new(RwLock::new(ClockCache::new(pages))),
            dirty: Arc::new(RwLock::new(FxHashSet::default())),
            empty: Arc::new(RwLock::new(BinaryHeap::with_capacity(32))),
            ops: Arc::new(RwLock::new(())),
            wal,
            _flusher: flusher,
            pending_free: Mutex::new(Vec::new()),
            file_dirty: AtomicBool::new(false),
            page_count: AtomicU32::new(page_count),
            layout_saved: AtomicBool::new(true),
            #[cfg(test)]
            scanned: AtomicBool::new(false),
            checkpointed_lsn,
            max_dirty,
            checkpoint_wal_bytes: options.checkpoint_wal_bytes,
            poisoned: AtomicBool::new(false),
            #[cfg(test)]
            crash_after_pages: std::sync::atomic::AtomicUsize::new(usize::MAX),
        }
    }

    /// Create a new database file.
    pub fn make_with(path: &Path, page_bytes: u32, options: Options) -> io::Result<Self> {
        if path.exists() {
            return Err(io::Error::other(format!("File exists: {path:?}")));
        }
        // A log without its database belongs to one that was deleted, and
        // would otherwise be replayed onto this one the next time it opens.
        let wal_dir = wal_dir(path);
        if wal_dir.exists() {
            fs::remove_dir_all(&wal_dir)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(path)?;
        lock(&file, path)?;

        let head = Head {
            page_bytes,
            page_count: 1,
        };

        let mut buf = BytesMut::with_capacity(HEAD + page_bytes as usize);
        buf.put_slice(MAGIC);
        buf.put_u32(head.page_bytes);
        buf.put_u32(head.page_count);

        let root = P::create(ROOT, head.page_bytes);
        buf.put_slice(root.as_ref());

        file.write_all(buf.as_ref())?;
        file.sync_all()?;

        let wal = Wal::create(&wal_dir, 1, 1, commit_policy(&options))?;
        Ok(Self::new(file, head, &options, wal))
    }

    /// Open an existing database file, recovering it from its log if it was
    /// not closed cleanly.
    pub fn open_with(path: &Path, options: Options) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        // Before reading the log: a live handle elsewhere owns it.
        lock(&file, path)?;

        let len = file.metadata()?.len() as usize;
        if len < HEAD {
            return Err(io::Error::other("File too short"));
        }

        let mut buf = BytesMut::with_capacity(HEAD);
        buf.extend_from_slice(&[0u8; HEAD]);
        file.read_exact(&mut buf[..])?;

        let mut magic = [0u8; 8];
        buf.copy_to_slice(&mut magic);
        if magic != MAGIC {
            return Err(io::Error::other(format!("MAGIC mismatch: {magic:?}")));
        }

        let head = Head {
            page_bytes: buf.get_u32(),
            page_count: buf.get_u32(),
        };

        if head.page_bytes > u16::MAX as u32 {
            return Err(io::Error::other(format!(
                "Page size too large: {}",
                head.page_bytes
            )));
        }

        if len < HEAD + head.page_bytes as usize {
            return Err(io::Error::other(
                "File does not contain one full page".to_string(),
            ));
        }

        // Recovery, first half: put the pages of the last complete checkpoint
        // back in place, in case a crash interrupted it.
        let wal_dir = wal_dir(path);
        let segments = wal::segments(&wal_dir)?;
        let logs = segments
            .iter()
            .map(|(_, path)| fs::read(path))
            .collect::<io::Result<Vec<_>>>()?;
        let plan = wal::recovery_plan(&logs);
        for (id, bytes) in &plan.images {
            if *id == 0 || bytes.len() != head.page_bytes as usize {
                return Err(io::Error::other(format!(
                    "Log page image {id} has {} bytes",
                    bytes.len()
                )));
            }
            let offset = HEAD as u64 + (*id as u64 - 1) * head.page_bytes as u64;
            write_at(&file, offset, bytes)?;
        }
        if !plan.images.is_empty() {
            debug!("Recovery: restored {} page images", plan.images.len());
            file.sync_data()?;
        }
        let len = file.metadata()?.len() as usize;
        file.seek(SeekFrom::Start(HEAD as u64))?;

        let mut root = P::reserve(head.page_bytes);
        file.read_exact(root.as_mut())?;

        let next_segment = segments.last().map(|(id, _)| id + 1).unwrap_or(1);
        let wal = Wal::create(
            &wal_dir,
            next_segment,
            plan.last_lsn + 1,
            commit_policy(&options),
        )?;
        let this = Self::new(file, head, &options, wal);

        let _ = this.cache.write().put(ROOT, root, |_| false);

        // Which pages the file has and which are free: from the checkpoint
        // recovery just redid, or from the header and the free-list trailer a
        // checkpoint left, or else -- an older file, or a crash that made the
        // trailer stale -- by scanning every page.
        let layout = match plan.layout.clone() {
            Some(layout) => {
                // after redoing a checkpoint's images, the header and trailer
                // on disk may predate it
                if !plan.images.is_empty() {
                    this.layout_saved.store(false, Ordering::Relaxed);
                }
                Some(layout)
            }
            None => read_layout(&this.file.read(), &this.head, len)
                .map(|free| (this.head.page_count, free)),
        };
        if let Some((page_count, free)) = layout {
            debug!("Layout: {page_count} pages, {} free", free.len());
            this.page_count.store(page_count, Ordering::Relaxed);
            this.empty.write().extend(free.into_iter().map(Reverse));
        } else {
            let total_pages = (len - HEAD) as u32 / this.head.page_bytes;
            debug!("Scanning {total_pages} pages for free ones");
            this.page_count.store(total_pages, Ordering::Relaxed);
            this.layout_saved.store(false, Ordering::Relaxed);
            #[cfg(test)]
            this.scanned.store(true, Ordering::Relaxed);
            let mut id = 2u32;
            while id <= total_pages {
                // Check if this is an overflow leader by reading the 16-byte header
                let page_offset = this.offset(id);
                if let Ok((magic, key_len, val_len)) = this.read_overflow_header(page_offset)
                    && magic == OVERFLOW_MAGIC
                {
                    // This is an overflow region leader -- skip its continuation pages
                    let data_len = (key_len + val_len) as usize;
                    let m = Self::overflow_multiplier(this.head.page_bytes, data_len);
                    debug!("Page id={id} is overflow leader (multiplier={m})");
                    id += m;
                    continue;
                }

                // Regular page -- check if empty
                if let Ok(page) = this.load(this.offset(id), this.head.page_bytes) {
                    if page.len() == 0 {
                        debug!("Page id={id} is empty");
                        this.empty.write().push(Reverse(id));
                    }
                } else {
                    error!("Page failed to load: id={id}");
                }
                id += 1;
            }
        }

        // Recovery, second half: replay what was committed after that
        // checkpoint, then checkpoint it, which also clears the old segments
        // and saves the layout if it was not.
        if plan.ops.is_empty() && this.layout_saved.load(Ordering::Relaxed) {
            for (_, path) in &segments {
                fs::remove_file(path)?;
            }
            if !segments.is_empty() {
                wal::sync_dir(&wal_dir)?;
            }
        } else {
            debug!("Recovery: replaying {} records", plan.ops.len());
            let _op = this.ops.write();
            for ops in &plan.ops {
                for op in ops {
                    if let Err(e) = this.apply(op) {
                        // Otherwise dropping `this` would checkpoint the
                        // partial replay and delete the log still holding the rest.
                        this.poisoned.store(true, Ordering::Release);
                        return Err(io::Error::other(e));
                    }
                }
            }
            this.checkpoint().map_err(io::Error::other)?;
        }
        Ok(this)
    }

    /// Read exactly `buf.len()` bytes starting at `offset`.
    ///
    /// `seek` + `read_exact` needs `&mut File`, which would force every read to
    /// take the file lock exclusively and serialize readers against each other.
    /// A positional read needs only `&File`, so readers share the handle. Writes
    /// still seek, but they run under the exclusive `ops` lock anyway.
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read().read_exact_at(buf, offset)
        }
        #[cfg(not(unix))]
        {
            let mut file = self.file.write();
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(buf)
        }
    }

    fn load(&self, offset: usize, length: u32) -> io::Result<P> {
        let mut page = P::reserve(length);
        self.read_exact_at(offset as u64, page.as_mut())?;
        debug!("Loading page {}", page.id());
        Ok(page)
    }

    fn save(&self, page: &P) -> io::Result<()> {
        debug!("Saving page {}", page.id());
        self.write_all_at(self.offset(page.id()) as u64, page.as_ref())
    }

    /// Write all of `buf` starting at `offset`: one `pwrite` instead of `seek` + `write`.
    fn write_all_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        write_at(&self.file.read(), offset, buf)
    }

    fn offset(&self, id: u32) -> usize {
        HEAD + (id - 1) as usize * self.head.page_bytes as usize
    }

    /// `Tree::page`, reporting a missing page as an error instead of `None`.
    ///
    /// A page id the tree refers to but that cannot be read means the file is
    /// inconsistent -- from a crash mid-write, or a bug. That should fail the
    /// operation in progress and leave the rest of the process alone, which is
    /// what the callers of these two get by using `?` instead of `unwrap`.
    fn try_page(&self, id: u32) -> Result<MappedRwLockReadGuard<'_, P>> {
        self.page(id)
            .ok_or_else(|| Error::Tree(id, "Page not found".to_string()))
    }

    /// `Tree::page_mut`, reporting a missing page as an error instead of `None`.
    fn try_page_mut(&self, id: u32) -> Result<MappedRwLockWriteGuard<'_, P>> {
        self.page_mut(id)
            .ok_or_else(|| Error::Tree(id, "Page not found".to_string()))
    }

    pub fn page_size(&self) -> u32 {
        self.head.page_bytes
    }

    /// Pages in the file, the root and the pages of overflow regions included.
    pub fn page_count(&self) -> u32 {
        self.page_count.load(Ordering::Relaxed)
    }

    /// A copy of the page with the given `id`, or `None` if it cannot be read.
    ///
    /// Inspection tools -- the `yak` CLI -- need to look at raw pages. Handing
    /// out a page guard would let a caller hold a lock across arbitrary work,
    /// and reach page-level state without going through the operation lock at
    /// all, which is the reason `Tree` is not public. A copy costs one page of
    /// memory and keeps both problems inside the crate.
    pub fn read_page(&self, id: u32) -> Option<P> {
        let _op = self.ops.read();
        self.usable().ok()?;
        let page = self.page(id)?;
        let mut copy = P::reserve(self.head.page_bytes);
        copy.as_mut().copy_from_slice(page.as_ref());
        Some(copy)
    }

    /// A copy of the root page, or `None` if it cannot be read. See `read_page`.
    pub fn read_root(&self) -> Option<P> {
        self.read_page(ROOT)
    }

    /// A debugging dump of the whole tree, with keys and values as hex strings.
    ///
    /// Walks every page, so it is only useful on small trees.
    pub fn dump(&self) -> String {
        let _op = self.ops.read();
        Tree::dump(self)
    }

    /// Make every write so far durable, whatever the `Durability`: flush the
    /// log and checkpoint, so that the file holds everything and the log is empty.
    pub fn sync(&self) -> Result<()> {
        let _op = self.ops.write();
        self.usable()?;
        self.checkpoint()
    }

    // ---- Overflow I/O ----

    /// Compute the number of pages needed for an overflow region holding `data_len` bytes.
    fn overflow_multiplier(page_bytes: u32, data_len: usize) -> u32 {
        let total = OVERFLOW_HEAD + data_len;
        (total as u32).div_ceil(page_bytes)
    }

    /// Allocate `multiplier` contiguous pages at the end of the file.
    /// Returns the starting page id.
    fn alloc_overflow(&self, multiplier: u32) -> Result<u32> {
        // one id per page of the region: the next allocation starts after it
        let id = self.page_count.fetch_add(multiplier, Ordering::Relaxed) + 1;
        let region_bytes = multiplier as usize * self.head.page_bytes as usize;
        let zeroes = vec![0u8; region_bytes];
        self.write_all_at(self.offset(id) as u64, &zeroes)?;
        self.file_dirty.store(true, Ordering::Relaxed);
        debug!("alloc_overflow: id={id} multiplier={multiplier}");
        Ok(id)
    }

    /// Write overflow header + key + value data into a previously allocated region.
    fn write_overflow(
        &self,
        start_page: u32,
        key: &[u8],
        val: &[u8],
        multiplier: u32,
    ) -> Result<()> {
        let region_bytes = multiplier as usize * self.head.page_bytes as usize;
        let mut buf = BytesMut::with_capacity(region_bytes);
        // Header: [id: u32] [OVERFLOW_MAGIC: u32] [key_len: u32] [val_len: u32]
        buf.put_u32(start_page);
        buf.put_u32(OVERFLOW_MAGIC);
        buf.put_u32(key.len() as u32);
        buf.put_u32(val.len() as u32);
        buf.put_slice(key);
        buf.put_slice(val);
        // Pad remainder with zeroes
        buf.extend_from_slice(&vec![0u8; region_bytes - buf.len()]);

        self.write_all_at(self.offset(start_page) as u64, &buf)?;
        debug!(
            "write_overflow: page={start_page} multiplier={multiplier} key_len={} val_len={}",
            key.len(),
            val.len()
        );
        Ok(())
    }

    /// Read the full key and value from an overflow region in a single I/O.
    fn read_overflow(&self, start_page: u32, multiplier: u32) -> Result<(Vec<u8>, Vec<u8>)> {
        let region_bytes = multiplier as usize * self.head.page_bytes as usize;
        let mut buf = vec![0u8; region_bytes];
        let offset = self.offset(start_page) as u64;
        self.read_exact_at(offset, &mut buf)?;

        let _id = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        let magic = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        if magic != OVERFLOW_MAGIC {
            return Err(Error::Tree(
                start_page,
                format!("Overflow magic mismatch: expected {OVERFLOW_MAGIC:#X}, got {magic:#X}"),
            ));
        }
        let key_len = u32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize;
        let val_len = u32::from_be_bytes(buf[12..16].try_into().unwrap()) as usize;

        let key = buf[OVERFLOW_HEAD..OVERFLOW_HEAD + key_len].to_vec();
        let val = buf[OVERFLOW_HEAD + key_len..OVERFLOW_HEAD + key_len + val_len].to_vec();
        Ok((key, val))
    }

    /// Free all pages in an overflow region. They are wiped and go back to the
    /// free list at the next checkpoint: until then the tree on disk may still
    /// refer to them, and recovery would need them intact.
    fn free_overflow(&self, start_page: u32, multiplier: u32) -> Result<()> {
        debug!("free_overflow: page={start_page} multiplier={multiplier}");
        self.pending_free
            .lock()
            .extend(start_page..start_page + multiplier);
        Ok(())
    }

    /// Scan all overflow entries on a leaf page for one whose full key matches `key`.
    /// This handles the case where overflow entries store only a key prefix inline,
    /// causing `ceil()` to miss them (prefix < full_key).
    fn scan_overflow_entries(&self, page: &P, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let n = page.len();
        for i in 0..n {
            if let Some(slot) = page.slot(i)
                && slot.is_overflow()
            {
                let inline_key = page.key(i);
                if key.len() >= inline_key.len() && key.starts_with(inline_key) {
                    let (full_key, full_val) = self.read_overflow(slot.page, slot.multiplier())?;
                    if key == full_key.as_slice() {
                        return Ok(Some(full_val));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Find an overflow entry on a page whose full key matches `key`.
    /// Returns (slot_index, overflow_page, multiplier) if found.
    fn find_overflow_by_prefix(&self, page: &P, key: &[u8]) -> Result<Option<(u32, u32, u32)>> {
        let n = page.len();
        for i in 0..n {
            if let Some(slot) = page.slot(i)
                && slot.is_overflow()
            {
                let inline_key = page.key(i);
                if key.len() >= inline_key.len() && key.starts_with(inline_key) {
                    let (full_key, _) = self.read_overflow(slot.page, slot.multiplier())?;
                    if key == full_key.as_slice() {
                        return Ok(Some((i, slot.page, slot.multiplier())));
                    }
                }
            }
        }
        Ok(None)
    }

    /// For a copy() entry, return the full key. If the entry is overflow and the inline
    /// key is a prefix, read the full key from the overflow region.
    fn resolve_full_key(&self, key: &[u8], page_ref: u32, raw_vlen: u32) -> Result<Vec<u8>> {
        if raw_vlen & OVERFLOW_FLAG != 0 {
            let m = raw_vlen & !OVERFLOW_FLAG;
            let (full_key, _) = self.read_overflow(page_ref, m)?;
            Ok(full_key)
        } else {
            Ok(key.to_vec())
        }
    }

    /// Put a key-value pair into leaf page `id`, moving the value -- and, if
    /// even the key does not fit, all but a prefix of the key -- out to an
    /// overflow region. Returns how full the page is afterwards.
    fn put_leaf(&self, id: u32, key: &[u8], val: &[u8]) -> Result<u8> {
        let prefix_len = {
            // every way through here changes the page: take it mutably at once
            let mut page = self.try_page_mut(id)?;
            if page.fits((key.len() + val.len()) as u32) {
                page.put_val(key, val);
                return Ok(page.full());
            }
            if page.fits(key.len() as u32) {
                key.len()
            } else {
                let max_prefix = page.free().saturating_sub(SLOT_BYTES as u32) as usize;
                max_prefix.min(key.len())
            }
        };

        let m = Self::overflow_multiplier(self.head.page_bytes, key.len() + val.len());
        let ov_page = self.alloc_overflow(m)?;
        self.write_overflow(ov_page, key, val, m)?;
        let mut page = self.try_page_mut(id)?;
        page.put_overflow(&key[..prefix_len], ov_page, OVERFLOW_FLAG | m);
        Ok(page.full())
    }

    /// A copy of page `id`, to move entries from while writing to others:
    /// holding its guard meanwhile would deadlock on the cache.
    fn snapshot(&self, id: u32) -> Result<P> {
        let page = self.try_page(id)?;
        let mut copy = P::reserve(self.head.page_bytes);
        copy.as_mut().copy_from_slice(page.as_ref());
        Ok(copy)
    }

    /// Put the entries of `src` at `range` into page `id`.
    fn fill(&self, id: u32, src: &P, range: std::ops::Range<u32>) -> Result<()> {
        if !self.try_page_mut(id)?.extend_from(src, range) {
            return Err(Error::Tree(id, "Entries do not fit".to_string()));
        }
        Ok(())
    }

    /// The whole key at `idx` of `page`: for an overflow entry the inline one
    /// may be only a prefix.
    fn full_key_at(&self, page: &P, idx: u32) -> Result<Vec<u8>> {
        let slot = page
            .slot(idx)
            .ok_or_else(|| Error::Tree(page.id(), format!("Slot not found: {idx}")))?;
        self.resolve_full_key(page.key(idx), slot.page, slot.vlen)
    }

    /// The largest key in page `id`, or `None` if the page is empty.
    fn page_max(&self, id: u32) -> Result<Option<Vec<u8>>> {
        let page = self.try_page(id)?;
        Ok(if page.len() > 0 {
            Some(page.max().to_vec())
        } else {
            None
        })
    }

    /// Bring the parent's entry `idx` for child `page_id` in line with the
    /// child's largest key, `max`, or drop the entry if the child is empty.
    ///
    /// Checks through a read guard first, so a parent that is already right --
    /// the usual case -- is not marked dirty.
    fn update_parent_ref(
        &self,
        parent_id: u32,
        idx: u32,
        page_id: u32,
        max: Option<Vec<u8>>,
    ) -> Result<()> {
        let stale = match &max {
            Some(max) => max.as_slice() < self.try_page(parent_id)?.key(idx),
            None => true,
        };
        if stale {
            let mut parent = self.try_page_mut(parent_id)?;
            parent.remove(idx);
            match max {
                Some(max) => {
                    parent.put_ref(&max, page_id);
                }
                // nothing refers to the empty page any more: reuse it
                None => {
                    drop(parent);
                    self.free_id(page_id);
                }
            }
        }
        Ok(())
    }

    /// Read only the overflow header to extract key_len and val_len.
    fn read_overflow_header(&self, page_offset: usize) -> io::Result<(u32, u32, u32)> {
        let mut hdr = [0u8; OVERFLOW_HEAD];
        self.read_exact_at(page_offset as u64, &mut hdr)?;
        let magic = u32::from_be_bytes(hdr[4..8].try_into().unwrap());
        let key_len = u32::from_be_bytes(hdr[8..12].try_into().unwrap());
        let val_len = u32::from_be_bytes(hdr[12..16].try_into().unwrap());
        Ok((magic, key_len, val_len))
    }
}

impl<P: Page> File<P> {
    /// Apply `ops` and log them as one record under the operation lock, then
    /// wait for the commit outside it, so that concurrent writers can share an fsync.
    fn write(&self, ops: &[Op<'_>]) -> Result<()> {
        let lsn = {
            let _op = self.ops.write();
            self.usable()?;
            // Before applying rather than after: a checkpoint that fails then
            // fails a write that changed nothing, instead of one whose record
            // it has already made durable, and that recovery would bring back.
            self.maybe_checkpoint()?;
            let mut changed = false;
            for op in ops {
                match self.apply(op) {
                    Ok(applied) => changed |= applied,
                    Err(e) => {
                        // The cache may now hold part of the change. Keep it
                        // from ever reaching the file.
                        self.poisoned.store(true, Ordering::Release);
                        return Err(e);
                    }
                }
            }
            if !changed {
                return Ok(());
            }
            match ops {
                [op] => self.wal.append(&Record::Op(*op)),
                _ => self.wal.append(&Record::Batch(ops.to_vec())),
            }
        };
        if let Err(e) = self.wal.commit(lsn) {
            // The change is in the cache, its record maybe not in the file, and
            // the log takes nothing more: as unusable as after a failed apply.
            self.poisoned.store(true, Ordering::Release);
            return Err(e.into());
        }
        Ok(())
    }

    /// Apply one operation to the tree. Returns whether anything changed.
    fn apply(&self, op: &Op<'_>) -> Result<bool> {
        #[cfg(test)]
        tests::fail_point()?;
        match *op {
            Op::Insert(key, val) => self.insert_locked(key, val).map(|()| true),
            Op::Remove(key) => self.remove_locked(key),
        }
    }

    /// Fails once a write or checkpoint failed half-way: the cache may hold
    /// part of a change, which neither writes nor reads may build on.
    fn usable(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(Error::Other(
                "A write failed half-way: reopen the database to recover".to_string(),
            ));
        }
        Ok(())
    }

    fn maybe_checkpoint(&self) -> Result<()> {
        let dirty = self.dirty.read().len();
        if dirty >= self.max_dirty || self.wal.segment_bytes() >= self.checkpoint_wal_bytes {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Write every modified page to the file, crash-safely, and start a new
    /// log segment. The caller holds `ops` exclusively.
    ///
    /// The pages go into the log first, and only once that is durable are they
    /// written in place, so that a crash half-way through the in-place writes
    /// is repaired on open by writing the images again.
    fn checkpoint(&self) -> Result<()> {
        let result = self.checkpoint_inner();
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    fn checkpoint_inner(&self) -> Result<()> {
        let lsn = self.wal.last_lsn();
        let idle = lsn == self.checkpointed_lsn.load(Ordering::Relaxed)
            && self.dirty.read().is_empty()
            && self.pending_free.lock().is_empty()
            && !self.file_dirty.load(Ordering::Relaxed)
            && self.layout_saved.load(Ordering::Relaxed);
        if idle {
            return Ok(());
        }
        let mut ids = self.dirty.read().iter().copied().collect::<Vec<_>>();
        // ascending page ids make the writes as sequential as the tree allows
        ids.sort_unstable();
        let freed = std::mem::take(&mut *self.pending_free.lock());
        debug!(
            "checkpoint: lsn={lsn} pages={} freed={}",
            ids.len(),
            freed.len()
        );

        // New pages and overflow regions went straight to the file: they must
        // be durable before any image that refers to them is.
        if self.file_dirty.swap(false, Ordering::Relaxed) {
            self.file.read().sync_data()?;
        }

        for &id in &ids {
            let page = self.try_page(id)?;
            self.wal.append(&Record::Page {
                id,
                bytes: page.as_ref(),
            });
            drop(page);
            if self.wal.buffered_bytes() >= wal::CHUNK_BYTES {
                self.wal.flush_all(false)?;
            }
        }
        // The file once this checkpoint is done: its page count, and the free
        // pages, the ones freed since the last checkpoint included.
        let page_count = self.page_count.load(Ordering::Relaxed);
        let mut free = self
            .empty
            .read()
            .iter()
            .map(|Reverse(id)| *id)
            .chain(freed.iter().copied())
            .collect::<Vec<_>>();
        free.sort_unstable();
        self.wal.append(&Record::Checkpoint {
            lsn,
            pages: ids.len() as u32,
            page_count,
            free: free.clone(),
        });
        self.wal.flush_all(true)?;

        for &id in &ids {
            #[cfg(test)]
            self.crash_point()?;
            let page = self.try_page(id)?;
            self.save(&page)?;
        }
        // Nothing on disk refers to the freed pages any more: wipe them, so
        // that `open` finds them empty.
        for &id in &freed {
            self.save(&P::create(id, self.head.page_bytes))?;
        }
        // The header and the free-list trailer after the last page, so that
        // the next open needs no scan. Whatever lies past the trailer -- pages
        // of a crash's lost writes -- is beyond `page_count`: cut it off.
        {
            let file = self.file.read();
            write_at(&file, PAGE_COUNT_OFFSET as u64, &page_count.to_be_bytes())?;
            let end = HEAD as u64 + page_count as u64 * self.head.page_bytes as u64;
            let trailer = encode_trailer(&free);
            write_at(&file, end, &trailer)?;
            file.set_len(end + trailer.len() as u64)?;
        }
        self.file.read().sync_data()?;

        self.dirty.write().clear();
        for id in freed {
            self.free_id(id);
        }
        self.wal.rotate()?;
        // Open the new segment with the layout, as a checkpoint without page
        // images. The trailer tells a clean open the same, but the first page
        // allocated after this overwrites it: recovery reads it from here.
        self.wal.append(&Record::Checkpoint {
            lsn,
            pages: 0,
            page_count,
            free,
        });
        self.wal.flush_all(true)?;
        self.layout_saved.store(true, Ordering::Relaxed);
        self.checkpointed_lsn.store(lsn, Ordering::Relaxed);
        Ok(())
    }

    /// Counts down the pages `crash_after_pages` lets through, then fails.
    #[cfg(test)]
    fn crash_point(&self) -> Result<()> {
        match self.crash_after_pages.load(Ordering::Relaxed) {
            usize::MAX => Ok(()),
            0 => Err(Error::Other("Injected crash".to_string())),
            left => {
                self.crash_after_pages.store(left - 1, Ordering::Relaxed);
                Ok(())
            }
        }
    }

    /// `insert` without the locking, logging and commit: the caller holds
    /// `ops` exclusively.
    fn insert_locked(&self, key: &[u8], val: &[u8]) -> Result<()> {
        debug!("insert: {} -> {}", hex(key), hex(val));
        // Walk down through read guards: a page is taken mutably -- and so
        // marked dirty and written out by the next flush -- only once it is
        // certain to change. Most inserts change the leaf alone.
        let mut id = ROOT;
        // (page id, index of the entry followed, whether `key` is beyond the
        // page's largest key -- then that entry's key has to become `key`)
        let mut path: Vec<(u32, u32, bool)> = Vec::with_capacity(8);
        loop {
            if path.len() >= MAX_DEPTH {
                return Err(Error::Tree(
                    id,
                    format!("Cyclic reference detected: {path:?}"),
                ));
            }
            let parent_id = path.last().map(|(id, _, _)| *id).unwrap_or_default();

            let (idx, slot, beyond) = {
                let page = self.try_page(id)?;
                if page.len() == 0 {
                    drop(page);
                    self.put_leaf(id, key, val)?;
                    return Ok(());
                }
                match page.ceil(key) {
                    Some(idx) => (idx, page.slot(idx), false),
                    None => (page.len() - 1, page.slot(page.len() - 1), true),
                }
            };

            // `ceil` found nothing at or above `key` in the parent exactly when
            // `key` is beyond the parent's entry for this page: no need to read
            // the parent again to find out.
            if let Some(&(parent_id, parent_idx, true)) = path.last() {
                let mut parent_page = self.try_page_mut(parent_id)?;
                parent_page.remove(parent_idx);
                parent_page.put_ref(key, id);
            }

            let slot = slot.ok_or_else(|| Error::Tree(id, format!("Slot not found: {idx}")))?;

            if slot.is_leaf() {
                // If replacing an existing overflow entry, free the old overflow region
                if slot.is_overflow() && self.try_page(id)?.key(idx) == key {
                    self.try_page_mut(id)?.remove(idx);
                    self.free_overflow(slot.page, slot.multiplier())?;
                }

                let full = self.put_leaf(id, key, val)?;
                if full > SPLIT_THRESHOLD {
                    self.split(id, parent_id)?;
                }

                while let Some((page_id, _, _)) = path.pop() {
                    let parent_id = path.last().map(|(id, _, _)| *id).unwrap_or_default();
                    let full = {
                        let page = self.try_page(page_id)?;
                        page.full()
                    };
                    if full > SPLIT_THRESHOLD {
                        self.split(page_id, parent_id)?;
                    }
                }

                return Ok(());
            }

            path.push((id, idx, beyond));
            id = slot.page;
        }
    }

    /// `remove` without the locking, logging and commit: the caller holds
    /// `ops` exclusively. Returns whether the key was there.
    fn remove_locked(&self, key: &[u8]) -> Result<bool> {
        debug!("remove: {}", hex(key));
        // Read guards on the way down, as in `insert`: a key that is not there
        // leaves every page clean.
        let mut id = ROOT;
        let mut path: Vec<(u32, u32)> = Vec::with_capacity(8);
        loop {
            if path.len() >= MAX_DEPTH {
                return Err(Error::Tree(id, "Cyclic reference detected".to_string()));
            }
            let page = self.try_page(id)?;
            let idx_opt = page.ceil(key);

            // Handle overflow prefix fallback: scan overflow entries if ceil misses
            if idx_opt.is_none() {
                let found = self.find_overflow_by_prefix(&page, key)?;
                drop(page);
                if let Some((idx, ov_page, m)) = found {
                    self.try_page_mut(id)?.remove(idx);
                    self.free_overflow(ov_page, m)?;

                    // Navigate up-tree
                    let mut page_id = id;
                    for (parent_id, idx) in path.iter().cloned().rev() {
                        let max_opt = self.page_max(page_id)?;
                        self.update_parent_ref(parent_id, idx, page_id, max_opt)?;
                        page_id = parent_id;
                    }
                    return Ok(true);
                }
                return Ok(false);
            }
            let idx = idx_opt.unwrap();

            let slot = page
                .slot(idx)
                .ok_or_else(|| Error::Tree(id, format!("Slot not found: {idx}")))?;

            if !slot.is_leaf() {
                drop(page);
                path.push((id, idx));
                id = slot.page;
                continue;
            }

            // Find the entry to remove, and the overflow region it owns, if any.
            let target = if slot.is_overflow() {
                let (full_key, _) = self.read_overflow(slot.page, slot.multiplier())?;
                if full_key.as_slice() == key {
                    Some((idx, Some((slot.page, slot.multiplier()))))
                } else {
                    // Ceil'd entry didn't match -- try prefix fallback
                    self.find_overflow_by_prefix(&page, key)?
                        .map(|(idx, ov_page, m)| (idx, Some((ov_page, m))))
                }
            } else if page.key(idx) == key {
                Some((idx, None))
            } else {
                // Regular entry didn't match -- check overflow entries on this page
                self.find_overflow_by_prefix(&page, key)?
                    .map(|(idx, ov_page, m)| (idx, Some((ov_page, m))))
            };
            drop(page);

            let (idx, overflow) = match target {
                Some(target) => target,
                None => return Ok(false), // not found
            };
            debug!("remove: key={} page={} idx={}", hex(key), id, idx);
            self.try_page_mut(id)?.remove(idx);
            if let Some((ov_page, m)) = overflow {
                self.free_overflow(ov_page, m)?;
            }

            // Navigate up-tree and remove/update references if needed
            let mut page_id = id;
            for (parent_id, mut idx) in path.iter().cloned().rev() {
                let full = self.try_page(page_id)?.full();
                if full < MERGE_THRESHOLD {
                    let peer_id = {
                        let parent = self.try_page(parent_id)?;
                        let mut peers = Vec::with_capacity(2);
                        if idx > 0 {
                            let peer = parent.slot(idx - 1).unwrap().page;
                            peers.push(peer);
                        }
                        if idx < parent.len() - 1 {
                            let peer = parent.slot(idx + 1).unwrap().page;
                            peers.push(peer);
                        }
                        drop(parent);

                        // A `for` loop rather than `filter_map`, so that a
                        // peer that cannot be read fails the operation
                        // instead of quietly dropping out of the candidates.
                        let mut candidates = Vec::with_capacity(peers.len());
                        for peer_id in peers {
                            let peer = self.try_page(peer_id)?;
                            let full = peer.full();
                            if peer.len() > 0 && full < MERGE_THRESHOLD {
                                candidates.push((peer_id, full));
                            }
                        }
                        candidates
                            .into_iter()
                            .min_by_key(|(_, full)| *full)
                            .map(|(peer_id, _)| peer_id)
                    };
                    if let Some(peer_id) = peer_id {
                        trace!(
                            "merge: found peer_id={peer_id} to merge page_id={page_id} (parent_id={parent_id})"
                        );
                        let peer_max = {
                            let peer = self.try_page(peer_id)?;
                            peer.max().to_vec()
                        };
                        trace!("\t merge: peer_max={}", hex(&peer_max));
                        let mut parent = self.try_page_mut(parent_id)?;
                        parent.remove(idx);
                        let peer_idx = parent.ceil(&peer_max).unwrap();
                        trace!("\t merge: parent remove: peer_idx={peer_idx} idx={idx}");
                        parent.remove(peer_idx);
                        drop(parent);

                        self.merge(page_id, peer_id)?;
                        let page_max = {
                            let peer = self.try_page(peer_id)?;
                            peer.max().to_vec()
                        };
                        trace!("\t merge: page_max={}", hex(&page_max));
                        let mut parent = self.try_page_mut(parent_id)?;
                        trace!(
                            "\t merge: parent insert: page_max={}, peer_id={}",
                            hex(&page_max),
                            peer_id
                        );
                        parent.put_ref(&page_max, peer_id);
                        idx = parent.ceil(&page_max).unwrap();
                        page_id = peer_id;
                    }
                }

                let max_opt = self.page_max(page_id)?;
                self.update_parent_ref(parent_id, idx, page_id, max_opt)?;
                page_id = parent_id;
            }

            return Ok(true);
        }
    }
}

impl<P: Page> Store for File<P> {
    fn lookup(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let _op = self.ops.read();
        self.usable()?;
        debug!("lookup: {}", hex(key));
        let mut page = self.root()?;
        for _ in 0..MAX_DEPTH {
            let idx_opt = page.ceil(key);

            // When ceil returns None, the search key is greater than all inline keys.
            // But an overflow entry with a prefix key (shorter than full key) might still
            // match -- scan overflow entries as a fallback.
            if idx_opt.is_none() {
                return self.scan_overflow_entries(&page, key);
            }
            let idx = idx_opt.unwrap();

            let slot_opt = page.slot(idx);
            if slot_opt.is_none() {
                return Err(Error::Tree(page.id(), format!("Slot not found: {idx}")));
            }
            let slot = slot_opt.unwrap();

            if slot.is_leaf() {
                if slot.is_overflow() {
                    // Overflow entry: read full key+value from overflow region
                    let (full_key, full_val) = self.read_overflow(slot.page, slot.multiplier())?;
                    if key == full_key.as_slice() {
                        return Ok(Some(full_val));
                    }
                    // The ceil'd entry didn't match -- scan other overflow entries
                    return self.scan_overflow_entries(&page, key);
                } else {
                    return if key == page.key(idx) {
                        Ok(Some(page.val(idx).to_vec()))
                    } else {
                        // Also check overflow entries at lower indices
                        self.scan_overflow_entries(&page, key)
                    };
                }
            } else {
                let id = page.id();
                drop(page);
                let page_opt = self.page(slot.page);
                if page_opt.is_none() {
                    return Err(Error::Tree(id, format!("Page not found: {}", slot.page)));
                }
                page = page_opt.unwrap();
            }
        }
        Err(Error::Tree(
            page.id(),
            "Cyclic reference detected".to_string(),
        ))
    }

    fn insert(&self, key: &[u8], val: &[u8]) -> Result<()> {
        self.write(&[Op::Insert(key, val)])
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
        self.write(&[Op::Remove(key)])
    }

    fn write_batch(&self, ops: &[Op<'_>]) -> Result<()> {
        self.write(ops)
    }

    fn is_empty(&self) -> bool {
        let _op = self.ops.read();
        // A root that cannot be read is not an empty tree: report the file as
        // non-empty so a caller does not mistake a broken one for a fresh one.
        // The same goes for one poisoned by a failed write.
        if self.usable().is_err() {
            return false;
        }
        self.root().map(|page| page.len() == 0).unwrap_or(false)
    }

    fn min(&self) -> Result<Option<Vec<u8>>> {
        let _op = self.ops.read();
        self.usable()?;
        let mut page = self.root()?;
        if page.len() == 0 {
            return Ok(None);
        }
        loop {
            let slot = page.slot(0).unwrap();
            if slot.is_leaf() {
                if slot.is_overflow() {
                    let (full_key, _) = self.read_overflow(slot.page, slot.multiplier())?;
                    return Ok(Some(full_key));
                }
                return Ok(Some(page.min().to_vec()));
            } else {
                let id = slot.page;
                drop(page);
                page = self.try_page(id)?;
            }
        }
    }

    fn max(&self) -> Result<Option<Vec<u8>>> {
        let _op = self.ops.read();
        self.usable()?;
        let mut page = self.root()?;
        if page.len() == 0 {
            return Ok(None);
        }
        loop {
            let last = page.len() - 1;
            let slot = page.slot(last).unwrap();
            if slot.is_leaf() {
                if slot.is_overflow() {
                    let (full_key, _) = self.read_overflow(slot.page, slot.multiplier())?;
                    return Ok(Some(full_key));
                }
                return Ok(Some(page.max().to_vec()));
            } else {
                let id = slot.page;
                drop(page);
                page = self.try_page(id)?;
            }
        }
    }

    fn above(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let _op = self.ops.read();
        self.usable()?;
        debug!("above: {}", hex(key));

        let mut path = Vec::with_capacity(8);
        let mut page = self.root()?;
        if page.len() == 0 {
            return Ok(None);
        }
        if page.max() < key {
            return Ok(None);
        }
        loop {
            let idx = page.ceil(key).unwrap();
            let slot = page.slot(idx).unwrap();
            if slot.is_leaf() {
                // Helper: resolve the full key for a slot (handles overflow)
                let resolve_key = |p: &P, i: u32| -> Result<Vec<u8>> {
                    let s = p.slot(i).unwrap();
                    if s.is_overflow() {
                        let (fk, _) = self.read_overflow(s.page, s.multiplier())?;
                        Ok(fk)
                    } else {
                        Ok(p.key(i).to_vec())
                    }
                };

                let current_key = resolve_key(&page, idx)?;
                if key < current_key.as_slice() {
                    return Ok(Some(current_key));
                } else if key == current_key.as_slice() && idx < page.len() - 1 {
                    let next_key = resolve_key(&page, idx + 1)?;
                    return Ok(Some(next_key));
                } else {
                    // ceil == key, need to take min value from parent's next adjacent subtree
                    for (parent_id, parent_idx) in path.iter().rev().cloned() {
                        // `page` holds a read guard on the cache and `page()` may
                        // need the write guard: releasing it first is what keeps
                        // this from deadlocking against itself.
                        drop(page);
                        page = self.try_page(parent_id)?;
                        if parent_idx < page.len() - 1 {
                            let id = page.slot(parent_idx + 1).unwrap().page;
                            drop(page);
                            page = self.try_page(id)?;
                            loop {
                                let slot = page.slot(0).unwrap();
                                if slot.is_leaf() {
                                    if slot.is_overflow() {
                                        let (fk, _) =
                                            self.read_overflow(slot.page, slot.multiplier())?;
                                        return Ok(Some(fk));
                                    }
                                    return Ok(Some(page.min().to_vec()));
                                } else {
                                    drop(page);
                                    page = self.try_page(slot.page)?;
                                }
                            }
                        }
                    }

                    // the key appears to be the maximum value stored in the tree
                    return Ok(None);
                }
            } else {
                path.push((page.id(), idx));
                let id = slot.page;
                drop(page);
                page = self.try_page(id)?;
            }
        }
    }

    fn below(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let _op = self.ops.read();
        self.usable()?;
        debug!("below: {}", hex(key));

        let mut path = Vec::with_capacity(8);
        let mut page = self.root()?;
        if page.len() == 0 {
            return Ok(None);
        }
        if page.max() < key {
            return Ok(Some(page.max().to_vec()));
        }
        loop {
            let idx = page.ceil(key).unwrap();
            let slot = page.slot(idx).unwrap();
            if slot.is_leaf() {
                if idx > 0 {
                    let prev_slot = page.slot(idx - 1).unwrap();
                    let prev_key = if prev_slot.is_overflow() {
                        let (fk, _) = self.read_overflow(prev_slot.page, prev_slot.multiplier())?;
                        fk
                    } else {
                        page.key(idx - 1).to_vec()
                    };
                    if key.to_vec() > prev_key {
                        return Ok(Some(prev_key));
                    }
                }

                // ceil == key or first entry, need to take max from parent's previous subtree
                for (parent_id, parent_idx) in path.iter().rev().cloned() {
                    drop(page);
                    page = self.try_page(parent_id)?;
                    if parent_idx > 0 {
                        let pidx = parent_idx - 1;
                        let id = page.slot(pidx).unwrap().page;
                        drop(page);
                        page = self.try_page(id)?;
                        // Get max key from this subtree leaf
                        let last = page.len() - 1;
                        let last_slot = page.slot(last).unwrap();
                        if last_slot.is_overflow() {
                            let (fk, _) =
                                self.read_overflow(last_slot.page, last_slot.multiplier())?;
                            return Ok(Some(fk));
                        }
                        return Ok(Some(page.max().to_vec()));
                    }
                }

                // the key seems to be the minimum value stored in the tree
                return Ok(None);
            } else {
                path.push((page.id(), idx));
                let id = slot.page;
                drop(page);
                page = self.try_page(id)?;
            }
        }
    }
}

impl<P: Page> Tree<P> for File<P> {
    fn root(&self) -> Result<MappedRwLockReadGuard<'_, P>> {
        self.try_page(ROOT)
    }

    fn page(&self, id: u32) -> Option<MappedRwLockReadGuard<'_, P>> {
        for _ in 0..CACHE_RETRIES {
            // Borrow first and load only on a miss: a hit, the common case, then
            // costs a single shared lock acquisition instead of two.
            //
            // On failure `try_map` hands the guard back: it has to be dropped
            // before `cache()`, which takes the write guard, or this deadlocks
            // against itself. The page is either not loaded yet or was evicted
            // between `cache()` putting it there and the borrow, so load and retry.
            match RwLockReadGuard::try_map(self.cache.read(), |cache| cache.get(&id)) {
                Ok(page) => return Some(page),
                Err(guard) => drop(guard),
            }
            self.cache(id).ok()?;
        }
        error!("Page {id} evicted {CACHE_RETRIES} times while being read");
        None
    }

    fn page_mut(&self, id: u32) -> Option<MappedRwLockWriteGuard<'_, P>> {
        self.cache(id).ok()?;
        self.mark(id);
        for _ in 0..CACHE_RETRIES {
            match RwLockWriteGuard::try_map(self.cache.write(), |cache| cache.get_mut(&id)) {
                Ok(page) => return Some(page),
                Err(guard) => drop(guard),
            }
            self.cache(id).ok()?;
        }
        error!("Page {id} evicted {CACHE_RETRIES} times while being written");
        None
    }

    fn cache(&self, id: u32) -> io::Result<()> {
        let has_id = self.cache.read().has(&id);
        if !has_id {
            let page = self.load(self.offset(id), self.head.page_bytes)?;
            // Dirty pages are pinned: one written in place before a checkpoint
            // could leave the file half-updated after a crash, beyond what
            // the log can repair.
            let evicted = {
                let dirty = self.dirty.read();
                self.cache.write().put(id, page, |id| dirty.contains(id))
            };
            if let Some((evicted_id, evicted_page)) = evicted
                && self.dirty.write().remove(&evicted_id)
            {
                // Cannot happen with the pin above; losing the change would be worse.
                error!("Dirty page {evicted_id} evicted: writing it back");
                self.save(&evicted_page)?;
            }
        }
        Ok(())
    }

    fn mark(&self, id: u32) {
        self.dirty.write().insert(id);
    }

    fn next_id(&self) -> Result<u32> {
        let is_empty = self.empty.read().is_empty();
        if !is_empty {
            let id = self.empty.write().pop().unwrap().0;
            let temp = P::create(id, self.head.page_bytes);
            let mut page = self.try_page_mut(id)?;
            page.as_mut().copy_from_slice(temp.as_ref());
            return Ok(id);
        }

        let id = self.page_count.fetch_add(1, Ordering::Relaxed) + 1;
        let page = P::create(id, self.head.page_bytes);
        self.write_all_at(self.offset(id) as u64, page.as_ref())?;
        self.file_dirty.store(true, Ordering::Relaxed);

        Ok(id)
    }

    fn free_id(&self, id: u32) {
        self.empty.write().push(Reverse(id))
    }

    fn split(&self, id: u32, parent_id: u32) -> Result<()> {
        // One page-sized copy to move entries from, instead of an owned key
        // and value per entry.
        let snap = self.snapshot(id)?;
        let len = snap.len();
        let half = len / 2;

        if id == ROOT {
            let lo_id = self.next_id()?;
            let hi_id = self.next_id()?;
            debug!("split: root={id} into lo={lo_id} and hi={hi_id} (parent={parent_id})");

            let lo_max = self.full_key_at(&snap, half - 1)?;
            let hi_max = self.full_key_at(&snap, len - 1)?;
            self.fill(lo_id, &snap, 0..half)?;
            self.fill(hi_id, &snap, half..len)?;

            let mut page = self.try_page_mut(id)?;
            page.clear();
            page.put_ref(&lo_max, lo_id);
            page.put_ref(&hi_max, hi_id);
            Ok(())
        } else {
            let peer_id = self.next_id()?;
            debug!("split: page={id} into peer={peer_id} (parent={parent_id})");

            // the page keeps the lower half, the peer takes the upper one
            let max = self.full_key_at(&snap, len - 1)?;
            let page_max = self.full_key_at(&snap, half - 1)?;
            let peer_max = max.clone();
            self.try_page_mut(id)?.truncate(half);
            self.fill(peer_id, &snap, half..len)?;

            {
                let mut parent = self.try_page_mut(parent_id)?;
                let idx = parent.find(&max).ok_or_else(|| {
                    Error::Tree(
                        parent_id,
                        format!("Child entry not found in parent: {}", hex(&max)),
                    )
                })?;
                parent.remove(idx);
                parent.put_ref(&page_max, id);
                parent.put_ref(&peer_max, peer_id);
            }

            self.check(parent_id, id)?;
            self.check(parent_id, peer_id)?;

            Ok(())
        }
    }

    fn check(&self, parent_id: u32, page_id: u32) -> Result<()> {
        let page_max = {
            let page = self.try_page(page_id)?;
            page.max().to_vec()
        };

        let (parent_key, parent_ref) = {
            let parent = self.try_page(parent_id)?;
            let page_idx = parent.find(&page_max).ok_or_else(|| {
                Error::Tree(
                    parent_id,
                    format!("Child entry not found in parent: {}", hex(&page_max)),
                )
            })?;
            let parent_key = parent.key(page_idx).to_vec();
            let parent_ref = parent
                .slot(page_idx)
                .map(|slot| slot.page)
                .unwrap_or_default();
            (parent_key, parent_ref)
        };

        if parent_key != page_max {
            log::error!(
                "parent_key != page_max: parent_key={} page_max={}",
                hex(&parent_key),
                hex(&page_max)
            );
            return Err(Error::Tree(
                parent_id,
                "Parent entry key does not match child page".to_string(),
            ));
        }

        if parent_ref != page_id {
            log::error!("parent_ref != page_id: parent_ref={parent_ref} page_id={page_id}",);
            return Err(Error::Tree(
                parent_id,
                "Parent entry ref does not match child page".to_string(),
            ));
        }

        Ok(())
    }

    fn merge(&self, src_id: u32, dst_id: u32) -> Result<()> {
        debug!("merge: src={src_id} into dst={dst_id}");
        let src = self.snapshot(src_id)?;
        self.fill(dst_id, &src, 0..src.len())?;
        self.try_page_mut(src_id)?.clear();

        self.free_id(src_id);
        Ok(())
    }

    fn dump(&self) -> String {
        fn dump_page<P: Page>(
            file: &File<P>,
            page_id: u32,
            parent_id: u32,
            acc: &mut String,
            prefix: String,
            tab: String,
        ) {
            if page_id == 0 {
                return;
            }

            let page = file.page(page_id).unwrap();
            let copy = page.copy();
            let full = page.full();
            // Loading a child may need the cache's write lock: holding this
            // read guard across the recursion would deadlock on a cache miss.
            drop(page);

            acc.push_str(&if copy.is_empty() {
                format!("{prefix}page={page_id}: empty")
            } else {
                let entries = copy
                    .iter()
                    .map(|(k, v, p, raw_vlen)| {
                        if *raw_vlen & OVERFLOW_FLAG != 0 {
                            format!(
                                "{}{}, [OVERFLOW M={} PAGE={}], {}",
                                prefix,
                                hex(k),
                                raw_vlen & !OVERFLOW_FLAG,
                                p,
                                p
                            )
                        } else {
                            format!("{}{}, {}, {}", prefix, hex(k), hex(v), p)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{prefix}page={page_id}: (parent={parent_id}) {full}% full\n{entries}")
            });

            acc.push('\n');
            // Only follow child page links for internal nodes (not overflow refs)
            let links = copy
                .iter()
                .filter(|(_, _, _, raw_vlen)| *raw_vlen & OVERFLOW_FLAG == 0)
                .map(|(_, _, p, _)| p)
                .cloned();
            links.for_each(|id| {
                let mut p = prefix.clone();
                p.push_str(&tab);
                dump_page(file, id, page_id, acc, p, tab.clone());
            });
        }

        let mut acc = String::with_capacity(1024);
        dump_page(self, ROOT, 0, &mut acc, "".to_string(), "\t".to_string());
        acc
    }
}

impl<P: Page> Drop for File<P> {
    /// Checkpoint on close, leaving the log empty and the next open quick.
    /// Skipped after a failed write: then the next open recovers instead,
    /// from a log that still gets the records waiting in memory, since those
    /// are all of writes that succeeded.
    fn drop(&mut self) {
        if self.poisoned.load(Ordering::Acquire) {
            if let Err(e) = self.wal.flush_all(false) {
                error!("Writing the log on close failed: {e}");
            }
            return;
        }
        let _op = self.ops.write();
        if let Err(e) = self.checkpoint() {
            error!("Checkpoint on close failed: {e}");
        }
    }
}

/// Take an exclusive lock on the database file, released when it is closed.
///
/// Opening replays, checkpoints and rotates the log: done to a database that
/// is open elsewhere, it would delete the log out from under that handle.
fn lock(file: &fs::File, path: &Path) -> io::Result<()> {
    file.try_lock().map_err(|e| match e {
        fs::TryLockError::WouldBlock => io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("Database is open elsewhere: {path:?}"),
        ),
        fs::TryLockError::Error(e) => e,
    })
}

fn encode_trailer(free: &[u32]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(FREE_MAGIC.len() + 8 + free.len() * 4);
    buf.extend_from_slice(FREE_MAGIC);
    buf.extend_from_slice(&(free.len() as u32).to_be_bytes());
    for id in free {
        buf.extend_from_slice(&id.to_be_bytes());
    }
    let crc = wal::crc32c(&buf);
    buf.extend_from_slice(&crc.to_be_bytes());
    buf
}

/// The free pages recorded by the last checkpoint, if the file is exactly as
/// it left it: `page_count` pages, then an intact trailer -- or no trailer at
/// all and no free pages, as `make` leaves a file. Anything else, and the
/// caller scans.
fn read_layout(file: &fs::File, head: &Head, len: usize) -> Option<Vec<u32>> {
    let end = HEAD + head.page_count as usize * head.page_bytes as usize;
    if len == end {
        return Some(vec![]);
    }
    let mut buf = vec![0u8; len.checked_sub(end)?];
    if buf.len() < FREE_MAGIC.len() + 8 {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(&mut buf, end as u64).ok()?;
    }
    #[cfg(not(unix))]
    {
        let mut file = file;
        file.seek(SeekFrom::Start(end as u64)).ok()?;
        file.read_exact(&mut buf).ok()?;
    }
    let (body, crc) = buf.split_at(buf.len() - 4);
    if !body.starts_with(FREE_MAGIC) || wal::crc32c(body).to_be_bytes() != crc {
        return None;
    }
    let body = &body[FREE_MAGIC.len()..];
    let count = u32::from_be_bytes(body[..4].try_into().ok()?) as usize;
    let ids = &body[4..];
    if ids.len() != count * 4 {
        return None;
    }
    let free = ids
        .as_chunks::<4>()
        .0
        .iter()
        .map(|id| u32::from_be_bytes(*id))
        .collect::<Vec<_>>();
    let valid = free.iter().all(|&id| id > 1 && id <= head.page_count);
    valid.then_some(free)
}

/// What a commit waits for, from the options.
fn commit_policy(options: &Options) -> Commit {
    match options.durability {
        Durability::Sync => Commit::SYNC,
        Durability::Os | Durability::Periodic(_) => Commit {
            sync: false,
            records: options.write_buffer.records as u64,
            bytes: options.write_buffer.bytes,
        },
    }
}

/// The log of the database at `path`: the directory `<path>.wal`.
fn wal_dir(path: &Path) -> PathBuf {
    let mut dir = path.as_os_str().to_owned();
    dir.push(".wal");
    PathBuf::from(dir)
}

/// Write all of `buf` at `offset` without moving the file position (on unix).
fn write_at(file: &fs::File, offset: u64, buf: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(buf, offset)
    }
    #[cfg(not(unix))]
    {
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::WriteBuffer;
    use crate::disk::block::Block;
    use crate::util;
    use crate::util::hex::hex;
    use rand::SeedableRng;
    use rand::prelude::StdRng;
    use rand::seq::SliceRandom;
    use std::ops::Deref;

    fn init_log() {
        let _ = env_logger::try_init();
    }

    fn get<P: Page>(page: &P, key: &[u8]) -> Option<(Vec<u8>, u32)> {
        page.find(key)
            .map(|idx| (page.val(idx).to_vec(), page.slot(idx).unwrap().page))
    }

    #[test]
    fn test_page() {
        let path = Path::new("target/test_page.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let size: u32 = 256;

        let data = vec![
            (b"aaa".to_vec(), b"zxczxczxc".to_vec(), 0u32, 9u32),
            (b"bbb".to_vec(), b"asdasdasd".to_vec(), 0, 9),
            (b"ccc".to_vec(), b"qweqweqwe".to_vec(), 0, 9),
            (b"ddd".to_vec(), b"123123123".to_vec(), 0, 9),
            (b"xxx".to_vec(), vec![], 3333, 0),
            (b"yyy".to_vec(), vec![], 2222, 0),
            (b"zzz".to_vec(), vec![], 1111, 0),
        ];

        {
            let file: File<Block> = File::make(path, size).unwrap();
            {
                let mut page = file.page_mut(ROOT).unwrap();
                for (k, v, p, _) in data.iter() {
                    if *p == 0 {
                        page.put_val(k, v);
                    } else {
                        page.put_ref(k, *p);
                    }
                }
            };
            let page = file.root().unwrap();
            file.save(page.deref()).unwrap();
        }

        let file: File<Block> = File::open(path).unwrap();
        let mut page = file.load(file.offset(ROOT), size).unwrap();

        assert_eq!(page.copy(), data);

        for (k, v, p, _) in data.iter() {
            assert_eq!(get(&page, k), Some((v.to_vec(), *p)));
        }

        page.remove(page.find(b"aaa").unwrap());
        assert_eq!(get(&page, b"aaa"), None);

        page.remove(page.find(b"zzz").unwrap());
        assert_eq!(get(&page, b"zzz"), None);
    }

    #[test]
    fn test_file() {
        let path = Path::new("target/test_file.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let size: u32 = 256;

        let data = [
            (b"uno".to_vec(), b"la squadra azzurra".to_vec()),
            (b"due".to_vec(), b"it's coming home".to_vec()),
            (b"tre".to_vec(), b"red devils".to_vec()),
        ];

        let file: File<Block> = File::make(path, size).unwrap();

        for (k, v) in data.iter() {
            file.insert(k, v).unwrap();
        }

        for (k, v) in data.iter() {
            assert_eq!(file.lookup(k).unwrap().unwrap().deref(), v);
            file.remove(k).unwrap();
        }

        for (k, _) in data.iter() {
            assert!(file.lookup(k).unwrap().is_none());
        }

        let root = file.root().unwrap();
        assert_eq!(root.copy(), vec![]);
    }

    #[test]
    fn test_split() {
        let path = Path::new("target/test_split.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let size: u32 = 256;
        let file: File<Block> = File::make(path, size).unwrap();

        let count = 25;
        let data = (0..count)
            .map(|i| {
                let c = b'a' + (i % (b'z' - b'a' + 1) as u64) as u8;
                (vec![c; 8], vec![c; 8])
            })
            .collect::<Vec<_>>();

        for (k, v) in data.iter() {
            file.insert(k, v).unwrap();
        }

        for (k, v) in data.iter() {
            assert_eq!(file.lookup(k).unwrap().unwrap().deref(), v);
        }
    }

    #[test]
    fn test_merge() {
        let path = Path::new("target/test_merge.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let size: u32 = 256;
        let file: File<Block> = File::make(path, size).unwrap();

        let count = 25;
        let data = {
            let mut rng = StdRng::seed_from_u64(3);
            let mut result = (0..count)
                .map(|i| {
                    let c = b'a' + (i % (b'z' - b'a' + 1) as u64) as u8;
                    (vec![c; 8], vec![c; 8])
                })
                .collect::<Vec<_>>();
            result.shuffle(&mut rng);
            result
        };

        for (k, v) in data.iter() {
            debug!("insert: key={} val={}", hex(k), hex(v));
            file.insert(k, v).unwrap();
        }
        debug!("{}", file.dump());

        let keys = {
            let mut rng = StdRng::seed_from_u64(3);
            let mut result = data.iter().map(|(k, _)| k).cloned().collect::<Vec<_>>();
            result.shuffle(&mut rng);
            result
        };

        let mut removed = std::collections::HashSet::with_capacity(size as usize);
        for key in keys.iter() {
            debug!("remove: key={}", hex(key));
            file.remove(key).unwrap();
            removed.insert(key.to_vec());
            for (k, v) in data.iter() {
                if removed.contains(k) {
                    assert!(file.lookup(k).unwrap().is_none());
                } else {
                    assert_eq!(file.lookup(k).unwrap().unwrap().deref(), v);
                }
            }
        }
        debug!("{}", file.dump());

        let root = file.root().unwrap();
        let copy = root.copy();
        assert_eq!(copy, vec![]);
    }

    #[test]
    fn test_above() {
        let path = Path::new("target/test_above.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let size: u32 = 256;
        let file: File<Block> = File::make(path, size).unwrap();

        let count = 10;
        let mut data = {
            let mut rng = StdRng::seed_from_u64(3);
            let mut result = (0..count)
                .map(|i| {
                    let b = (i + 1) * count;
                    (vec![b; 8], vec![b; 8])
                })
                .collect::<Vec<_>>();
            result.shuffle(&mut rng);
            result
        };

        for (k, v) in data.iter() {
            debug!("insert: key={} val={}", hex(k), hex(v));
            file.insert(k, v).unwrap();
        }

        data.sort();
        let min = file.min().unwrap().unwrap().to_vec();
        let max = file.max().unwrap().unwrap().to_vec();
        assert_eq!(min, data[0].0.to_vec());
        assert_eq!(max, data.last().unwrap().0.to_vec());
        assert!(file.above(&max).unwrap().is_none());

        let asc = {
            let mut result = Vec::with_capacity(data.len());
            let mut val = file.min().unwrap().unwrap().to_vec();
            result.push(val.clone());
            while let Some(next) = file.above(&val).unwrap() {
                result.push(next.to_vec());
                val = next.to_vec();
            }
            result
        };
        assert_eq!(
            asc,
            data.clone().into_iter().map(|(k, _)| k).collect::<Vec<_>>()
        );

        assert_eq!(file.above(&max).unwrap(), None);
        let mut max = max;
        *max.last_mut().unwrap() += 1;
        assert_eq!(file.above(&max).unwrap(), None);
    }

    #[test]
    fn test_below() {
        let path = Path::new("target/test_below.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let size: u32 = 256;
        let file: File<Block> = File::make(path, size).unwrap();

        let count = 10;
        let mut data = {
            let mut rng = StdRng::seed_from_u64(3);
            let mut result = (0..count)
                .map(|i| {
                    let b = (i + 1) * count;
                    (vec![b; 8], vec![b; 8])
                })
                .collect::<Vec<_>>();
            result.shuffle(&mut rng);
            result
        };

        for (k, v) in data.iter() {
            debug!("insert: key={} val={}", hex(k), hex(v));
            file.insert(k, v).unwrap();
        }

        data.sort();
        let min = file.min().unwrap().unwrap().to_vec();
        let max = file.max().unwrap().unwrap().to_vec();
        assert_eq!(min, data[0].0.to_vec());
        assert_eq!(max, data.last().unwrap().0.to_vec());
        assert_eq!(file.below(&min).unwrap().map(|v| v.to_vec()), None);

        data.reverse();
        let desc = {
            let mut result = Vec::with_capacity(data.len());
            let mut val = file.max().unwrap().unwrap().to_vec();
            result.push(val.clone());
            while let Some(next) = file.below(&val).unwrap() {
                result.push(next.to_vec());
                val = next.to_vec();
            }
            result
        };
        assert_eq!(
            desc,
            data.clone().into_iter().map(|(k, _)| k).collect::<Vec<_>>()
        );

        assert_eq!(file.below(&min).unwrap(), None);
        let mut min = min;
        *min.last_mut().unwrap() -= 1;
        assert_eq!(file.below(&min).unwrap(), None);
    }

    #[test]
    fn test_1k() {
        let path = Path::new("target/test_1k.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let size: u32 = 4096;
        let file: File<Block> = File::make(path, size).unwrap();

        let count = 1000;
        let data = util::data(count, 42);

        for (i, (k, v)) in data.iter().enumerate() {
            debug!("({:05}) insert: key={} val={}", i, hex(k), hex(v));
            file.insert(k, v).unwrap();
        }

        drop(file);
        let file: File<Block> = File::open(path).unwrap();

        for (k, v) in data.iter() {
            assert_eq!(file.lookup(k).unwrap().unwrap().deref(), v);
        }

        let mut sorted = data.iter().map(|(k, _)| k).cloned().collect::<Vec<_>>();
        sorted.sort();
        let min = file.min().unwrap().unwrap().to_vec();
        let max = file.max().unwrap().unwrap().to_vec();
        assert_eq!(min, sorted[0]);
        assert_eq!(max, sorted.last().cloned().unwrap());
        assert_eq!(file.below(&min).unwrap().map(|v| v.to_vec()), None);
        assert_eq!(file.above(&max).unwrap().map(|v| v.to_vec()), None);
        let asc = {
            let mut result = Vec::with_capacity(data.len());
            let mut this = file.min().unwrap().unwrap().to_vec();
            result.push(this.clone());
            while let Some(next) = file.above(&this).unwrap() {
                result.push(next.to_vec());
                this = next.to_vec();
            }
            result
        };
        let desc = {
            let mut result = Vec::with_capacity(data.len());
            let mut this = file.max().unwrap().unwrap().to_vec();
            result.push(this.clone());
            while let Some(next) = file.below(&this).unwrap() {
                result.push(next.to_vec());
                this = next.to_vec();
            }
            result
        };

        for (i, (put, got)) in sorted.iter().zip(asc.iter()).enumerate() {
            assert_eq!(
                put,
                got,
                "ASC: index={}: expected '{}' but got '{}'",
                i,
                hex(put),
                hex(got)
            );
        }
        assert_eq!(asc, sorted);

        sorted.reverse();
        for (i, (put, got)) in sorted.iter().zip(desc.iter()).enumerate() {
            assert_eq!(
                put,
                got,
                "DESC: index={}: expected '{}' but got '{}'",
                i,
                hex(put),
                hex(got)
            );
        }
        assert_eq!(desc, sorted);

        for (i, (key, _)) in data.iter().enumerate() {
            debug!("({:05}) remove: key={}", i, hex(key));
            file.remove(key).unwrap();
        }

        for (i, (key, _)) in data.iter().enumerate() {
            debug!("({:05}) lookup: key={}", i, hex(key));
            let found = file.lookup(key).unwrap().map(|v| hex(&v));
            assert_eq!(found, None);
        }

        let copy = file.root().unwrap().copy();
        debug!("{}", file.dump());
        assert!(copy.is_empty());
    }

    #[test]
    fn test_large_value_overflow() {
        let path = Path::new("target/test_large_value.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        // Key fits in page, value is larger than a page -- triggers overflow
        let key = b"hello";
        let val = vec![42u8; 1024];
        file.insert(key, &val).unwrap();

        let found = file.lookup(key).unwrap().unwrap();
        assert_eq!(found, val);

        // Remove the overflow entry
        file.remove(key).unwrap();
        assert!(file.lookup(key).unwrap().is_none());
    }

    #[test]
    fn test_large_key_overflow() {
        let path = Path::new("target/test_large_key.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        // Both key and value are larger than a page
        let big_key = vec![42u8; 1024];
        let big_val = vec![99u8; 2048];
        file.insert(&big_key, &big_val).unwrap();

        let found = file.lookup(&big_key).unwrap().unwrap();
        assert_eq!(found, big_val);

        file.remove(&big_key).unwrap();
        assert!(file.lookup(&big_key).unwrap().is_none());
    }

    #[test]
    fn test_overflow_mixed() {
        let path = Path::new("target/test_overflow_mixed.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        // Mix of small inline entries and overflow entries
        let small_data = [
            (b"aaa".to_vec(), b"small_value_1".to_vec()),
            (b"bbb".to_vec(), b"small_value_2".to_vec()),
            (b"ccc".to_vec(), b"small_value_3".to_vec()),
        ];
        let big_val = vec![0xFFu8; 512];
        let big_data = [
            (b"ddd".to_vec(), big_val.clone()),
            (b"eee".to_vec(), big_val.clone()),
        ];

        for (k, v) in small_data.iter().chain(big_data.iter()) {
            file.insert(k, v).unwrap();
        }

        for (k, v) in small_data.iter().chain(big_data.iter()) {
            let found = file.lookup(k).unwrap().unwrap();
            assert_eq!(found.deref(), v.as_slice());
        }

        // Remove all
        for (k, _) in small_data.iter().chain(big_data.iter()) {
            file.remove(k).unwrap();
        }
        assert!(file.is_empty());
    }

    #[test]
    fn test_overflow_persistence() {
        let path = Path::new("target/test_overflow_persist.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let page_bytes: u32 = 256;
        let key = b"persist_key";
        let val = vec![0xABu8; 800];

        {
            let file: File<Block> = File::make(path, page_bytes).unwrap();
            file.insert(key, &val).unwrap();
        }

        // Reopen and verify
        {
            let file: File<Block> = File::open(path).unwrap();
            let found = file.lookup(key).unwrap().unwrap();
            assert_eq!(found, val);
        }
    }

    #[test]
    fn test_overflow_with_split() {
        // Overflow entries survive a split that reorganizes the tree
        let path = Path::new("target/test_overflow_split.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xCCu8; 512];

        // Insert several small entries plus some overflow entries to trigger splits
        let mut all = Vec::new();
        for i in 0u8..10 {
            let key = vec![b'a' + i; 4];
            let val = vec![i; 4];
            file.insert(&key, &val).unwrap();
            all.push((key, val));
        }

        // Now insert overflow entries
        for i in 0u8..3 {
            let key = vec![b'A' + i; 8];
            file.insert(&key, &big_val).unwrap();
            all.push((key, big_val.clone()));
        }

        // Verify all entries are retrievable
        for (k, v) in all.iter() {
            let found = file.lookup(k).unwrap();
            assert!(found.is_some(), "Key {} not found", hex(k));
            assert_eq!(found.unwrap().deref(), v.as_slice());
        }

        // Remove some and verify the rest
        for (k, _) in all.iter().take(5) {
            file.remove(k).unwrap();
        }
        for (k, v) in all.iter().skip(5) {
            let found = file.lookup(k).unwrap();
            assert!(
                found.is_some(),
                "Key {} not found after partial removal",
                hex(k)
            );
            assert_eq!(found.unwrap().deref(), v.as_slice());
        }
    }

    #[test]
    fn test_overflow_persistence_mixed() {
        // Mixed inline + overflow entries survive close and reopen
        let path = Path::new("target/test_overflow_persist_mixed.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let page_bytes: u32 = 256;
        let big_val = vec![0xDDu8; 600];

        let data = [
            (b"small1".to_vec(), b"val1".to_vec()),
            (b"small2".to_vec(), b"val2".to_vec()),
            (b"big1".to_vec(), big_val.clone()),
            (b"big2".to_vec(), big_val.clone()),
            (b"small3".to_vec(), b"val3".to_vec()),
        ];

        {
            let file: File<Block> = File::make(path, page_bytes).unwrap();
            for (k, v) in data.iter() {
                file.insert(k, v).unwrap();
            }
        }

        // Reopen and verify all entries persist
        {
            let file: File<Block> = File::open(path).unwrap();
            for (k, v) in data.iter() {
                let found = file.lookup(k).unwrap();
                assert!(found.is_some(), "Key {} not found after reopen", hex(k));
                assert_eq!(found.unwrap().deref(), v.as_slice());
            }

            // Also remove and verify
            for (k, _) in data.iter() {
                file.remove(k).unwrap();
            }
            assert!(file.is_empty());
        }
    }

    #[test]
    fn test_overflow_replace() {
        // Replacing an overflow entry with a new value
        let path = Path::new("target/test_overflow_replace.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let key = b"replace_me";
        let val1 = vec![0x11u8; 400];
        let val2 = vec![0x22u8; 500];

        file.insert(key, &val1).unwrap();
        assert_eq!(file.lookup(key).unwrap().unwrap(), val1);

        // Replace with a different overflow value
        file.insert(key, &val2).unwrap();
        assert_eq!(file.lookup(key).unwrap().unwrap(), val2);

        file.remove(key).unwrap();
        assert!(file.lookup(key).unwrap().is_none());
    }

    // ---- Coverage: open/make error paths ----

    #[test]
    fn test_make_file_exists() {
        let path = Path::new("target/test_make_exists.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let _file: File<Block> = File::make(path, 256).unwrap();
        // Second make should fail
        let result = File::<Block>::make(path, 256);
        assert!(result.is_err());
    }

    #[test]
    fn test_open_short_file() {
        let path = Path::new("target/test_open_short.tmp");
        // Write a file that's too short
        fs::write(path, b"short").unwrap();
        let result = File::<Block>::open(path);
        assert!(result.is_err());
    }

    #[test]
    fn test_open_bad_magic() {
        let path = Path::new("target/test_open_bad_magic.tmp");
        // Write 16 bytes with wrong magic
        let mut buf = vec![0u8; 272]; // HEAD(16) + one page(256)
        buf[0..8].copy_from_slice(b"BADMAGIC");
        buf[8..12].copy_from_slice(&256u32.to_be_bytes());
        buf[12..16].copy_from_slice(&1u32.to_be_bytes());
        fs::write(path, &buf).unwrap();
        let result = File::<Block>::open(path);
        assert!(result.is_err());
    }

    #[test]
    fn test_open_page_size_too_large() {
        let path = Path::new("target/test_open_large_page.tmp");
        let mut buf = vec![0u8; 16 + 65536];
        buf[0..8].copy_from_slice(b"YAKVDB42");
        buf[8..12].copy_from_slice(&(65536u32 + 1).to_be_bytes()); // > u16::MAX
        buf[12..16].copy_from_slice(&1u32.to_be_bytes());
        fs::write(path, &buf).unwrap();
        let result = File::<Block>::open(path);
        assert!(result.is_err());
    }

    #[test]
    fn test_open_incomplete_file() {
        let path = Path::new("target/test_open_incomplete.tmp");
        // Write valid header but not enough data for one full page
        let mut buf = vec![0u8; 20]; // HEAD(16) + 4 bytes (not a full page)
        buf[0..8].copy_from_slice(b"YAKVDB42");
        buf[8..12].copy_from_slice(&256u32.to_be_bytes());
        buf[12..16].copy_from_slice(&1u32.to_be_bytes());
        fs::write(path, &buf).unwrap();
        let result = File::<Block>::open(path);
        assert!(result.is_err());
    }

    // ---- Coverage: page_size() ----

    #[test]
    fn test_page_size() {
        let path = Path::new("target/test_page_size.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let file: File<Block> = File::make(path, 512).unwrap();
        assert_eq!(file.page_size(), 512);
    }

    // ---- Coverage: empty tree operations ----

    #[test]
    fn test_empty_tree_min_max() {
        let path = Path::new("target/test_empty_minmax.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let file: File<Block> = File::make(path, 256).unwrap();
        assert!(file.is_empty());
        assert_eq!(file.min().unwrap(), None);
        assert_eq!(file.max().unwrap(), None);
    }

    #[test]
    fn test_empty_tree_above_below() {
        let path = Path::new("target/test_empty_abovebelow.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let file: File<Block> = File::make(path, 256).unwrap();
        assert_eq!(file.above(b"anything").unwrap(), None);
        assert_eq!(file.below(b"anything").unwrap(), None);
    }

    // ---- Coverage: overflow with min/max/above/below ----

    #[test]
    fn test_overflow_min_max() {
        let path = Path::new("target/test_overflow_minmax.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xAAu8; 512];
        // Insert small keys with big values, triggering overflow
        file.insert(b"aaa", &big_val).unwrap();
        file.insert(b"zzz", &big_val).unwrap();

        let min = file.min().unwrap().unwrap();
        let max = file.max().unwrap().unwrap();
        assert_eq!(min, b"aaa".to_vec());
        assert_eq!(max, b"zzz".to_vec());
    }

    #[test]
    fn test_overflow_above_below() {
        let path = Path::new("target/test_overflow_abovebelow.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xBBu8; 512];
        file.insert(b"bbb", &big_val).unwrap();
        file.insert(b"ddd", &big_val).unwrap();
        file.insert(b"fff", &big_val).unwrap();

        // above
        let above_b = file.above(b"bbb").unwrap();
        assert_eq!(above_b, Some(b"ddd".to_vec()));
        let above_d = file.above(b"ddd").unwrap();
        assert_eq!(above_d, Some(b"fff".to_vec()));
        let above_f = file.above(b"fff").unwrap();
        assert_eq!(above_f, None);

        // below
        let below_f = file.below(b"fff").unwrap();
        assert_eq!(below_f, Some(b"ddd".to_vec()));
        let below_d = file.below(b"ddd").unwrap();
        assert_eq!(below_d, Some(b"bbb".to_vec()));
        let below_b = file.below(b"bbb").unwrap();
        assert_eq!(below_b, None);
    }

    // ---- Coverage: dump with overflow entries ----

    #[test]
    fn test_dump_with_overflow() {
        let path = Path::new("target/test_dump_overflow.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xCCu8; 400];
        file.insert(b"key1", b"small_val").unwrap();
        file.insert(b"key2", &big_val).unwrap();

        let dump = file.dump();
        assert!(!dump.is_empty());
        // Dump should mention OVERFLOW for the big entry
        assert!(dump.contains("OVERFLOW"));
    }

    // ---- Coverage: open with overflow regions and empty pages ----

    #[test]
    fn test_open_with_overflow_and_empty_pages() {
        init_log();
        let path = Path::new("target/test_open_overflow_empty.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let page_bytes: u32 = 256;
        let big_val = vec![0xEEu8; 600];

        // Insert many entries to create splits, then remove some to create empty pages
        {
            let file: File<Block> = File::make(path, page_bytes).unwrap();
            for i in 0u8..15 {
                let key = vec![b'a' + i; 4];
                let val = vec![i; 4];
                file.insert(&key, &val).unwrap();
            }
            // Insert an overflow entry
            file.insert(b"zzzz", &big_val).unwrap();

            // Remove some to create empty pages after merge
            for i in 0u8..10 {
                let key = vec![b'a' + i; 4];
                file.remove(&key).unwrap();
            }
        }

        // Reopen -- should correctly skip overflow regions and find empty pages
        let file: File<Block> = File::open(path).unwrap();
        // The remaining entries should still be accessible
        let found = file.lookup(b"zzzz").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap(), big_val);
    }

    // ---- Coverage: lookup/remove on keys not found (non-overflow) ----

    #[test]
    fn test_lookup_miss_with_entries() {
        let path = Path::new("target/test_lookup_miss.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let file: File<Block> = File::make(path, 256).unwrap();
        file.insert(b"aaa", b"111").unwrap();
        file.insert(b"ccc", b"333").unwrap();

        // Key not in the tree (between existing keys)
        assert!(file.lookup(b"bbb").unwrap().is_none());
        // Key beyond max
        assert!(file.lookup(b"zzz").unwrap().is_none());
    }

    #[test]
    fn test_remove_nonexistent() {
        let path = Path::new("target/test_remove_miss.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let file: File<Block> = File::make(path, 256).unwrap();
        file.insert(b"aaa", b"111").unwrap();

        // Remove key that doesn't exist -- should be ok
        file.remove(b"bbb").unwrap();
        // Original still there
        assert_eq!(file.lookup(b"aaa").unwrap().unwrap().deref(), b"111");
    }

    // ---- Coverage: overflow entry not matching (scan miss) ----

    #[test]
    fn test_overflow_lookup_scan_miss() {
        let path = Path::new("target/test_overflow_scan_miss.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let file: File<Block> = File::make(path, 256).unwrap();

        let big_val = vec![0xAAu8; 500];
        file.insert(b"abc_key", &big_val).unwrap();

        // Lookup a key that shares prefix with the overflow entry but differs
        assert!(file.lookup(b"abc_key_extra").unwrap().is_none());
        // Lookup a key that doesn't share prefix at all
        assert!(file.lookup(b"xyz_key").unwrap().is_none());
    }

    // ---- Coverage: overflow entry insert into non-empty page (key fits inline, val overflow) ----

    #[test]
    fn test_insert_overflow_into_nonempty_page() {
        let path = Path::new("target/test_insert_ov_nonempty.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let file: File<Block> = File::make(path, 256).unwrap();

        // First insert a small entry
        file.insert(b"aaa", b"small").unwrap();

        // Then insert an overflow entry into the same page
        let big_val = vec![0xFFu8; 500];
        file.insert(b"bbb", &big_val).unwrap();

        assert_eq!(file.lookup(b"aaa").unwrap().unwrap().deref(), b"small");
        assert_eq!(file.lookup(b"bbb").unwrap().unwrap(), big_val);
    }

    // ---- Coverage: remove overflow entry found by ceil (not prefix fallback) ----

    #[test]
    fn test_remove_overflow_ceil_match() {
        let path = Path::new("target/test_remove_ov_ceil.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let file: File<Block> = File::make(path, 256).unwrap();

        let big_val = vec![0xDDu8; 500];
        file.insert(b"hello", &big_val).unwrap();
        assert!(file.lookup(b"hello").unwrap().is_some());

        // Remove it -- ceil should find it because key fits inline
        file.remove(b"hello").unwrap();
        assert!(file.lookup(b"hello").unwrap().is_none());
    }

    // ---- Coverage: remove overflow entry where key doesn't match ----

    #[test]
    fn test_remove_overflow_key_mismatch() {
        let path = Path::new("target/test_remove_ov_mismatch.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let file: File<Block> = File::make(path, 256).unwrap();

        let big_val = vec![0xCCu8; 500];
        file.insert(b"hello", &big_val).unwrap();

        // Try to remove a key that ceil matches to the overflow entry but isn't equal
        file.remove(b"help").unwrap(); // "help" < "hello", but ceil("help") -> "hello"
        // Original should still be there
        assert!(file.lookup(b"hello").unwrap().is_some());
    }

    // ---- Coverage: above/below at boundaries ----

    #[test]
    fn test_above_beyond_max() {
        let path = Path::new("target/test_above_beyond.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let file: File<Block> = File::make(path, 256).unwrap();
        file.insert(b"aaa", b"111").unwrap();
        file.insert(b"bbb", b"222").unwrap();

        // Key beyond max
        assert_eq!(file.above(b"zzz").unwrap(), None);
        // Key equal to max, no next
        assert_eq!(file.above(b"bbb").unwrap(), None);
        // Key below first
        assert_eq!(file.above(b"000").unwrap(), Some(b"aaa".to_vec()));
    }

    #[test]
    fn test_below_at_min() {
        let path = Path::new("target/test_below_at_min.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let file: File<Block> = File::make(path, 256).unwrap();
        file.insert(b"bbb", b"222").unwrap();
        file.insert(b"ccc", b"333").unwrap();

        assert_eq!(file.below(b"bbb").unwrap(), None);
        assert_eq!(file.below(b"aaa").unwrap(), None);
        assert_eq!(file.below(b"ccc").unwrap(), Some(b"bbb".to_vec()));
    }

    // ---- Coverage: split with overflow entries at root level ----

    #[test]
    fn test_split_root_with_overflow() {
        init_log();
        let path = Path::new("target/test_split_root_ov.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xAAu8; 400];

        // Each overflow entry uses ~(key_len + SLOT_BYTES) bytes inline on the page.
        // With page_bytes=256, capacity=240. SPLIT_THRESHOLD=80% → need > 192 bytes.
        // Using 20-byte keys: 20+16=36 bytes each. Need 6 entries → 216 > 192.
        let keys: Vec<Vec<u8>> = (0..8)
            .map(|i| format!("overflowkey_{:04}", i).into_bytes())
            .collect();
        for key in &keys {
            file.insert(key, &big_val).unwrap();
        }

        // Verify all entries survived the root split
        for key in &keys {
            let found = file.lookup(key).unwrap();
            assert!(
                found.is_some(),
                "Key {:?} not found after root split",
                std::str::from_utf8(key)
            );
            assert_eq!(found.unwrap(), big_val);
        }

        // Tree should have structure visible in dump
        let dump = file.dump();
        assert!(dump.contains("OVERFLOW"));
    }

    // ---- Coverage: merge with overflow entries ----

    #[test]
    fn test_merge_with_overflow() {
        init_log();
        let path = Path::new("target/test_merge_ov.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xBBu8; 400];

        // Insert many small entries spread across many leaves
        for i in 0u8..20 {
            let key = format!("s{:04}", i as u16 * 3);
            file.insert(key.as_bytes(), &[i; 8]).unwrap();
        }
        // Insert overflow entries into different parts of the tree
        file.insert(b"s0001", &big_val).unwrap();
        file.insert(b"s0031", &big_val).unwrap();
        file.insert(b"s0058", &big_val).unwrap();

        // Aggressively remove small entries to trigger merges of pages with overflow
        for i in 0u8..18 {
            let key = format!("s{:04}", i as u16 * 3);
            file.remove(key.as_bytes()).unwrap();
        }

        // Remaining entries should still be accessible
        for i in 18u8..20 {
            let key = format!("s{:04}", i as u16 * 3);
            let found = file.lookup(key.as_bytes()).unwrap();
            assert!(found.is_some(), "Key {} not found after merge", key);
        }
        assert_eq!(file.lookup(b"s0001").unwrap().unwrap(), big_val);
        assert_eq!(file.lookup(b"s0031").unwrap().unwrap(), big_val);
        assert_eq!(file.lookup(b"s0058").unwrap().unwrap(), big_val);
    }

    // ---- Coverage: large key overflow with remove via prefix fallback ----

    #[test]
    fn test_large_key_remove_via_prefix() {
        let path = Path::new("target/test_large_key_remove_prefix.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        // Key larger than a page
        let big_key = vec![0x42u8; 512];
        let big_val = vec![0x99u8; 512];
        file.insert(&big_key, &big_val).unwrap();

        // Lookup via prefix scan
        assert_eq!(file.lookup(&big_key).unwrap().unwrap(), big_val);

        // Remove via prefix scan
        file.remove(&big_key).unwrap();
        assert!(file.lookup(&big_key).unwrap().is_none());
    }

    // ---- Coverage: above/below traversal through multi-level tree with overflow ----

    #[test]
    fn test_above_below_multilevel_overflow() {
        init_log();
        let path = Path::new("target/test_above_below_ml_ov.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xCCu8; 400];

        // Create a multi-level tree with overflow entries
        // Small entries to build tree structure
        for i in 0u8..10 {
            let key = vec![b'a' + i * 2; 4]; // a, c, e, g, i, k, m, o, q, s
            file.insert(&key, &[i; 4]).unwrap();
        }
        // Overflow entries interspersed
        file.insert(b"bbbb", &big_val).unwrap();
        file.insert(b"llll", &big_val).unwrap();

        // Test above/below traversal
        let min = file.min().unwrap().unwrap();
        let max = file.max().unwrap().unwrap();

        // Walk up from min
        let mut current = min.clone();
        let mut count = 1;
        while let Some(next) = file.above(&current).unwrap() {
            assert!(next > current, "above should return strictly greater keys");
            current = next;
            count += 1;
        }
        assert_eq!(count, 12); // 10 small + 2 overflow

        // Walk down from max
        current = max;
        count = 1;
        while let Some(next) = file.below(&current).unwrap() {
            assert!(next < current, "below should return strictly smaller keys");
            current = next;
            count += 1;
        }
        assert_eq!(count, 12);
    }

    // ---- Coverage: insert with key prefix path (key doesn't fit inline) ----

    #[test]
    fn test_insert_key_prefix_into_nonempty() {
        // Use a larger page so the full key can serve as separator in internal nodes
        let path = Path::new("target/test_insert_keyprefix_nonempty.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 512;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        // Insert a small entry first
        file.insert(b"aaa", b"small").unwrap();

        // Now insert a key that is larger than remaining page free space
        // (triggers key prefix path in insert)
        // page free after "aaa"+"small" = 512 - 16(head) - 16(slot) - 8(data) = 472
        // key(460) + SLOT(16) = 476 > 472, so key doesn't fit -> prefix path
        let big_key = vec![0xFFu8; 460];
        let big_val = vec![0xEEu8; 300];
        file.insert(&big_key, &big_val).unwrap();

        // Verify both entries
        assert_eq!(file.lookup(b"aaa").unwrap().unwrap().deref(), b"small");
        assert_eq!(file.lookup(&big_key).unwrap().unwrap(), big_val);
    }

    // ---- Coverage: above/below needing subtree traversal where overflow is at boundary ----

    #[test]
    fn test_above_subtree_overflow_boundary() {
        init_log();
        // Construct a multi-level tree where above() must jump to the next subtree
        // and the first entry in that subtree is an overflow entry.
        let path = Path::new("target/test_above_subtree_ov.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xABu8; 400];

        // Build a multi-level tree with many small entries
        for i in 0u16..20 {
            let key = format!("k{:04}", i * 3);
            file.insert(key.as_bytes(), &[i as u8; 8]).unwrap();
        }

        // Now insert an overflow entry that will land near a subtree boundary
        // This key should be between two existing small keys, and will be
        // in a leaf that's at a subtree boundary
        file.insert(b"k0031", &big_val).unwrap(); // between k0030 and k0033

        // Query above() for keys near the overflow entry
        // This will require subtree traversal if the overflow is the first entry in a subtree
        for i in 0u16..20 {
            let key = format!("k{:04}", i * 3);
            if let Some(next) = file.above(key.as_bytes()).unwrap() {
                assert!(next > key.as_bytes().to_vec());
            }
        }

        // Query above the overflow entry
        let above = file.above(b"k0031").unwrap();
        if let Some(next) = above {
            assert!(next > b"k0031".to_vec());
        }
    }

    #[test]
    fn test_below_subtree_overflow_boundary() {
        init_log();
        // Construct a multi-level tree where below() must jump to the previous subtree
        // and the last entry is an overflow entry.
        let path = Path::new("target/test_below_subtree_ov.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xCDu8; 400];

        // Build the tree
        for i in 0u16..20 {
            let key = format!("m{:04}", i * 3);
            file.insert(key.as_bytes(), &[i as u8; 8]).unwrap();
        }

        // Insert overflow entries near boundaries
        file.insert(b"m0029", &big_val).unwrap();
        file.insert(b"m0002", &big_val).unwrap();

        // Query below() for keys near the overflow entries
        for i in (0u16..20).rev() {
            let key = format!("m{:04}", i * 3);
            if let Some(prev) = file.below(key.as_bytes()).unwrap() {
                assert!(prev < key.as_bytes().to_vec());
            }
        }

        // Query below the overflow entry
        let below = file.below(b"m0029").unwrap();
        if let Some(prev) = below {
            assert!(prev < b"m0029".to_vec());
        }
    }

    // ---- Coverage: remove overflow entry from child page (not root) via prefix fallback ----

    #[test]
    fn test_remove_large_key_from_child_page() {
        init_log();
        // Use page_bytes=512 so the full key fits as a separator in internal nodes
        // but doesn't fit inline in a leaf that already has entries.
        let path = Path::new("target/test_remove_large_key_child.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 512;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        // Build a multi-level tree first with small entries
        for i in 0u16..30 {
            let key = format!("entry{:04}", i);
            file.insert(key.as_bytes(), &[i as u8; 8]).unwrap();
        }

        // Insert a big key that requires the prefix overflow path in a leaf.
        // max fit in leaf = 512 - 16 - slots/data already used. With a busy leaf:
        // free ~350, so key of 460 bytes won't fit inline (460+16=476>350),
        // but fits as separator in an empty root/internal node (476<496)
        let big_key = vec![0x42u8; 460];
        let val = b"bigval".to_vec();
        file.insert(&big_key, &val).unwrap();

        // Verify lookup
        assert_eq!(
            file.lookup(&big_key).unwrap().unwrap().deref(),
            val.as_slice()
        );

        // Remove the big key -- since it's stored with a prefix, ceil won't find it
        // exactly, so the prefix fallback path kicks in
        file.remove(&big_key).unwrap();
        assert!(file.lookup(&big_key).unwrap().is_none());

        // All small entries should still be intact
        for i in 0u16..30 {
            let key = format!("entry{:04}", i);
            assert!(
                file.lookup(key.as_bytes()).unwrap().is_some(),
                "Entry {} missing after big key removal",
                i
            );
        }
    }

    // ---- Coverage: next_id reuses freed pages ----

    #[test]
    fn test_next_id_reuse_freed_pages() {
        init_log();
        let path = Path::new("target/test_nextid_reuse.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;

        // Create entries causing splits (many pages), then remove most
        {
            let file: File<Block> = File::make(path, page_bytes).unwrap();
            // Insert enough to create multiple page splits
            for i in 0u16..20 {
                let key = format!("r{:04}", i);
                file.insert(key.as_bytes(), &[i as u8; 8]).unwrap();
            }
            // Remove most entries to trigger merges that leave empty pages
            for i in 0u16..18 {
                let key = format!("r{:04}", i);
                file.remove(key.as_bytes()).unwrap();
            }
        }

        // Reopen -- open() should find empty pages and add them to the heap
        {
            let file: File<Block> = File::open(path).unwrap();
            // Insert enough new entries to trigger splits and page allocation.
            // next_id() should reuse freed page IDs from the empty heap.
            for i in 0u16..20 {
                let key = format!("n{:04}", i);
                file.insert(key.as_bytes(), &[i as u8; 8]).unwrap();
            }
            // Verify
            for i in 0u16..20 {
                let key = format!("n{:04}", i);
                assert!(
                    file.lookup(key.as_bytes()).unwrap().is_some(),
                    "Key n{:04} not found",
                    i
                );
            }
        }
    }

    // ---- Coverage: merge page with overflow entries through the merge code path ----

    #[test]
    fn test_merge_moves_overflow_entries() {
        init_log();
        let path = Path::new("target/test_merge_moves_ov.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xEEu8; 400];

        // Insert entries to create a multi-level tree
        for i in 0u8..15 {
            let key = format!("node_{:02}", i);
            file.insert(key.as_bytes(), &[i; 8]).unwrap();
        }

        // Insert overflow entries spread across different leaf pages
        file.insert(b"node_03_ov", &big_val).unwrap();
        file.insert(b"node_08_ov", &big_val).unwrap();
        file.insert(b"node_12_ov", &big_val).unwrap();

        // Now remove most small entries to trigger merges
        for i in 0u8..12 {
            let key = format!("node_{:02}", i);
            file.remove(key.as_bytes()).unwrap();
        }

        // The overflow entries should survive merges
        assert_eq!(file.lookup(b"node_03_ov").unwrap().unwrap(), big_val);
        assert_eq!(file.lookup(b"node_08_ov").unwrap().unwrap(), big_val);
        assert_eq!(file.lookup(b"node_12_ov").unwrap().unwrap(), big_val);

        // Remaining small entries
        for i in 12u8..15 {
            let key = format!("node_{:02}", i);
            assert!(file.lookup(key.as_bytes()).unwrap().is_some());
        }
    }

    // ---- Coverage: non-root split with overflow entries ----

    #[test]
    fn test_nonroot_split_with_overflow() {
        init_log();
        let path = Path::new("target/test_nonroot_split_ov.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xDDu8; 400];

        // First build a multi-level tree
        for i in 0u8..10 {
            let key = format!("base{:03}", i * 10);
            file.insert(key.as_bytes(), &[i; 8]).unwrap();
        }

        // Now insert overflow entries into a specific leaf page to make it split
        for i in 0u8..4 {
            let key = format!("base{:03}", i * 10 + 5); // between existing entries
            file.insert(key.as_bytes(), &big_val).unwrap();
        }

        // Verify all entries
        for i in 0u8..10 {
            let key = format!("base{:03}", i * 10);
            assert!(
                file.lookup(key.as_bytes()).unwrap().is_some(),
                "base entry {} missing",
                i
            );
        }
        for i in 0u8..4 {
            let key = format!("base{:03}", i * 10 + 5);
            assert_eq!(
                file.lookup(key.as_bytes()).unwrap().unwrap(),
                big_val,
                "overflow entry {} mismatch",
                i
            );
        }
    }

    // ---- Coverage: dump with empty tree ----

    #[test]
    fn test_dump_empty() {
        let path = Path::new("target/test_dump_empty.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let file: File<Block> = File::make(path, 256).unwrap();
        let dump = file.dump();
        assert!(dump.contains("empty"));
    }

    // ---- Coverage: above/below where key matches last/first entry exactly ----

    #[test]
    fn test_above_equal_last_in_subtree() {
        // Test that above() handles the case where the queried key equals
        // the last entry in a leaf, requiring traversal to parent's next subtree
        let path = Path::new("target/test_above_eq_last.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        // Build a 2-level tree
        for i in 0u16..16 {
            let key = format!("x{:04}", i);
            file.insert(key.as_bytes(), &[i as u8; 8]).unwrap();
        }

        // Query above for each key -- ensures subtree traversal at boundaries
        let mut keys: Vec<String> = (0..16).map(|i| format!("x{:04}", i)).collect();
        keys.sort();

        for w in keys.windows(2) {
            let above = file.above(w[0].as_bytes()).unwrap();
            assert_eq!(
                above,
                Some(w[1].as_bytes().to_vec()),
                "above({}) should be {}",
                w[0],
                w[1]
            );
        }
        // Last key should have no above
        assert_eq!(file.above(keys.last().unwrap().as_bytes()).unwrap(), None);
    }

    #[test]
    fn test_below_equal_first_in_subtree() {
        let path = Path::new("target/test_below_eq_first.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        for i in 0u16..16 {
            let key = format!("y{:04}", i);
            file.insert(key.as_bytes(), &[i as u8; 8]).unwrap();
        }

        let mut keys: Vec<String> = (0..16).map(|i| format!("y{:04}", i)).collect();
        keys.sort();

        for w in keys.windows(2) {
            let below = file.below(w[1].as_bytes()).unwrap();
            assert_eq!(
                below,
                Some(w[0].as_bytes().to_vec()),
                "below({}) should be {}",
                w[1],
                w[0]
            );
        }
        // First key should have no below
        assert_eq!(file.below(keys.first().unwrap().as_bytes()).unwrap(), None);
    }

    // ---- Coverage: min/max with overflow in multi-level tree ----

    #[test]
    fn test_min_max_overflow_multilevel() {
        let path = Path::new("target/test_minmax_ov_ml.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xFFu8; 400];

        // Build multi-level tree with small entries
        for i in 1u16..15 {
            let key = format!("q{:04}", i);
            file.insert(key.as_bytes(), &[i as u8; 8]).unwrap();
        }

        // Make the min and max entries overflow
        file.insert(b"q0000", &big_val).unwrap(); // new min is overflow
        file.insert(b"q9999", &big_val).unwrap(); // new max is overflow

        let min = file.min().unwrap().unwrap();
        let max = file.max().unwrap().unwrap();
        assert_eq!(min, b"q0000".to_vec());
        assert_eq!(max, b"q9999".to_vec());
    }

    // ---- Coverage: insert overflow where parent needs update ----

    #[test]
    fn test_insert_overflow_updates_parent() {
        let path = Path::new("target/test_insert_ov_parent.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xBBu8; 400];

        // Build a multi-level tree
        for i in 0u16..10 {
            let key = format!("p{:04}", i * 10);
            file.insert(key.as_bytes(), &[i as u8; 8]).unwrap();
        }

        // Insert an overflow entry with a key greater than the current max
        // of a leaf page, triggering parent key update
        file.insert(b"p0999", &big_val).unwrap();

        // Verify
        assert_eq!(file.lookup(b"p0999").unwrap().unwrap(), big_val);

        // Check tree integrity by dumping
        let dump = file.dump();
        assert!(!dump.is_empty());
    }

    // ---- Coverage: comprehensive overflow stress test ----

    #[test]
    fn test_overflow_stress() {
        init_log();
        let path = Path::new("target/test_overflow_stress.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xFFu8; 400];
        let mut all_keys: Vec<Vec<u8>> = Vec::new();

        // Phase 1: Build a multi-level tree with many overflow entries
        // to ensure overflow entries end up at subtree boundaries
        for i in 0u32..30 {
            let key = format!("ov{:06}", i * 7);
            file.insert(key.as_bytes(), &big_val).unwrap();
            all_keys.push(key.into_bytes());
        }
        all_keys.sort();

        // Phase 2: Verify all entries
        for key in &all_keys {
            assert!(
                file.lookup(key).unwrap().is_some(),
                "Missing key after insert: {:?}",
                std::str::from_utf8(key)
            );
        }

        // Phase 3: Test above/below traversal (overflow entries at boundaries)
        for w in all_keys.windows(2) {
            let above = file.above(&w[0]).unwrap();
            assert_eq!(
                above,
                Some(w[1].clone()),
                "above({:?}) should be {:?}",
                std::str::from_utf8(&w[0]),
                std::str::from_utf8(&w[1])
            );
        }
        assert_eq!(file.above(all_keys.last().unwrap()).unwrap(), None);

        for w in all_keys.windows(2) {
            let below = file.below(&w[1]).unwrap();
            assert_eq!(
                below,
                Some(w[0].clone()),
                "below({:?}) should be {:?}",
                std::str::from_utf8(&w[1]),
                std::str::from_utf8(&w[0])
            );
        }
        assert_eq!(file.below(all_keys.first().unwrap()).unwrap(), None);

        // Phase 4: min/max
        assert_eq!(file.min().unwrap().unwrap(), all_keys[0]);
        assert_eq!(file.max().unwrap().unwrap(), *all_keys.last().unwrap());

        // Phase 5: dump should show overflow entries
        let dump = file.dump();
        assert!(dump.contains("OVERFLOW"));

        // Phase 6: Remove half and verify rest
        for key in all_keys.iter().step_by(2) {
            file.remove(key).unwrap();
        }
        for (i, key) in all_keys.iter().enumerate() {
            let found = file.lookup(key).unwrap();
            if i % 2 == 0 {
                assert!(
                    found.is_none(),
                    "Key {:?} should have been removed",
                    std::str::from_utf8(key)
                );
            } else {
                assert!(
                    found.is_some(),
                    "Key {:?} should still exist",
                    std::str::from_utf8(key)
                );
            }
        }
    }

    // ---- Coverage: mixed overflow and inline with full tree operations ----

    #[test]
    fn test_overflow_interleaved_with_inline() {
        init_log();
        let path = Path::new("target/test_overflow_interleaved.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes: u32 = 256;
        let file: File<Block> = File::make(path, page_bytes).unwrap();

        let big_val = vec![0xCCu8; 500];
        let mut all: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        // Interleave small and overflow entries
        for i in 0u32..40 {
            let key = format!("k{:06}", i);
            let val = if i % 3 == 0 {
                big_val.clone()
            } else {
                vec![i as u8; 8]
            };
            file.insert(key.as_bytes(), &val).unwrap();
            all.push((key.into_bytes(), val));
        }
        all.sort_by(|a, b| a.0.cmp(&b.0));

        // Test above/below on this mixed tree
        for w in all.windows(2) {
            let above = file.above(&w[0].0).unwrap();
            assert_eq!(above.as_ref(), Some(&w[1].0));
        }
        for w in all.windows(2) {
            let below = file.below(&w[1].0).unwrap();
            assert_eq!(below.as_ref(), Some(&w[0].0));
        }

        // Remove overflow entries specifically, then remove small entries
        for (key, val) in &all {
            if val.len() > 100 {
                file.remove(key).unwrap();
            }
        }
        for (key, val) in &all {
            let found = file.lookup(key).unwrap();
            if val.len() > 100 {
                assert!(found.is_none());
            } else {
                assert!(found.is_some());
            }
        }
    }

    /// Random inserts, replacements and removes -- some large enough to go to
    /// overflow pages -- against a `BTreeMap`, through a cache of only a few
    /// pages so that nearly every step evicts, and writes back, dirty pages.
    /// Then reopen the file and check that it holds exactly the model.
    #[test]
    fn test_tiny_cache_matches_model() {
        use rand::Rng;
        use std::collections::BTreeMap;

        let path = Path::new("target/test_tiny_cache_matches_model.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let page_bytes = 256;

        let mut rng = StdRng::seed_from_u64(11);
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        {
            let file: File<Block> = File::make_with(
                path,
                page_bytes,
                Options {
                    cache_bytes: 64 * page_bytes as usize,
                    ..Options::default()
                },
            )
            .unwrap();
            for i in 0..5_000 {
                let key = format!("{:05}", rng.gen_range(0..1_500)).into_bytes();
                if rng.gen_bool(0.7) {
                    let len = if rng.gen_bool(0.05) {
                        600
                    } else {
                        rng.gen_range(1..40)
                    };
                    let val = vec![(i % 251) as u8; len];
                    file.insert(&key, &val).unwrap();
                    model.insert(key, val);
                } else {
                    file.remove(&key).unwrap();
                    model.remove(&key);
                }
                if i % 97 == 0 {
                    let probe = format!("{:05}", rng.gen_range(0..1_500)).into_bytes();
                    assert_eq!(file.lookup(&probe).unwrap(), model.get(&probe).cloned());
                }
            }
        }

        let file: File<Block> = File::open(path).unwrap();
        for (key, val) in model.iter() {
            assert_eq!(
                file.lookup(key).unwrap().as_ref(),
                Some(val),
                "key={}",
                hex(key)
            );
        }
        let mut keys = Vec::with_capacity(model.len());
        let mut next = file.min().unwrap();
        while let Some(key) = next {
            next = file.above(&key).unwrap();
            keys.push(key);
        }
        assert_eq!(keys, model.keys().cloned().collect::<Vec<_>>());

        fs::remove_file(path).unwrap();
    }

    // ---- write-ahead log and recovery ----

    type Model = std::collections::BTreeMap<Vec<u8>, Vec<u8>>;

    fn fresh(name: &str) -> PathBuf {
        let path = PathBuf::from(format!("target/{name}.tmp"));
        let _ = fs::remove_file(&path);
        let _ = fs::remove_dir_all(wal_dir(&path));
        path
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_dir_all(wal_dir(path));
    }

    /// Leave the file and its log exactly as a killed process would: no
    /// checkpoint on close, nothing flushed beyond what was committed.
    fn crash<P: Page>(file: File<P>) {
        // the OS drops the lock of a process that dies
        file.file.read().unlock().unwrap();
        std::mem::forget(file);
    }

    thread_local! {
        /// Operations `apply` lets through before failing (`usize::MAX`: all).
        static FAIL_APPLY_AFTER: std::cell::Cell<usize> =
            const { std::cell::Cell::new(usize::MAX) };
    }

    pub(super) fn fail_point() -> Result<()> {
        FAIL_APPLY_AFTER.with(|left| match left.get() {
            usize::MAX => Ok(()),
            0 => Err(Error::Other("Injected apply failure".to_string())),
            n => {
                left.set(n - 1);
                Ok(())
            }
        })
    }

    fn assert_matches(file: &File<Block>, model: &Model) {
        for (key, val) in model.iter() {
            assert_eq!(
                file.lookup(key).unwrap().as_ref(),
                Some(val),
                "key={}",
                hex(key)
            );
        }
        let mut keys = Vec::with_capacity(model.len());
        let mut next = file.min().unwrap();
        while let Some(key) = next {
            next = file.above(&key).unwrap();
            keys.push(key);
        }
        assert_eq!(keys, model.keys().cloned().collect::<Vec<_>>());
    }

    /// Inserts, some big enough for overflow pages, replacements and removes.
    fn workload(file: &File<Block>, model: &mut Model, seed: u64, n: usize) {
        use rand::Rng;
        let mut rng = StdRng::seed_from_u64(seed);
        for i in 0..n {
            let key = format!("{:05}", rng.gen_range(0..n as u32)).into_bytes();
            if rng.gen_bool(0.75) {
                let len = if rng.gen_bool(0.05) {
                    700
                } else {
                    rng.gen_range(1..60)
                };
                let val = vec![(i % 251) as u8; len];
                file.insert(&key, &val).unwrap();
                model.insert(key, val);
            } else {
                file.remove(&key).unwrap();
                model.remove(&key);
            }
        }
    }

    #[test]
    fn test_recovery_replays_log_after_crash() {
        let path = fresh("test_recovery_replays_log_after_crash");
        let mut model = Model::new();
        {
            let file: File<Block> = File::make(&path, 256).unwrap();
            workload(&file, &mut model, 1, 2_000);
            crash(file);
        }

        let file: File<Block> = File::open(&path).unwrap();
        assert_matches(&file, &model);
        // recovery checkpointed what it replayed: only a fresh segment is left
        assert_eq!(wal::segments(&wal_dir(&path)).unwrap().len(), 1);

        // and the database carries on normally after it
        workload(&file, &mut model, 2, 500);
        drop(file);
        let file: File<Block> = File::open(&path).unwrap();
        assert_matches(&file, &model);
        drop(file);
        cleanup(&path);
    }

    /// A crash after the page images are durable but part-way through writing
    /// the pages in place: every stopping point must recover the same state.
    #[test]
    fn test_recovery_from_crash_mid_checkpoint() {
        for crash_after in [0, 1, 2, 5, 10, 25] {
            let path = fresh(&format!("test_recovery_mid_checkpoint_{crash_after}"));
            let mut model = Model::new();
            {
                let file: File<Block> = File::make(&path, 256).unwrap();
                workload(&file, &mut model, 3, 600);
                file.sync().unwrap();
                workload(&file, &mut model, 4, 600);
                let dirty = file.dirty.read().len();
                assert!(dirty > 25, "only {dirty} dirty pages");

                file.crash_after_pages
                    .store(crash_after, std::sync::atomic::Ordering::Relaxed);
                assert!(file.sync().is_err());
                assert!(
                    file.insert(b"after", b"crash").is_err(),
                    "a failed checkpoint must stop further writes"
                );
                crash(file);
            }

            let file: File<Block> = File::open(&path).unwrap();
            assert_matches(&file, &model);
            drop(file);
            cleanup(&path);
        }
    }

    /// Cut the log at many points, as a power loss can: every cut must recover
    /// exactly the operations whose records were complete before it.
    #[test]
    fn test_recovery_from_torn_log() {
        let path = fresh("test_recovery_from_torn_log");
        let mut snapshots = vec![Model::new()];
        {
            let file: File<Block> = File::make(&path, 256).unwrap();
            let mut model = Model::new();
            for i in 0..120u32 {
                let lsn = file.wal.last_lsn();
                let key = format!("{:03}", i % 70).into_bytes();
                if i % 5 == 4 {
                    file.remove(&key).unwrap();
                    model.remove(&key);
                } else {
                    let val = vec![i as u8; if i % 17 == 0 { 600 } else { 20 }];
                    file.insert(&key, &val).unwrap();
                    model.insert(key, val);
                }
                // removing a missing key logs nothing: one snapshot per record
                if file.wal.last_lsn() != lsn {
                    snapshots.push(model.clone());
                }
            }
            crash(file);
        }

        let segments = wal::segments(&wal_dir(&path)).unwrap();
        assert_eq!(segments.len(), 1);
        let log = fs::read(&segments[0].1).unwrap();
        let db = fs::read(&path).unwrap();

        for cut in (0..log.len()).step_by(97).chain([log.len()]) {
            let copy = fresh("test_recovery_from_torn_log_copy");
            fs::write(&copy, &db).unwrap();
            fs::create_dir_all(wal_dir(&copy)).unwrap();
            fs::write(wal::segment_path(&wal_dir(&copy), 1), &log[..cut]).unwrap();

            let complete = wal::frames(&log[..cut]).count();
            let file: File<Block> = File::open(&copy).unwrap();
            assert_matches(&file, &snapshots[complete]);
            drop(file);
            cleanup(&copy);
        }
        cleanup(&path);
    }

    #[test]
    fn test_write_batch_is_one_record() {
        let path = fresh("test_write_batch_is_one_record");
        {
            let file: File<Block> = File::make(&path, 256).unwrap();
            file.insert(b"b", b"old").unwrap();
            let before = file.wal.last_lsn();
            file.write_batch(&[
                Op::Insert(b"a", b"1"),
                Op::Remove(b"b"),
                Op::Insert(b"c", b"3"),
            ])
            .unwrap();
            assert_eq!(file.wal.last_lsn(), before + 1);
            crash(file);
        }
        let file: File<Block> = File::open(&path).unwrap();
        let model = Model::from([
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]);
        assert_matches(&file, &model);
        drop(file);
        cleanup(&path);
    }

    #[test]
    fn test_removing_missing_key_logs_nothing() {
        let path = fresh("test_removing_missing_key_logs_nothing");
        let file: File<Block> = File::make(&path, 256).unwrap();
        file.insert(b"a", b"1").unwrap();
        let lsn = file.wal.last_lsn();
        file.remove(b"missing").unwrap();
        assert_eq!(file.wal.last_lsn(), lsn);
        assert!(
            file.dirty.read().len() <= 1,
            "only the root, from the insert"
        );
        drop(file);
        cleanup(&path);
    }

    #[test]
    fn test_sync_durability_shares_fsyncs() {
        let path = fresh("test_sync_durability_shares_fsyncs");
        let options = Options {
            durability: Durability::Sync,
            ..Options::default()
        };
        let threads = 8;
        let per_thread = 25;
        {
            let file = Arc::new(File::<Block>::make_with(&path, 256, options.clone()).unwrap());
            let handles = (0..threads)
                .map(|t| {
                    let file = file.clone();
                    std::thread::spawn(move || {
                        for i in 0..per_thread {
                            let key = format!("{t}-{i:03}");
                            file.insert(key.as_bytes(), b"v").unwrap();
                        }
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                handle.join().unwrap();
            }
            let syncs = file.wal.syncs();
            assert!(syncs >= 1);
            assert!(
                syncs < (threads * per_thread) as u64,
                "{syncs} fsyncs for {} commits: none were shared",
                threads * per_thread
            );
            // every commit returned only once fsynced: all of it survives
            crash(Arc::try_unwrap(file).ok().unwrap());
        }
        let file: File<Block> = File::open_with(&path, options).unwrap();
        let mut model = Model::new();
        for t in 0..threads {
            for i in 0..per_thread {
                model.insert(format!("{t}-{i:03}").into_bytes(), b"v".to_vec());
            }
        }
        assert_matches(&file, &model);
        drop(file);
        cleanup(&path);
    }

    #[test]
    fn test_periodic_durability_fsyncs_in_background() {
        let path = fresh("test_periodic_durability_fsyncs_in_background");
        let options = Options {
            durability: Durability::Periodic(std::time::Duration::from_millis(5)),
            ..Options::default()
        };
        let file: File<Block> = File::make_with(&path, 256, options).unwrap();
        file.insert(b"a", b"1").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while file.wal.syncs() == 0 {
            assert!(std::time::Instant::now() < deadline, "never fsynced");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        drop(file);
        cleanup(&path);
    }

    #[test]
    fn test_make_discards_stale_log() {
        let path = fresh("test_make_discards_stale_log");
        {
            let file: File<Block> = File::make(&path, 256).unwrap();
            file.insert(b"stale", b"value").unwrap();
            crash(file);
        }
        fs::remove_file(&path).unwrap();

        let file: File<Block> = File::make(&path, 256).unwrap();
        drop(file);
        let file: File<Block> = File::open(&path).unwrap();
        assert!(file.is_empty());
        drop(file);
        cleanup(&path);
    }

    /// Replacing an overflow value frees the old region. It must stay intact
    /// until a checkpoint: if a power loss takes the replacement's record
    /// from the log, the tree on disk still points at the old region.
    #[test]
    fn test_freed_overflow_survives_until_checkpoint() {
        let path = fresh("test_freed_overflow_survives_until_checkpoint");
        let old = vec![1u8; 2_000];
        let new = vec![2u8; 3_000];
        {
            let file: File<Block> = File::make(&path, 256).unwrap();
            file.insert(b"big", &old).unwrap();
            file.sync().unwrap();
            file.insert(b"big", &new).unwrap();
            assert!(!file.pending_free.lock().is_empty());
            crash(file);
        }

        // lose the replacement's record, as an unsynced write can be lost
        let segments = wal::segments(&wal_dir(&path)).unwrap();
        let (_, segment) = segments.last().unwrap();
        let log = fs::read(segment).unwrap();
        // the checkpoint's layout frame, then the replacement's record
        assert_eq!(wal::frames(&log).count(), 2);
        let (_, first) = wal::decode(&log).unwrap();
        fs::write(segment, &log[..first]).unwrap();

        let file: File<Block> = File::open(&path).unwrap();
        assert_eq!(file.lookup(b"big").unwrap(), Some(old));
        drop(file);
        cleanup(&path);
    }

    /// With a cache of a few dozen pages, checkpoints are frequent and clean
    /// pages are evicted all the time: dirty ones must never reach the file
    /// between checkpoints, or replaying the log on top of them breaks the tree.
    #[test]
    fn test_recovery_with_small_cache() {
        let path = fresh("test_recovery_with_small_cache");
        let options = Options {
            cache_bytes: 32 * 256,
            ..Options::default()
        };
        let mut model = Model::new();
        {
            let file: File<Block> = File::make_with(&path, 256, options.clone()).unwrap();
            workload(&file, &mut model, 5, 1_500);
            crash(file);
        }
        let file: File<Block> = File::open_with(&path, options).unwrap();
        assert_matches(&file, &model);
        drop(file);
        cleanup(&path);
    }

    /// Between checkpoints no page already in the file may change, however
    /// hard the cache is churned: only appended pages are new. A dirty page
    /// evicted and written back in place would break that, and with it what
    /// recovery assumes about the file.
    #[test]
    fn test_no_page_written_in_place_between_checkpoints() {
        let path = fresh("test_no_page_written_in_place_between_checkpoints");
        let options = Options {
            cache_bytes: 32 * 256,
            ..Options::default()
        };
        let file: File<Block> = File::make_with(&path, 256, options).unwrap();
        let mut model = Model::new();
        workload(&file, &mut model, 6, 400);
        file.sync().unwrap();
        let segment = wal::segments(&wal_dir(&path)).unwrap();
        let before = fs::read(&path).unwrap();

        for i in 0..4u8 {
            let key = format!("{:05}", 90_000 + i as u32).into_bytes();
            file.insert(&key, &[i; 40]).unwrap();
            model.insert(key, vec![i; 40]);
            for key in model.keys() {
                file.lookup(key).unwrap();
            }
        }
        assert!(!file.dirty.read().is_empty());
        assert_eq!(
            wal::segments(&wal_dir(&path)).unwrap(),
            segment,
            "a checkpoint ran: the test needs fewer writes"
        );

        let after = fs::read(&path).unwrap();
        assert!(after.len() >= before.len());
        let changed = (0..before.len() / 256)
            .filter(|i| before[i * 256..(i + 1) * 256] != after[i * 256..(i + 1) * 256])
            .count();
        assert_eq!(changed, 0, "{changed} blocks of the file changed in place");

        assert_matches(&file, &model);
        drop(file);
        cleanup(&path);
    }

    // ---- review fixes ----

    /// A replay that fails must leave the log alone, so that a later open can
    /// still recover everything from it.
    #[test]
    fn test_failed_replay_keeps_the_log() {
        let path = fresh("test_failed_replay_keeps_the_log");
        let mut model = Model::new();
        {
            let file: File<Block> = File::make(&path, 256).unwrap();
            workload(&file, &mut model, 7, 300);
            crash(file);
        }

        FAIL_APPLY_AFTER.with(|left| left.set(20));
        assert!(File::<Block>::open(&path).is_err());
        FAIL_APPLY_AFTER.with(|left| left.set(usize::MAX));

        let file: File<Block> = File::open(&path).unwrap();
        assert_matches(&file, &model);
        drop(file);
        cleanup(&path);
    }

    /// Once a commit fails, later writes must fail before changing anything.
    #[test]
    fn test_failed_commit_stops_writes() {
        let path = fresh("test_failed_commit_stops_writes");
        let options = Options {
            durability: Durability::Sync,
            ..Options::default()
        };
        let file: File<Block> = File::make_with(&path, 256, options).unwrap();
        file.insert(b"a", b"1").unwrap();

        file.wal
            .fail_syncs
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(file.insert(b"b", b"2").is_err());
        let lsn = file.wal.last_lsn();
        assert!(file.insert(b"c", b"3").is_err());
        assert_eq!(file.wal.last_lsn(), lsn, "the second write was logged");
        assert!(
            file.lookup(b"a").is_err(),
            "reads go on after a failed write"
        );
        drop(file);
        cleanup(&path);
    }

    #[test]
    fn test_second_open_is_refused() {
        let path = fresh("test_second_open_is_refused");
        let file: File<Block> = File::make(&path, 256).unwrap();
        let err = File::<Block>::open(&path).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        drop(file);
        let file: File<Block> = File::open(&path).unwrap();
        drop(file);
        cleanup(&path);
    }

    /// A write whose checkpoint fails must not come back after a reopen.
    #[test]
    fn test_failed_checkpoint_fails_an_unapplied_write() {
        let path = fresh("test_failed_checkpoint_fails_an_unapplied_write");
        let options = Options {
            checkpoint_dirty_bytes: 2 * 256,
            ..Options::default()
        };
        {
            let file: File<Block> = File::make_with(&path, 256, options).unwrap();
            let mut model = Model::new();
            workload(&file, &mut model, 8, 100);
            file.sync().unwrap();
            // bounded: were the checkpoint to run after each write instead of
            // before it, the dirty count would never get there
            for i in 0..1_000 {
                if file.dirty.read().len() >= file.max_dirty {
                    break;
                }
                file.insert(format!("fill-{i}").as_bytes(), b"v").unwrap();
            }

            file.crash_after_pages
                .store(0, std::sync::atomic::Ordering::Relaxed);
            assert!(file.insert(b"lost", b"v").is_err());
            crash(file);
        }
        let file: File<Block> = File::open(&path).unwrap();
        assert_eq!(file.lookup(b"lost").unwrap(), None);
        drop(file);
        cleanup(&path);
    }

    /// After a batch fails half-way, its first half must not be readable.
    #[test]
    fn test_reads_refused_after_failed_write() {
        let path = fresh("test_reads_refused_after_failed_write");
        let file: File<Block> = File::make(&path, 256).unwrap();
        file.insert(b"c", b"3").unwrap();

        FAIL_APPLY_AFTER.with(|left| left.set(1));
        let result = file.write_batch(&[
            Op::Insert(b"a", b"1"),
            Op::Insert(b"b", b"2"),
            Op::Remove(b"c"),
        ]);
        FAIL_APPLY_AFTER.with(|left| left.set(usize::MAX));
        assert!(result.is_err());

        assert!(file.lookup(b"a").is_err());
        assert!(file.min().is_err());
        assert!(file.above(b"").is_err());
        assert!(!file.is_empty());
        assert!(file.read_root().is_none());
        drop(file);
        cleanup(&path);
    }

    /// Opening and closing without writing must not checkpoint or rotate.
    #[test]
    fn test_idle_close_skips_checkpoint() {
        let path = fresh("test_idle_close_skips_checkpoint");
        let ids = |path: &Path| {
            wal::segments(&wal_dir(path))
                .unwrap()
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>()
        };
        let file: File<Block> = File::make(&path, 256).unwrap();
        file.remove(b"missing").unwrap();
        drop(file);
        assert_eq!(ids(&path), vec![1]);

        let file: File<Block> = File::open(&path).unwrap();
        file.lookup(b"a").unwrap();
        drop(file);
        assert_eq!(ids(&path), vec![2], "open starts a segment, close keeps it");
        cleanup(&path);
    }

    /// `dump` on a tree bigger than the cache must not deadlock on a miss.
    #[test]
    fn test_dump_with_small_cache() {
        let path = fresh("test_dump_with_small_cache");
        let options = Options {
            cache_bytes: 8 * 256,
            ..Options::default()
        };
        let file = Arc::new(File::<Block>::make_with(&path, 256, options).unwrap());
        let mut model = Model::new();
        workload(&file, &mut model, 9, 300);
        file.sync().unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        {
            let file = file.clone();
            std::thread::spawn(move || tx.send(file.dump().len()).unwrap());
        }
        let len = rx
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("dump deadlocked");
        assert!(len > 0);
        drop(Arc::try_unwrap(file).ok().unwrap());
        cleanup(&path);
    }

    // ---- write buffer ----

    fn buffered(records: usize, bytes: usize, interval: Option<std::time::Duration>) -> Options {
        Options {
            write_buffer: WriteBuffer {
                records,
                bytes,
                interval,
            },
            ..Options::default()
        }
    }

    fn keys_present(file: &File<Block>, n: usize) -> Vec<usize> {
        (0..n)
            .filter(|i| {
                file.lookup(format!("key-{i:04}").as_bytes())
                    .unwrap()
                    .is_some()
            })
            .collect()
    }

    /// With a buffer of 100 records, a crash loses what had not reached 100.
    #[test]
    fn test_buffered_records_up_to_the_limit_are_lost_on_crash() {
        let path = fresh("test_buffered_records_lost_on_crash");
        {
            let file: File<Block> =
                File::make_with(&path, 256, buffered(100, 1 << 20, None)).unwrap();
            for i in 0..150 {
                file.insert(format!("key-{i:04}").as_bytes(), b"v").unwrap();
            }
            crash(file);
        }
        let file: File<Block> = File::open(&path).unwrap();
        assert_eq!(keys_present(&file, 150), (0..100).collect::<Vec<_>>());
        drop(file);
        cleanup(&path);
    }

    /// Closing, like a checkpoint or `sync`, writes out whatever is buffered.
    #[test]
    fn test_buffered_records_survive_close() {
        let path = fresh("test_buffered_records_survive_close");
        {
            let file: File<Block> =
                File::make_with(&path, 256, buffered(1000, 1 << 20, None)).unwrap();
            for i in 0..150 {
                file.insert(format!("key-{i:04}").as_bytes(), b"v").unwrap();
            }
        }
        let file: File<Block> = File::open(&path).unwrap();
        assert_eq!(keys_present(&file, 150).len(), 150);
        drop(file);
        cleanup(&path);
    }

    #[test]
    fn test_buffer_interval_writes_in_background() {
        let path = fresh("test_buffer_interval_writes_in_background");
        {
            let options = buffered(1000, 1 << 20, Some(std::time::Duration::from_millis(5)));
            let file: File<Block> = File::make_with(&path, 256, options).unwrap();
            for i in 0..10 {
                file.insert(format!("key-{i:04}").as_bytes(), b"v").unwrap();
            }
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while file.wal.buffered().0 > 0 {
                assert!(std::time::Instant::now() < deadline, "never written");
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            crash(file);
        }
        let file: File<Block> = File::open(&path).unwrap();
        assert_eq!(keys_present(&file, 10).len(), 10);
        drop(file);
        cleanup(&path);
    }

    /// A failed write poisons the database, but the buffered records before
    /// it are of writes that succeeded: closing still writes them.
    #[test]
    fn test_poisoned_close_keeps_buffered_records() {
        let path = fresh("test_poisoned_close_keeps_buffered_records");
        {
            let file: File<Block> =
                File::make_with(&path, 256, buffered(1000, 1 << 20, None)).unwrap();
            for i in 0..10 {
                file.insert(format!("key-{i:04}").as_bytes(), b"v").unwrap();
            }
            FAIL_APPLY_AFTER.with(|left| left.set(0));
            assert!(file.insert(b"failed", b"v").is_err());
            FAIL_APPLY_AFTER.with(|left| left.set(usize::MAX));
        }
        let file: File<Block> = File::open(&path).unwrap();
        assert_eq!(keys_present(&file, 10).len(), 10);
        assert_eq!(file.lookup(b"failed").unwrap(), None);
        drop(file);
        cleanup(&path);
    }

    // ---- page count and free-list trailer ----

    fn free_pages(file: &File<Block>) -> Vec<u32> {
        let mut free = file
            .empty
            .read()
            .iter()
            .map(|Reverse(id)| *id)
            .collect::<Vec<_>>();
        free.extend(file.pending_free.lock().iter().copied());
        free.sort_unstable();
        free
    }

    /// Big values (overflow regions of several pages) and enough removes to
    /// free pages: a close must leave a file the next open reads without a scan.
    #[test]
    fn test_open_reads_free_list_without_scan() {
        let path = fresh("test_open_reads_free_list_without_scan");
        let mut model = Model::new();
        let (page_count, free) = {
            let file: File<Block> = File::make(&path, 256).unwrap();
            workload(&file, &mut model, 10, 3_000);
            file.sync().unwrap();
            (file.page_count(), free_pages(&file))
        };
        assert!(!free.is_empty(), "the workload should leave free pages");

        let file: File<Block> = File::open(&path).unwrap();
        assert!(
            !file.scanned.load(Ordering::Relaxed),
            "open scanned the file"
        );
        assert_eq!(file.page_count(), page_count);
        assert_eq!(free_pages(&file), free);
        let len = fs::metadata(&path).unwrap().len() as usize;
        assert_eq!(
            len,
            HEAD + page_count as usize * 256 + encode_trailer(&free).len(),
            "the file ends right after the trailer"
        );

        // a cleanly closed file needs no log: copied alone, it opens the same
        drop(file);
        fs::remove_dir_all(wal_dir(&path)).unwrap();
        let file: File<Block> = File::open(&path).unwrap();
        assert!(
            !file.scanned.load(Ordering::Relaxed),
            "open without the log scanned"
        );
        assert_eq!(free_pages(&file), free);

        // pages keep coming from the free list and after `page_count`, never
        // from the middle of an overflow region
        workload(&file, &mut model, 11, 3_000);
        drop(file);
        let file: File<Block> = File::open(&path).unwrap();
        assert!(!file.scanned.load(Ordering::Relaxed));
        assert_matches(&file, &model);
        drop(file);
        cleanup(&path);
    }

    /// A file from before the trailer (or with a broken one) is scanned once,
    /// and the next close saves the layout.
    #[test]
    fn test_file_without_trailer_is_scanned_once() {
        let path = fresh("test_file_without_trailer_is_scanned_once");
        let mut model = Model::new();
        let free = {
            let file: File<Block> = File::make(&path, 256).unwrap();
            workload(&file, &mut model, 12, 2_000);
            file.sync().unwrap();
            free_pages(&file)
        };
        // as an older version would have left it: header count 1, no trailer
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        let len = HEAD as u64 + {
            let reopened: File<Block> = File::open(&path).unwrap();
            reopened.page_count() as u64 * 256
        };
        write_at(&file, PAGE_COUNT_OFFSET as u64, &1u32.to_be_bytes()).unwrap();
        file.set_len(len).unwrap();
        drop(file);

        let file: File<Block> = File::open(&path).unwrap();
        assert!(file.scanned.load(Ordering::Relaxed));
        assert_eq!(
            free_pages(&file),
            free,
            "the scan finds the same free pages"
        );
        assert_matches(&file, &model);
        drop(file);

        let file: File<Block> = File::open(&path).unwrap();
        assert!(
            !file.scanned.load(Ordering::Relaxed),
            "the layout was saved"
        );
        drop(file);
        cleanup(&path);
    }

    /// After a crash, pages allocated since the last checkpoint lie beyond its
    /// page count: recovery reuses them instead of leaking them, and the next
    /// checkpoint cuts off what is left.
    #[test]
    fn test_crash_leaves_no_pages_behind() {
        let path = fresh("test_crash_leaves_no_pages_behind");
        let mut model = Model::new();
        let (checkpointed, at_crash) = {
            let file: File<Block> = File::make(&path, 256).unwrap();
            workload(&file, &mut model, 13, 500);
            file.sync().unwrap();
            let checkpointed = file.page_count();
            // big values only: new overflow regions after the checkpoint
            for i in 0..20u32 {
                let key = format!("big-{i}").into_bytes();
                let val = vec![i as u8; 2_000];
                file.insert(&key, &val).unwrap();
                model.insert(key, val);
            }
            let at_crash = file.page_count();
            assert!(at_crash > checkpointed + 100);
            crash(file);
            (checkpointed, at_crash)
        };
        let file: File<Block> = File::open(&path).unwrap();
        assert!(
            !file.scanned.load(Ordering::Relaxed),
            "recovery scanned the file"
        );
        assert_matches(&file, &model);
        // replaying allocates exactly what the lost writes had: from the same
        // page count and free list, the pages past the checkpoint are reused
        assert_eq!(
            file.page_count(),
            at_crash,
            "{checkpointed} pages at the checkpoint"
        );
        drop(file);
        cleanup(&path);
    }
}
