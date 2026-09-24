//! Write-ahead log: frame format and segment files. See `doc/wal.md`.
//!
//! A WAL is a directory of segments, `{:020}.wal`, each a sequence of frames:
//!
//! ```text
//! [len: u32]   bytes that follow this field
//! [crc: u32]   CRC-32C of everything after this field
//! [lsn: u64]
//! [kind: u8]
//! [payload]
//! ```
//!
//! Integers are big-endian, like the rest of the file format. Reading stops at
//! the first frame that runs past the end of the segment or fails its CRC:
//! that is the torn tail of the last write before a crash, and nothing after it
//! was ever acknowledged.

use log::{debug, error};
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const INSERT: u8 = 1;
const REMOVE: u8 = 2;
const BATCH: u8 = 3;
const PAGE: u8 = 4;
const CHECKPOINT: u8 = 5;

/// Initial capacity of the append buffers, and the most a buffer keeps after
/// a flush.
const BUFFER_BYTES: usize = 64 * 1024;
const KEEP_BUFFER_BYTES: usize = 1024 * 1024;

/// A checkpoint writes its page images out whenever this much is buffered,
/// rather than buffering them all: growing a buffer to the whole dirty set
/// (up to 128 MiB) one doubling at a time, every checkpoint, left the
/// allocator holding gigabytes of freed but still resident memory.
pub(crate) const CHUNK_BYTES: usize = 512 * 1024;

/// Shortest interval of `Durability::Periodic`: less would spin a core.
const MIN_FLUSH_INTERVAL: Duration = Duration::from_millis(1);

/// `len` and `crc`, which precede what the CRC covers.
const PREFIX: usize = 8;
/// `lsn` and `kind`: the smallest possible frame body.
const BODY_HEAD: usize = 9;

/// One key-value operation, as logged and as replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op<'a> {
    Insert(&'a [u8], &'a [u8]),
    Remove(&'a [u8]),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Record<'a> {
    Op(Op<'a>),
    /// Operations applied atomically: replayed all together or not at all.
    Batch(Vec<Op<'a>>),
    /// A page image written by a checkpoint, before the page goes in place.
    Page {
        id: u32,
        bytes: &'a [u8],
    },
    /// Closes a checkpoint: the `pages` frames before it are its page images,
    /// and together they hold the tree as of `lsn`. `page_count` and `free`
    /// describe the file that tree lives in: how many pages it has, and which
    /// of them are free for reuse.
    Checkpoint {
        lsn: u64,
        pages: u32,
        page_count: u32,
        free: Vec<u32>,
    },
}

/// A frame read back from a segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Frame<'a> {
    pub(crate) lsn: u64,
    pub(crate) record: Record<'a>,
}

/// Append the frame for `record` to `buf`.
pub(crate) fn encode(buf: &mut Vec<u8>, lsn: u64, record: &Record<'_>) {
    let start = buf.len();
    buf.extend_from_slice(&[0u8; PREFIX]); // len and crc, filled in below
    buf.extend_from_slice(&lsn.to_be_bytes());
    match record {
        Record::Op(op) => encode_op(buf, op),
        Record::Batch(ops) => {
            buf.push(BATCH);
            put_u32(buf, ops.len() as u32);
            for op in ops {
                encode_op(buf, op);
            }
        }
        Record::Page { id, bytes } => {
            buf.push(PAGE);
            put_u32(buf, *id);
            buf.extend_from_slice(bytes);
        }
        Record::Checkpoint {
            lsn,
            pages,
            page_count,
            free,
        } => {
            buf.push(CHECKPOINT);
            buf.extend_from_slice(&lsn.to_be_bytes());
            put_u32(buf, *pages);
            put_u32(buf, *page_count);
            put_u32(buf, free.len() as u32);
            for id in free {
                put_u32(buf, *id);
            }
        }
    }
    let len = (buf.len() - start - 4) as u32;
    let crc = crc32c(&buf[start + PREFIX..]);
    buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
    buf[start + 4..start + PREFIX].copy_from_slice(&crc.to_be_bytes());
}

fn encode_op(buf: &mut Vec<u8>, op: &Op<'_>) {
    match op {
        Op::Insert(key, val) => {
            buf.push(INSERT);
            put_u32(buf, key.len() as u32);
            put_u32(buf, val.len() as u32);
            buf.extend_from_slice(key);
            buf.extend_from_slice(val);
        }
        Op::Remove(key) => {
            buf.push(REMOVE);
            put_u32(buf, key.len() as u32);
            buf.extend_from_slice(key);
        }
    }
}

/// Decode the frame at the start of `buf`, returning it and its encoded size,
/// or `None` if there is no complete, intact frame there.
pub(crate) fn decode(buf: &[u8]) -> Option<(Frame<'_>, usize)> {
    let mut head = Reader(buf);
    let len = head.u32()? as usize;
    let crc = head.u32()?;
    if len < 4 + BODY_HEAD {
        return None;
    }
    let body = buf.get(PREFIX..4 + len)?;
    if crc32c(body) != crc {
        return None;
    }

    let mut r = Reader(body);
    let lsn = r.u64()?;
    let record = match r.u8()? {
        BATCH => {
            let count = r.u32()? as usize;
            // each op takes at least 5 bytes: do not trust `count` to size the Vec
            let mut ops = Vec::with_capacity(count.min(r.0.len() / 5));
            for _ in 0..count {
                let kind = r.u8()?;
                ops.push(decode_op(kind, &mut r)?);
            }
            Record::Batch(ops)
        }
        PAGE => {
            let id = r.u32()?;
            Record::Page {
                id,
                bytes: r.take(r.0.len())?,
            }
        }
        CHECKPOINT => {
            let lsn = r.u64()?;
            let pages = r.u32()?;
            let page_count = r.u32()?;
            let count = r.u32()? as usize;
            let mut free = Vec::with_capacity(count.min(r.0.len() / 4));
            for _ in 0..count {
                free.push(r.u32()?);
            }
            Record::Checkpoint {
                lsn,
                pages,
                page_count,
                free,
            }
        }
        kind => Record::Op(decode_op(kind, &mut r)?),
    };
    if !r.0.is_empty() {
        return None;
    }
    Some((Frame { lsn, record }, 4 + len))
}

fn decode_op<'a>(kind: u8, r: &mut Reader<'a>) -> Option<Op<'a>> {
    match kind {
        INSERT => {
            let klen = r.u32()? as usize;
            let vlen = r.u32()? as usize;
            Some(Op::Insert(r.take(klen)?, r.take(vlen)?))
        }
        REMOVE => {
            let klen = r.u32()? as usize;
            Some(Op::Remove(r.take(klen)?))
        }
        _ => None,
    }
}

/// The intact frames at the start of `buf`, in order, up to the first torn or
/// corrupted one.
#[cfg(test)]
pub(crate) fn frames(buf: &[u8]) -> impl Iterator<Item = Frame<'_>> {
    let mut pos = 0;
    std::iter::from_fn(move || {
        let (frame, len) = decode(&buf[pos..])?;
        pos += len;
        Some(frame)
    })
}

/// Length of the intact prefix of `buf`: where the next frame should go when
/// appending to a segment whose tail may be torn.
#[cfg(test)]
pub(crate) fn valid_len(buf: &[u8]) -> usize {
    let mut pos = 0;
    while let Some((_, len)) = decode(&buf[pos..]) {
        pos += len;
    }
    pos
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if n > self.0.len() {
            return None;
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Some(head)
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }
}

fn put_u32(buf: &mut Vec<u8>, val: u32) {
    buf.extend_from_slice(&val.to_be_bytes());
}

/// CRC-32C (Castagnoli), with the CPU's CRC instructions where there are any.
///
/// Every page image a checkpoint logs goes through this, so a byte-at-a-time
/// table was the single biggest cost of a checkpoint.
pub(crate) fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// The segments in `dir`, ordered by id. A missing directory has none.
pub(crate) fn segments(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e),
    };
    let mut segments = vec![];
    for entry in entries {
        let path = entry?.path();
        let id = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(".wal"))
            .and_then(|id| id.parse::<u64>().ok());
        if let Some(id) = id {
            segments.push((id, path));
        }
    }
    segments.sort_unstable_by_key(|(id, _)| *id);
    Ok(segments)
}

pub(crate) fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}.wal"))
}

/// The segment being appended to.
pub(crate) struct Segment {
    id: u64,
    file: fs::File,
}

impl Segment {
    /// Create segment `id` in `dir`, which is created if missing.
    ///
    /// The caller syncs `dir` before relying on the segment: `sync_data` of
    /// the segment makes its contents durable, but not the entry naming it.
    pub(crate) fn create(dir: &Path, id: u64) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(segment_path(dir, id))?;
        Ok(Self { id, file })
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Append `frames`, whole encoded frames, handing them to the OS.
    pub(crate) fn write_all(&mut self, frames: &[u8]) -> io::Result<()> {
        self.file.write_all(frames)
    }

    /// Make everything written so far durable.
    pub(crate) fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
}

/// What `open` has to do to bring a database back to its last committed state.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Plan<'a> {
    /// Page images of the last complete checkpoint, to write in place.
    pub(crate) images: Vec<(u32, &'a [u8])>,
    /// Operations logged after that checkpoint, to replay in order; each
    /// entry is one record, applied atomically.
    pub(crate) ops: Vec<Vec<Op<'a>>>,
    /// The highest LSN seen, so that new records continue after it.
    pub(crate) last_lsn: u64,
    /// Page count and free pages of the file as of that checkpoint, if any:
    /// they go with the images, and the header on disk may be older.
    pub(crate) layout: Option<(u32, Vec<u32>)>,
}

/// Work out recovery from the contents of the segments, in segment order.
///
/// Reading stops at the first torn or corrupted frame, and ignores every
/// segment after it: a segment is only started by a checkpoint, after the
/// previous one was fsynced whole, so a later segment cannot legitimately
/// follow a torn one.
pub(crate) fn recovery_plan(segments: &[Vec<u8>]) -> Plan<'_> {
    let mut all = vec![];
    for buf in segments {
        let mut pos = 0;
        while let Some((frame, len)) = decode(&buf[pos..]) {
            pos += len;
            all.push(frame);
        }
        if pos < buf.len() {
            break;
        }
    }

    // The last CHECKPOINT whose page images all made it: the `pages` frames
    // right before it must be exactly its PAGE frames.
    let mut start = 0;
    let mut images = vec![];
    let mut checkpoint_lsn = 0;
    let mut layout = None;
    for (idx, frame) in all.iter().enumerate() {
        if let Record::Checkpoint {
            lsn,
            pages,
            page_count,
            free,
        } = &frame.record
        {
            let (lsn, pages) = (*lsn, *pages as usize);
            let complete = idx >= pages
                && all[idx - pages..idx]
                    .iter()
                    .all(|frame| matches!(frame.record, Record::Page { .. }));
            if complete {
                images = all[idx - pages..idx]
                    .iter()
                    .filter_map(|frame| match frame.record {
                        Record::Page { id, bytes } => Some((id, bytes)),
                        _ => None,
                    })
                    .collect();
                start = idx + 1;
                checkpoint_lsn = lsn;
                layout = Some((*page_count, free.clone()));
            }
        }
    }

    let ops = all[start..]
        .iter()
        .filter(|frame| frame.lsn > checkpoint_lsn)
        .filter_map(|frame| match &frame.record {
            Record::Op(op) => Some(vec![*op]),
            Record::Batch(ops) => Some(ops.clone()),
            // images of a checkpoint that did not complete
            Record::Page { .. } | Record::Checkpoint { .. } => None,
        })
        .collect();

    Plan {
        images,
        ops,
        last_lsn: all.iter().map(|frame| frame.lsn).max().unwrap_or(0),
        layout,
    }
}

struct State {
    /// Frames appended since the last write, and a spare buffer swapped in
    /// when a leader takes `buf`, so that neither is reallocated every time.
    buf: Vec<u8>,
    spare: Vec<u8>,
    next_lsn: u64,
    /// LSN of the last record appended, written to the OS, and fsynced.
    appended: u64,
    written: u64,
    synced: u64,
    /// A leader is writing or syncing: others wait for it instead of starting their own.
    flushing: bool,
    /// A write or sync failed. The segment may now end in a torn frame, and
    /// frames appended after it would be unreachable on recovery, so the log
    /// refuses everything from here on.
    failed: bool,
    /// Bytes appended to the current segment, buffered or not.
    segment_bytes: u64,
}

/// What `Wal::commit` waits for.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Commit {
    /// Wait for the fsync; the other fields then do not apply.
    pub(crate) sync: bool,
    /// Otherwise, write to the OS once this many records wait in memory ...
    pub(crate) records: u64,
    /// ... or once they take this many bytes.
    pub(crate) bytes: usize,
}

impl Commit {
    /// Every commit waits for its fsync.
    pub(crate) const SYNC: Commit = Commit {
        sync: true,
        records: 1,
        bytes: 0,
    };
    /// Every commit waits for its write to the OS.
    #[cfg(test)]
    pub(crate) const WRITE: Commit = Commit {
        sync: false,
        records: 1,
        bytes: 0,
    };
}

/// The write-ahead log of an open database, with group commit.
///
/// Writers `append` a record while holding the database's operation lock,
/// which keeps log order the same as apply order, then release that lock and
/// wait in `flush` for their record to be written (and, if asked, fsynced).
/// The first to arrive while nothing is in flight becomes the leader: it takes
/// everything appended so far and writes it with a single write and a single
/// fsync, on behalf of every writer waiting at that moment.
pub(crate) struct Wal {
    dir: PathBuf,
    commit: Commit,
    state: Mutex<State>,
    cond: Condvar,
    /// Only ever locked by a leader or by `rotate`, so never contended by writers.
    segment: Mutex<Segment>,
    syncs: AtomicU64,
    /// Test hook: fail every fsync.
    #[cfg(test)]
    pub(crate) fail_syncs: AtomicBool,
}

impl Wal {
    /// Start a log in `dir` at a new segment `segment_id`, numbering records
    /// from `next_lsn`, with `commit` deciding what a commit waits for.
    pub(crate) fn create(
        dir: &Path,
        segment_id: u64,
        next_lsn: u64,
        commit: Commit,
    ) -> io::Result<Self> {
        let segment = Segment::create(dir, segment_id)?;
        sync_dir(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            commit: Commit {
                records: commit.records.max(1),
                ..commit
            },
            state: Mutex::new(State {
                buf: Vec::with_capacity(BUFFER_BYTES),
                spare: Vec::with_capacity(BUFFER_BYTES),
                next_lsn,
                appended: next_lsn - 1,
                written: next_lsn - 1,
                synced: next_lsn - 1,
                flushing: false,
                failed: false,
                segment_bytes: 0,
            }),
            cond: Condvar::new(),
            segment: Mutex::new(segment),
            syncs: AtomicU64::new(0),
            #[cfg(test)]
            fail_syncs: AtomicBool::new(false),
        })
    }

    /// Buffer `record` and return its LSN. Nothing is written until a `flush`.
    pub(crate) fn append(&self, record: &Record<'_>) -> u64 {
        let mut st = self.state.lock();
        let lsn = st.next_lsn;
        st.next_lsn += 1;
        let before = st.buf.len();
        encode(&mut st.buf, lsn, record);
        st.segment_bytes += (st.buf.len() - before) as u64;
        st.appended = lsn;
        lsn
    }

    /// Wait until record `lsn` is as durable as this log promises on commit:
    /// fsynced, written to the OS, or -- while the buffer limits allow --
    /// merely buffered.
    pub(crate) fn commit(&self, lsn: u64) -> io::Result<()> {
        // With a buffer of one record every commit writes: skip the check.
        if !self.commit.sync && self.commit.records > 1 {
            let st = self.state.lock();
            if st.written >= lsn {
                return Ok(());
            }
            let waiting = st.appended - st.written;
            if !st.failed && waiting < self.commit.records && st.buf.len() < self.commit.bytes {
                return Ok(());
            }
        }
        self.flush(lsn, self.commit.sync)
    }

    /// Bytes appended but not yet written to the OS.
    pub(crate) fn buffered_bytes(&self) -> usize {
        self.state.lock().buf.len()
    }

    /// Records and bytes appended but not yet written to the OS.
    #[cfg(test)]
    pub(crate) fn buffered(&self) -> (u64, usize) {
        let st = self.state.lock();
        (st.appended - st.written, st.buf.len())
    }

    /// Write, and if `sync` also fsync, everything appended so far.
    pub(crate) fn flush_all(&self, sync: bool) -> io::Result<()> {
        let lsn = self.state.lock().appended;
        self.flush(lsn, sync)
    }

    /// Wait until record `lsn` is written, or fsynced if `sync`, leading the
    /// flush if no one else is.
    pub(crate) fn flush(&self, lsn: u64, sync: bool) -> io::Result<()> {
        let mut st = self.state.lock();
        loop {
            let done = if sync { st.synced } else { st.written };
            if done >= lsn {
                return Ok(());
            }
            if st.failed {
                return Err(io::Error::other(
                    "the write-ahead log failed earlier and accepts no more writes",
                ));
            }
            if st.flushing {
                self.cond.wait(&mut st);
                continue;
            }

            // Lead: take everything appended so far, write it with the lock released.
            st.flushing = true;
            let upto = st.appended;
            let spare = std::mem::take(&mut st.spare);
            let mut batch = std::mem::replace(&mut st.buf, spare);
            let res = MutexGuard::unlocked(&mut st, || -> io::Result<()> {
                let mut segment = self.segment.lock();
                if !batch.is_empty() {
                    segment.write_all(&batch)?;
                }
                if sync {
                    #[cfg(test)]
                    if self.fail_syncs.load(Ordering::Relaxed) {
                        return Err(io::Error::other("Injected fsync failure"));
                    }
                    segment.sync()?;
                    self.syncs.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            });
            batch.clear();
            if batch.capacity() > KEEP_BUFFER_BYTES {
                batch = Vec::with_capacity(BUFFER_BYTES);
            }
            st.spare = batch;
            st.flushing = false;
            self.cond.notify_all();
            match res {
                Ok(()) => {
                    st.written = upto;
                    if sync {
                        st.synced = upto;
                    }
                }
                Err(e) => {
                    error!("WAL write failed: {e}");
                    st.failed = true;
                    return Err(e);
                }
            }
        }
    }

    /// LSN of the last record appended.
    pub(crate) fn last_lsn(&self) -> u64 {
        self.state.lock().appended
    }

    /// Bytes appended to the current segment.
    pub(crate) fn segment_bytes(&self) -> u64 {
        self.state.lock().segment_bytes
    }

    /// Number of fsyncs of the log so far.
    #[cfg(test)]
    pub(crate) fn syncs(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    /// Start the next segment and delete the ones before it, with a single
    /// directory sync for both.
    ///
    /// Only for a checkpoint, once it has made the tree file durable: nothing
    /// in the older segments is needed after that, so losing them to a crash
    /// before the sync is harmless. The caller must have flushed everything
    /// appended, and must keep new appends out meanwhile.
    pub(crate) fn rotate(&self) -> io::Result<()> {
        let old = {
            let mut segment = self.segment.lock();
            let old = segment.id();
            *segment = Segment::create(&self.dir, old + 1)?;
            old
        };
        self.state.lock().segment_bytes = 0;
        for (id, path) in segments(&self.dir)? {
            if id <= old {
                fs::remove_file(path)?;
            }
        }
        sync_dir(&self.dir)?;
        debug!("WAL rotated to segment {}", old + 1);
        Ok(())
    }
}

/// Background thread that writes the log to the OS every `write_every`
/// (`WriteBuffer::interval`) and fsyncs it every `sync_every`
/// (`Durability::Periodic`).
pub(crate) struct Flusher {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Flusher {
    pub(crate) fn start(
        wal: Arc<Wal>,
        write_every: Option<Duration>,
        sync_every: Option<Duration>,
    ) -> Self {
        let write_every = write_every.map(|every| every.max(MIN_FLUSH_INTERVAL));
        let sync_every = sync_every.map(|every| every.max(MIN_FLUSH_INTERVAL));
        let tick = match (write_every, sync_every) {
            (Some(write), Some(sync)) => write.min(sync),
            (write, sync) => write.or(sync).unwrap_or(Duration::from_secs(1)),
        };
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = stop.clone();
            thread::spawn(move || {
                let mut last_sync = std::time::Instant::now();
                while !stop.load(Ordering::Acquire) {
                    thread::park_timeout(tick);
                    let sync = sync_every.is_some_and(|every| last_sync.elapsed() >= every);
                    if sync {
                        last_sync = std::time::Instant::now();
                    } else if write_every.is_none() {
                        continue;
                    }
                    if let Err(e) = wal.flush_all(sync) {
                        error!("WAL flusher stopped: {e}");
                        break;
                    }
                }
            })
        };
        Self {
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            handle.thread().unpark();
            let _ = handle.join();
        }
    }
}

/// Sync a directory, making the creation or removal of entries in it durable.
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint(lsn: u64, pages: u32) -> Record<'static> {
        Record::Checkpoint {
            lsn,
            pages,
            page_count: 40 + lsn as u32,
            free: vec![3, 5, 8],
        }
    }

    fn sample() -> Vec<(u64, Record<'static>)> {
        vec![
            (1, Record::Op(Op::Insert(b"key", b"value"))),
            (2, Record::Op(Op::Remove(b"key"))),
            (3, Record::Op(Op::Insert(b"", b""))),
            (
                4,
                Record::Batch(vec![
                    Op::Insert(b"a", b"1"),
                    Op::Remove(b"b"),
                    Op::Insert(b"c", &[0xFF; 300]),
                ]),
            ),
            (5, Record::Batch(vec![])),
            (
                6,
                Record::Page {
                    id: 42,
                    bytes: &[7u8; 256],
                },
            ),
            (7, checkpoint(5, 1)),
        ]
    }

    fn encode_all(records: &[(u64, Record<'_>)]) -> Vec<u8> {
        let mut buf = vec![];
        for (lsn, record) in records {
            encode(&mut buf, *lsn, record);
        }
        buf
    }

    #[test]
    fn test_crc32c() {
        // the check value from the CRC catalogue
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn test_roundtrip() {
        let records = sample();
        let buf = encode_all(&records);
        let decoded = frames(&buf)
            .map(|frame| (frame.lsn, frame.record))
            .collect::<Vec<_>>();
        assert_eq!(decoded, records);
        assert_eq!(valid_len(&buf), buf.len());
    }

    /// A crash can cut the last write at any byte: every cut must yield
    /// exactly the frames that were complete before it, and nothing else.
    #[test]
    fn test_torn_tail_at_every_offset() {
        let records = sample();
        let buf = encode_all(&records);
        let mut ends = vec![];
        let mut pos = 0;
        while let Some((_, len)) = decode(&buf[pos..]) {
            pos += len;
            ends.push(pos);
        }

        for cut in 0..buf.len() {
            let complete = ends.iter().filter(|&&end| end <= cut).count();
            assert_eq!(frames(&buf[..cut]).count(), complete, "cut={cut}");
            assert_eq!(
                valid_len(&buf[..cut]),
                ends.get(complete.wrapping_sub(1)).copied().unwrap_or(0),
                "cut={cut}"
            );
        }
    }

    /// Flipping any single bit of a frame must make it unreadable.
    #[test]
    fn test_corruption_is_detected() {
        let mut buf = vec![];
        encode(&mut buf, 9, &Record::Op(Op::Insert(b"key", b"value")));
        for byte in 0..buf.len() {
            for bit in 0..8 {
                let mut bad = buf.clone();
                bad[byte] ^= 1 << bit;
                assert!(decode(&bad).is_none(), "byte={} bit={}", byte, bit);
            }
        }
    }

    #[test]
    fn test_garbage_after_valid_frames_is_ignored() {
        let records = sample();
        let mut buf = encode_all(&records);
        let len = buf.len();
        buf.extend_from_slice(&[0u8; 64]);
        buf.extend_from_slice(&[0xFFu8; 64]);
        assert_eq!(frames(&buf).count(), records.len());
        assert_eq!(valid_len(&buf), len);
    }

    #[test]
    fn test_segments() {
        let dir = PathBuf::from("target/test_wal_segments");
        let _ = fs::remove_dir_all(&dir);
        assert!(segments(&dir).unwrap().is_empty());

        let records = sample();
        for id in [3u64, 1, 2] {
            let mut segment = Segment::create(&dir, id).unwrap();
            assert_eq!(segment.id(), id);
            segment
                .write_all(&encode_all(&records[..id as usize]))
                .unwrap();
            segment.sync().unwrap();
        }
        fs::write(dir.join("notes.txt"), b"not a segment").unwrap();
        assert!(
            Segment::create(&dir, 2).is_err(),
            "segments are never reused"
        );

        let found = segments(&dir).unwrap();
        assert_eq!(
            found.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        for (id, path) in found {
            let buf = fs::read(path).unwrap();
            assert_eq!(frames(&buf).count(), id as usize);
        }

        fs::remove_dir_all(&dir).unwrap();
    }

    fn insert(key: &'static [u8]) -> Record<'static> {
        Record::Op(Op::Insert(key, b"v"))
    }

    fn page(id: u32) -> Record<'static> {
        Record::Page {
            id,
            bytes: &[1u8; 16],
        }
    }

    #[test]
    fn test_recovery_plan_without_checkpoint() {
        let seg = encode_all(&[(1, insert(b"a")), (2, Record::Op(Op::Remove(b"a")))]);
        let plan = recovery_plan(std::slice::from_ref(&seg));
        assert!(plan.images.is_empty());
        assert_eq!(
            plan.ops,
            vec![vec![Op::Insert(b"a", b"v")], vec![Op::Remove(b"a")]]
        );
        assert_eq!(plan.last_lsn, 2);
    }

    #[test]
    fn test_recovery_plan_replays_after_last_complete_checkpoint() {
        let seg = encode_all(&[
            (1, insert(b"a")),
            (2, page(7)),
            (3, checkpoint(1, 1)),
            (4, insert(b"b")),
            (5, page(8)),
            (6, page(9)),
            (7, checkpoint(4, 2)),
            (8, insert(b"c")),
            (9, Record::Batch(vec![Op::Remove(b"b")])),
        ]);
        let plan = recovery_plan(std::slice::from_ref(&seg));
        assert_eq!(
            plan.images.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![8, 9]
        );
        assert_eq!(
            plan.ops,
            vec![vec![Op::Insert(b"c", b"v")], vec![Op::Remove(b"b")]]
        );
        assert_eq!(plan.last_lsn, 9);
        assert_eq!(
            plan.layout,
            Some((44, vec![3, 5, 8])),
            "the last checkpoint's"
        );
    }

    /// A crash while a checkpoint appends its images leaves no CHECKPOINT:
    /// the partial images are ignored and the earlier records replayed.
    #[test]
    fn test_recovery_plan_ignores_incomplete_checkpoint() {
        let mut seg = encode_all(&[(1, insert(b"a")), (2, page(3)), (3, page(4))]);
        let whole = seg.len();
        encode(&mut seg, 4, &checkpoint(1, 2));
        seg.truncate(whole + 5); // torn CHECKPOINT
        let plan = recovery_plan(std::slice::from_ref(&seg));
        assert!(plan.images.is_empty());
        assert_eq!(plan.ops, vec![vec![Op::Insert(b"a", b"v")]]);

        // a CHECKPOINT claiming more images than precede it is not complete either
        let seg = encode_all(&[(1, insert(b"a")), (2, page(3)), (3, checkpoint(1, 2))]);
        let plan = recovery_plan(std::slice::from_ref(&seg));
        assert!(plan.images.is_empty());
        assert_eq!(plan.ops, vec![vec![Op::Insert(b"a", b"v")]]);
    }

    #[test]
    fn test_recovery_plan_across_segments_stops_at_torn_one() {
        let one = encode_all(&[(1, insert(b"a")), (2, insert(b"b"))]);
        let mut two = encode_all(&[(3, insert(b"c"))]);
        two.push(0xFF); // torn tail
        let three = encode_all(&[(4, insert(b"d"))]);
        let segments = [one, two, three];
        let plan = recovery_plan(&segments);
        assert_eq!(plan.ops.len(), 3, "the segment after a torn one is ignored");
        assert_eq!(plan.last_lsn, 3);
    }

    #[test]
    fn test_group_commit() {
        let dir = PathBuf::from("target/test_wal_group_commit");
        let _ = fs::remove_dir_all(&dir);
        let wal = Arc::new(Wal::create(&dir, 1, 1, Commit::SYNC).unwrap());

        let threads = 8;
        let per_thread = 50;
        let handles = (0..threads)
            .map(|_| {
                let wal = wal.clone();
                thread::spawn(move || {
                    for _ in 0..per_thread {
                        let lsn = wal.append(&insert(b"key"));
                        wal.commit(lsn).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap();
        }

        let total = (threads * per_thread) as u64;
        assert_eq!(wal.last_lsn(), total);
        assert!(
            wal.syncs() < total,
            "{} fsyncs for {} commits: none were shared",
            wal.syncs(),
            total
        );

        let buf = fs::read(segment_path(&dir, 1)).unwrap();
        assert_eq!(frames(&buf).count() as u64, total);
        let lsns = frames(&buf).map(|frame| frame.lsn).collect::<Vec<_>>();
        assert_eq!(lsns, (1..=total).collect::<Vec<_>>(), "in LSN order");

        wal.rotate().unwrap();
        assert_eq!(
            segments(&dir)
                .unwrap()
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(wal.segment_bytes(), 0);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_buffers_shrink_after_big_flush() {
        let dir = PathBuf::from("target/test_wal_buffers_shrink");
        let _ = fs::remove_dir_all(&dir);
        let wal = Wal::create(&dir, 1, 1, Commit::WRITE).unwrap();
        let big = vec![0u8; 4 * 1024 * 1024];
        wal.append(&Record::Page { id: 1, bytes: &big });
        wal.flush_all(false).unwrap();
        wal.append(&insert(b"k"));
        wal.flush_all(false).unwrap();
        let st = wal.state.lock();
        assert!(st.buf.capacity() <= KEEP_BUFFER_BYTES);
        assert!(st.spare.capacity() <= KEEP_BUFFER_BYTES);
        drop(st);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_commit_buffers_up_to_the_limits() {
        let dir = PathBuf::from("target/test_wal_commit_buffers");
        let _ = fs::remove_dir_all(&dir);
        let commit = Commit {
            sync: false,
            records: 3,
            bytes: 1024 * 1024,
        };
        let wal = Wal::create(&dir, 1, 1, commit).unwrap();
        for _ in 0..2 {
            let lsn = wal.append(&insert(b"k"));
            wal.commit(lsn).unwrap();
        }
        assert_eq!(wal.buffered().0, 2, "two records fit the limit of three");
        let lsn = wal.append(&insert(b"k"));
        wal.commit(lsn).unwrap();
        assert_eq!(wal.buffered(), (0, 0), "the third one writes them all");

        let bytes = Commit {
            sync: false,
            records: 1000,
            bytes: 100,
        };
        let dir2 = PathBuf::from("target/test_wal_commit_buffers_bytes");
        let _ = fs::remove_dir_all(&dir2);
        let wal = Wal::create(&dir2, 1, 1, bytes).unwrap();
        for _ in 0..20 {
            let lsn = wal.append(&insert(b"k"));
            wal.commit(lsn).unwrap();
            assert!(
                wal.buffered().1 < 100,
                "never more than the byte limit waits"
            );
        }
        fs::remove_dir_all(&dir).unwrap();
        fs::remove_dir_all(&dir2).unwrap();
    }
}
