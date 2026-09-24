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

use std::convert::TryInto;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const INSERT: u8 = 1;
const REMOVE: u8 = 2;
const BATCH: u8 = 3;
const PAGE: u8 = 4;
const CHECKPOINT: u8 = 5;

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
    /// and together they hold the tree as of `lsn`.
    Checkpoint {
        lsn: u64,
        pages: u32,
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
        Record::Checkpoint { lsn, pages } => {
            buf.push(CHECKPOINT);
            buf.extend_from_slice(&lsn.to_be_bytes());
            put_u32(buf, *pages);
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
        CHECKPOINT => Record::Checkpoint {
            lsn: r.u64()?,
            pages: r.u32()?,
        },
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

/// CRC-32C (Castagnoli), table-driven.
fn crc32c(data: &[u8]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut crc = i as u32;
            let mut bit = 0;
            while bit < 8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ 0x82F6_3B78
                } else {
                    crc >> 1
                };
                bit += 1;
            }
            table[i] = crc;
            i += 1;
        }
        table
    };
    let mut crc = !0u32;
    for &byte in data {
        crc = TABLE[((crc ^ byte as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
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
    /// Create segment `id` in `dir`, which is created if missing. The segment
    /// and the directory entry pointing at it are synced before this returns,
    /// so that a later `sync_data` of the segment is enough to make its frames
    /// durable.
    pub(crate) fn create(dir: &Path, id: u64) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(segment_path(dir, id))?;
        file.sync_all()?;
        sync_dir(dir)?;
        Ok(Self { id, file })
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Append `frames` (whole encoded frames) and make them durable.
    pub(crate) fn append(&mut self, frames: &[u8]) -> io::Result<()> {
        self.file.write_all(frames)?;
        self.file.sync_data()
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
            (7, Record::Checkpoint { lsn: 5, pages: 1 }),
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
                .append(&encode_all(&records[..id as usize]))
                .unwrap();
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
}
