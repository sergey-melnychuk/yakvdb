use crate::api::page::{Page, Slot};
use crate::util::bsearch::bsearch;
use bytes::{BufMut, BytesMut};
use std::mem::size_of;
use std::sync::atomic::{AtomicU32, Ordering};

pub struct Block {
    buf: BytesMut,
    /// Offset of the lowest key/value byte in the page (`cap` when empty), or
    /// `UNKNOWN` when it has to be recomputed from the slots.
    ///
    /// Key/value data grows down from the end of the page and is kept packed,
    /// so this is where the next entry goes and, with the slot count, all that
    /// `free` needs. Caching it turns `free`, `fits` and `full` from a scan of
    /// every slot into arithmetic. It is not stored on disk, so the file format
    /// is unchanged: a freshly loaded page starts at `UNKNOWN`.
    ///
    /// Atomic only so that `free(&self)` can fill it in lazily while `Block`
    /// stays `Sync`; every write that changes it goes through `&mut self`.
    lo: AtomicU32,
}

const UNKNOWN: u32 = 0;

impl AsMut<[u8]> for Block {
    fn as_mut(&mut self) -> &mut [u8] {
        // the caller may rewrite anything, slots included
        *self.lo.get_mut() = UNKNOWN;
        &mut self.buf[..]
    }
}

impl AsRef<[u8]> for Block {
    fn as_ref(&self) -> &[u8] {
        &self.buf[..]
    }
}

const ID_OFFSET: usize = 0;
const CAP_OFFSET: usize = 4;
const SIZE_OFFSET: usize = 8;
const RESERVED: u32 = 0xC0DEFACE;

impl Block {
    fn new(buf: BytesMut, lo: u32) -> Self {
        Self {
            buf,
            lo: AtomicU32::new(lo),
        }
    }

    /// See `lo`: the offset of the lowest data byte, computed on first use.
    fn data_lo(&self) -> u32 {
        let lo = self.lo.load(Ordering::Relaxed);
        if lo != UNKNOWN {
            return lo;
        }
        let lo = (0..self.len())
            .map(|idx| get_u32(&self.buf, slot_pos(idx)))
            .min()
            .unwrap_or_else(|| self.cap());
        self.lo.store(lo, Ordering::Relaxed);
        lo
    }

    /// Insert an entry keeping slots ordered by key, replacing an equal key.
    ///
    /// `val` is the bytes stored inline after the key, while `vlen` is the raw
    /// slot field: the same as `val.len()` for a regular entry, but carrying
    /// `OVERFLOW_FLAG | multiplier` for an overflow one, whose value is elsewhere.
    fn put_entry(&mut self, key: &[u8], val: &[u8], vlen: u32, page: u32) -> Option<u32> {
        let len = (key.len() + val.len()) as u32;
        if !self.fits(len) {
            return None;
        }

        let n = self.len();
        let idx = self.ceil(key).unwrap_or(n);
        if idx < n && self.key(idx) == key {
            // `idx` is still where the new entry belongs once the old one is gone
            self.remove(idx);
        }

        let n = self.len();
        let offset = self.data_lo() - len;

        // `fits` guaranteed one more slot does not run into the data
        let at = slot_pos(idx);
        self.buf.copy_within(at..slot_pos(n), at + SLOT);
        put_slot(
            &mut self.buf,
            idx,
            &Slot::new(offset, key.len() as u32, vlen, page),
        );
        put_size(&mut self.buf, n + 1);

        put_slice(&mut self.buf, offset as usize, key);
        put_slice(&mut self.buf, offset as usize + key.len(), val);
        *self.lo.get_mut() = offset;

        Some(idx)
    }
}

impl Page for Block {
    fn reserve(capacity: u32) -> Self {
        Self::new(BytesMut::zeroed(capacity as usize), UNKNOWN)
    }

    fn create(id: u32, cap: u32) -> Self {
        let mut buf = BytesMut::zeroed(cap as usize);
        put_head(&mut buf, id, cap);
        Self::new(buf, cap)
    }

    fn id(&self) -> u32 {
        get_u32(&self.buf, ID_OFFSET)
    }

    fn cap(&self) -> u32 {
        get_u32(&self.buf, CAP_OFFSET)
    }

    fn len(&self) -> u32 {
        get_u32(&self.buf, SIZE_OFFSET)
    }

    fn slot(&self, idx: u32) -> Option<Slot> {
        if idx >= self.len() {
            return None;
        }
        let pos = slot_pos(idx);
        let offset = get_u32(&self.buf, pos);
        let klen = get_u32(&self.buf, pos + 4);
        let vlen = get_u32(&self.buf, pos + 8);
        let page = get_u32(&self.buf, pos + 12);
        Some(Slot::new(offset, klen, vlen, page))
    }

    fn min(&self) -> &[u8] {
        self.key(0)
    }

    fn max(&self) -> &[u8] {
        self.key(self.len() - 1)
    }

    fn key(&self, idx: u32) -> &[u8] {
        self.slot(idx)
            .map(|slot| {
                let at = slot.offset as usize;
                let to = at + slot.klen as usize;
                &self.buf[at..to]
            })
            .unwrap_or_default()
    }

    fn val(&self, idx: u32) -> &[u8] {
        self.slot(idx)
            .map(|slot| {
                let vlen = slot.inline_vlen();
                let at = slot.offset as usize + slot.klen as usize;
                let to = at + vlen as usize;
                &self.buf[at..to]
            })
            .unwrap_or_default()
    }

    fn free(&self) -> u32 {
        let slots_end = slot_pos(self.len()) as u32;
        let data_lo = self.data_lo();
        assert!(slots_end <= data_lo);
        data_lo - slots_end
    }

    fn full(&self) -> u8 {
        let len = self.cap() - HEAD as u32;
        ((len - self.free()) * 100 / len) as u8
    }

    fn fits(&self, len: u32) -> bool {
        self.free() >= len + SLOT as u32
    }

    fn find(&self, key: &[u8]) -> Option<u32> {
        let n = self.len();
        if n == 0 {
            return None;
        }

        let k = bsearch(key, 0, n - 1, |i| self.key(i));
        if self.key(k) == key { Some(k) } else { None }
    }

    fn ceil(&self, key: &[u8]) -> Option<u32> {
        let n = self.len();
        if n == 0 {
            return None;
        }

        let k = bsearch(key, 0, n - 1, |i| self.key(i));
        if self.key(k) >= key { Some(k) } else { None }
    }

    fn put_val(&mut self, key: &[u8], val: &[u8]) -> Option<u32> {
        self.put_entry(key, val, val.len() as u32, 0)
    }

    fn put_ref(&mut self, key: &[u8], page: u32) -> Option<u32> {
        self.put_entry(key, &[], 0, page)
    }

    fn put_overflow(&mut self, key: &[u8], overflow_page: u32, raw_vlen: u32) -> Option<u32> {
        // Overflow entries store only the key inline (no value bytes).
        // The vlen field carries OVERFLOW_FLAG | multiplier.
        self.put_entry(key, &[], raw_vlen, overflow_page)
    }

    fn remove(&mut self, idx: u32) {
        let n = self.len();
        if idx >= n {
            return;
        }

        let removed = self.slot(idx).unwrap();
        let off = removed.offset;
        let len = removed.klen + removed.inline_vlen();
        let lo = self.data_lo();

        if len > 0 {
            // Close the gap: everything stored below the removed entry moves up
            // by its length, so the data stays packed against the end of the page.
            self.buf
                .copy_within(lo as usize..off as usize, (lo + len) as usize);
            self.buf[lo as usize..(lo + len) as usize].fill(0);
            for i in 0..n {
                let pos = slot_pos(i);
                let offset = get_u32(&self.buf, pos);
                if i != idx && offset < off {
                    put_u32(&mut self.buf, pos, offset + len);
                }
            }
        }

        let at = slot_pos(idx);
        let end = slot_pos(n);
        self.buf.copy_within(at + SLOT..end, at);
        self.buf[end - SLOT..end].fill(0);
        put_size(&mut self.buf, n - 1);

        *self.lo.get_mut() = if n == 1 { self.cap() } else { lo + len };
    }

    fn extend_from(&mut self, other: &Self, range: std::ops::Range<u32>) -> bool {
        range.into_iter().all(|idx| match other.slot(idx) {
            Some(slot) => self
                .put_entry(other.key(idx), other.val(idx), slot.vlen, slot.page)
                .is_some(),
            None => false,
        })
    }

    fn truncate(&mut self, len: u32) {
        let n = self.len();
        if len >= n {
            return;
        }
        // Repack the kept entries against the end of the page, reading them
        // from a copy: their old places may overlap the new ones.
        let old = self.buf.clone();
        let cap = self.cap();
        let mut offset = cap;
        for idx in 0..len {
            let pos = slot_pos(idx);
            let at = get_u32(&old, pos) as usize;
            let klen = get_u32(&old, pos + 4);
            let vlen = get_u32(&old, pos + 8);
            let slot = Slot::new(at as u32, klen, vlen, 0);
            let bytes = (klen + slot.inline_vlen()) as usize;
            offset -= bytes as u32;
            self.buf[offset as usize..offset as usize + bytes]
                .copy_from_slice(&old[at..at + bytes]);
            put_u32(&mut self.buf, pos, offset);
        }
        self.buf[slot_pos(len)..offset as usize].fill(0);
        put_size(&mut self.buf, len);
        *self.lo.get_mut() = offset;
    }

    fn copy(&self) -> Vec<(Vec<u8>, Vec<u8>, u32, u32)> {
        (0..self.len())
            .filter_map(|idx| self.slot(idx))
            .map(|slot| {
                (
                    get_key(&self.buf, &slot).to_vec(),
                    get_val(&self.buf, &slot).to_vec(),
                    slot.page,
                    slot.vlen,
                )
            })
            .collect::<Vec<_>>()
    }

    fn clear(&mut self) {
        let (id, cap) = (self.id(), self.cap());
        put_head(&mut self.buf, id, cap);
        self.buf[HEAD..].fill(0xFF);
        *self.lo.get_mut() = cap;
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

const U32: usize = size_of::<u32>();
const SLOT: usize = size_of::<Slot>();
const HEAD: usize = 4 * U32; // page header: id, length, size, reserved

fn slot_pos(idx: u32) -> usize {
    HEAD + idx as usize * SLOT
}

fn put_head(buf: &mut BytesMut, id: u32, cap: u32) {
    let mut head = &mut buf[..HEAD];
    head.put_u32(id);
    head.put_u32(cap);
    head.put_u32(0);
    head.put_u32(RESERVED);
}

fn get_u32(buf: &BytesMut, pos: usize) -> u32 {
    let mut src = [0u8; U32];
    src.copy_from_slice(&buf[pos..(pos + U32)]);
    u32::from_be_bytes(src)
}

fn get_key<'a>(buf: &'a BytesMut, slot: &'a Slot) -> &'a [u8] {
    &buf[(slot.offset as usize)..(slot.offset as usize + slot.klen as usize)]
}

fn get_val<'a>(buf: &'a BytesMut, slot: &'a Slot) -> &'a [u8] {
    let vlen = slot.inline_vlen();
    &buf[(slot.offset as usize + slot.klen as usize)
        ..(slot.offset as usize + slot.klen as usize + vlen as usize)]
}

fn put_u32(buf: &mut BytesMut, pos: usize, val: u32) {
    let dst = &mut buf[pos..(pos + U32)];
    dst.copy_from_slice(&val.to_be_bytes());
}

fn put_slice(buf: &mut BytesMut, pos: usize, src: &[u8]) {
    let dst = &mut buf[pos..(pos + src.len())];
    dst.copy_from_slice(src);
}

fn put_size(buf: &mut BytesMut, val: u32) {
    put_u32(buf, SIZE_OFFSET, val);
}

fn put_slot(buf: &mut BytesMut, idx: u32, slot: &Slot) {
    let pos = slot_pos(idx);
    put_u32(buf, pos, slot.offset);
    put_u32(buf, pos + 4, slot.klen);
    put_u32(buf, pos + 8, slot.vlen);
    put_u32(buf, pos + 12, slot.page);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use rand::prelude::*;
    use std::collections::HashSet;

    #[test]
    fn test_sizes() {
        assert_eq!(U32, 4);
        assert_eq!(SLOT, 16);
        assert_eq!(HEAD, 16);
    }

    #[test]
    fn test_sorted() {
        let mut rng = thread_rng();

        let size = 32;
        let len = size * size_of::<u64>() * 4;

        let mut keys = (0..size)
            .map(|_| rng.r#gen::<u64>().to_be_bytes().to_vec())
            .collect::<Vec<_>>();

        let mut page = Block::create(42, len as u32);

        for (i, key) in keys.iter().enumerate() {
            if i % 2 == 0 {
                page.put_ref(key, 42).unwrap();
            } else {
                page.put_val(key, b"undefined").unwrap();
            }
        }

        keys.sort();

        let read = (0..size)
            .map(|idx| page.key(idx as u32).to_vec())
            .collect::<Vec<_>>();

        assert_eq!(read, keys);
    }

    #[test]
    fn test_find() {
        let mut rng = thread_rng();

        let size = 64;
        let len = size * size_of::<u64>() * 10;

        let keys = (0..size)
            .map(|_| rng.r#gen::<u64>())
            .collect::<HashSet<_>>();

        let pairs = keys
            .iter()
            .map(|k| {
                (
                    k.to_be_bytes().to_vec(),
                    rng.r#gen::<u64>().to_be_bytes().to_vec(),
                )
            })
            .collect::<Vec<_>>();

        let mut page = Block::create(42, len as u32);
        for (key, val) in pairs.iter() {
            page.put_val(key, val).unwrap();
        }

        for (key, val) in pairs.iter() {
            let idx = page.find(key).unwrap();
            assert_eq!(page.key(idx), key);
            assert_eq!(page.val(idx), val);
        }
    }

    #[test]
    fn test_ceil() {
        let mut rng = thread_rng();

        let size = 64;
        let len = size * size_of::<u64>() * 10;

        let keys = (0..size)
            .map(|_| {
                let x = rng.r#gen::<u64>();
                x - (x % 100)
            })
            .collect::<Vec<_>>();

        let mut page = Block::create(42, len as u32);
        for key in keys.iter() {
            page.put_ref(&key.to_be_bytes(), 42).unwrap();
        }

        for k in keys.iter() {
            let r = rng.r#gen::<u64>() % 100;
            let key = &(k - r).to_be_bytes();

            let exp = &k.to_be_bytes();
            let idx = page.ceil(key).unwrap();
            assert_eq!(page.key(idx), exp);

            let idx = page.ceil(exp).unwrap();
            assert_eq!(page.key(idx), exp);
        }

        let missing = keys.iter().max().cloned().unwrap() + 1;
        assert_eq!(page.ceil(&missing.to_be_bytes()), None);
    }

    #[test]
    fn test_size() {
        let count = 32;
        let pairs = util::data(count, 42);

        let len =
            pairs.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() + HEAD + pairs.len() * SLOT;

        let mut page = Block::create(42, len as u32);
        assert_eq!(page.free(), len as u32 - HEAD as u32);
        assert_eq!(page.full(), 0);

        let half = count / 2;
        for (k, v) in pairs.iter().take(half) {
            page.put_val(k, v).unwrap();
        }
        let free = half * (size_of::<u64>() * 2 + SLOT);
        assert_eq!(page.free(), free as u32);
        assert_eq!(page.full(), 50);

        for (k, v) in pairs.iter().skip(half) {
            page.put_val(k, v).unwrap();
        }
        assert_eq!(page.free(), 0);
        assert_eq!(page.full(), 100);
    }

    #[test]
    fn test_page() {
        let k1 = b"bb-cc-dd-ee";
        let v1 = b"0000-1111-2222";

        let k2 = b"\x03\x04\x05\x06";
        let v2 = b"ABCDEFGH";

        let k3 = b"xx-yy-zz";
        let p3 = 142;

        let id = 42;
        let len = 128;
        let mut page = Block::create(id, len);
        assert_eq!(page.id(), id);
        assert_eq!(page.cap(), len);
        assert_eq!(page.buf.len(), len as usize);
        assert_eq!(
            &page.buf[0..HEAD],
            &[
                0, 0, 0, id as u8, 0, 0, 0, len as u8, 0, 0, 0, 0, 0xC0, 0xDE, 0xFA, 0xCE,
            ]
        );

        assert_eq!(page.put_val(k1, v1), Some(0));
        assert_eq!(page.put_val(k2, v2), Some(0));
        assert_eq!(page.put_ref(k3, p3), Some(2));

        let slots = (0..page.len())
            .filter_map(|idx| page.slot(idx))
            .collect::<Vec<_>>();

        assert_eq!(
            slots,
            vec![
                Slot::new(
                    len - k2.len() as u32 - v2.len() as u32 - k1.len() as u32 - v1.len() as u32,
                    k2.len() as u32,
                    v2.len() as u32,
                    0
                ),
                Slot::new(
                    len - k1.len() as u32 - v1.len() as u32,
                    k1.len() as u32,
                    v1.len() as u32,
                    0
                ),
                Slot::new(
                    len - k2.len() as u32
                        - v2.len() as u32
                        - k1.len() as u32
                        - v1.len() as u32
                        - k3.len() as u32,
                    k3.len() as u32,
                    0,
                    p3
                ),
            ]
        );

        assert_eq!(page.key(0), k2);
        assert_eq!(page.key(1), k1);
        assert_eq!(page.key(2), k3);

        assert_eq!(page.val(0), v2);
        assert_eq!(page.val(1), v1);
        assert_eq!(page.val(2), &[]);

        assert_eq!(page.find(k1).unwrap(), 1);
        assert_eq!(page.find(k2).unwrap(), 0);
        assert_eq!(page.find(k3).unwrap(), 2);
        assert_eq!(page.find(b"no-such-key"), None);

        assert_eq!(page.ceil(b"\x01"), Some(0));
        assert_eq!(page.ceil(b"\x03"), Some(0));
        assert_eq!(page.ceil(b"a"), Some(1));
        assert_eq!(page.ceil(b"b"), Some(1));
        assert_eq!(page.ceil(b"o"), Some(2));
        assert_eq!(page.ceil(b"x"), Some(2));
        assert_eq!(page.ceil(b"z"), None);

        let free = len
            - HEAD as u32
            - 3 * SLOT as u32
            - k1.len() as u32
            - v1.len() as u32
            - k2.len() as u32
            - v2.len() as u32
            - k3.len() as u32;
        assert_eq!(page.free(), free);

        page.remove(1); // remove (k1, v1)
        assert_eq!(page.free(), free + 16 + k1.len() as u32 + v1.len() as u32);

        assert_eq!(page.find(k2).unwrap(), 0);
        assert_eq!(page.find(k3).unwrap(), 1);
        assert_eq!(page.find(b"no-such-key"), None);
    }

    #[test]
    fn test_put_find_3() {
        let data = vec![
            (b"uno".to_vec(), b"la squadra azzurra".to_vec()),
            (b"due".to_vec(), b"it's coming home".to_vec()),
            (b"tre".to_vec(), b"red devils".to_vec()),
        ];

        let id = 42;
        let len = 256;
        let mut page = Block::create(id, len);

        for (k, v) in data.iter() {
            page.put_val(k, v);
        }

        let mut copy = data.clone();
        copy.sort_by_key(|x| x.0.clone());

        assert_eq!(
            page.copy()
                .into_iter()
                .map(|(k, v, _, _)| (k, v))
                .collect::<Vec<_>>(),
            copy
        );

        assert_eq!(page.find(&data[0].0), Some(2));
        assert_eq!(page.find(&data[1].0), Some(0));
        assert_eq!(page.find(&data[2].0), Some(1));
    }

    #[test]
    fn test_large_key() {
        let mut page = Block::create(1, 64);
        let buf = vec![42u8; 256];
        let opt = page.put_val(&buf, &buf);
        assert!(opt.is_none());
    }

    #[test]
    fn test_is_empty() {
        let page = Block::create(1, 128);
        assert!(page.is_empty());

        let mut page2 = Block::create(2, 128);
        page2.put_val(b"key", b"val");
        assert!(!page2.is_empty());
    }

    #[test]
    fn test_slot_out_of_bounds() {
        let page = Block::create(1, 128);
        assert_eq!(page.slot(0), None);
        assert_eq!(page.slot(999), None);
    }

    #[test]
    fn test_remove_out_of_bounds() {
        let mut page = Block::create(1, 128);
        page.put_val(b"key", b"val");
        // Remove beyond size should be no-op
        page.remove(5);
        assert_eq!(page.len(), 1);
    }

    #[test]
    fn test_find_empty_page() {
        let page = Block::create(1, 128);
        assert_eq!(page.find(b"anything"), None);
    }

    #[test]
    fn test_put_entry_replaces_existing() {
        let mut page = Block::create(1, 256);
        page.put_val(b"key1", b"val1");
        assert_eq!(page.len(), 1);
        assert_eq!(page.val(0), b"val1");

        // Replace with same key, different value
        page.put_val(b"key1", b"val2");
        assert_eq!(page.len(), 1);
        assert_eq!(page.val(0), b"val2");
    }

    #[test]
    fn test_put_overflow_doesnt_fit() {
        let mut page = Block::create(1, 64);
        let big_key = vec![42u8; 256];
        let opt = page.put_overflow(&big_key, 10, 0x80000001);
        assert!(opt.is_none());
    }

    #[test]
    fn test_put_overflow_replaces_existing() {
        let mut page = Block::create(1, 256);
        page.put_overflow(b"overkey", 10, 0x80000001);
        assert_eq!(page.len(), 1);

        // Replace with same key
        page.put_overflow(b"overkey", 20, 0x80000002);
        assert_eq!(page.len(), 1);
        let slot = page.slot(0).unwrap();
        assert_eq!(slot.page, 20);
        assert_eq!(slot.vlen, 0x80000002);
    }

    /// Random puts and removes against a `BTreeMap`, checking after every step
    /// that the page holds exactly the model, in order, and that the cached
    /// data offset agrees with one recomputed from scratch.
    #[test]
    fn test_random_ops_match_model() {
        use std::collections::BTreeMap;

        let mut rng = StdRng::seed_from_u64(7);
        let mut page = Block::create(1, 4096);
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        for _ in 0..20_000 {
            let key = {
                let len = rng.gen_range(1..24);
                (0..len).map(|_| rng.gen_range(0..4u8)).collect::<Vec<_>>()
            };
            if rng.gen_bool(0.6) {
                let val = vec![rng.r#gen::<u8>(); rng.gen_range(0..64)];
                if page.put_val(&key, &val).is_some() {
                    model.insert(key, val);
                } else {
                    assert!(!page.fits((key.len() + val.len()) as u32));
                }
            } else if let Some(idx) = page.find(&key) {
                page.remove(idx);
                model.remove(&key);
            }

            assert_eq!(page.len() as usize, model.len());
            for (idx, (key, val)) in model.iter().enumerate() {
                assert_eq!(page.key(idx as u32), key.as_slice());
                assert_eq!(page.val(idx as u32), val.as_slice());
            }

            // A copy made through `as_mut` knows nothing and recomputes.
            let mut copy = Block::reserve(page.cap());
            copy.as_mut().copy_from_slice(page.as_ref());
            assert_eq!(copy.free(), page.free());

            let used: u32 = model.iter().map(|(k, v)| (k.len() + v.len()) as u32).sum();
            assert_eq!(
                page.free(),
                page.cap() - HEAD as u32 - model.len() as u32 * SLOT as u32 - used,
                "data must stay packed against the end of the page"
            );
        }
    }

    #[test]
    fn test_truncate() {
        let mut rng = StdRng::seed_from_u64(9);
        for len in [0usize, 1, 7, 20, 39, 40, 41] {
            let mut page = Block::create(1, 4096);
            let mut entries = vec![];
            while entries.len() < 40 {
                let key = rng.r#gen::<u64>().to_be_bytes().to_vec();
                let val = vec![rng.r#gen::<u8>(); rng.gen_range(0..40)];
                if page.find(&key).is_none() {
                    page.put_val(&key, &val).unwrap();
                    entries.push((key, val));
                }
            }
            // remove a few to leave the data out of slot order
            for _ in 0..5 {
                let idx = rng.gen_range(0..page.len());
                let key = page.key(idx).to_vec();
                page.remove(idx);
                entries.retain(|(k, _)| *k != key);
            }
            entries.sort();

            page.truncate(len as u32);
            let kept = len.min(entries.len());
            assert_eq!(page.len() as usize, kept);
            for (idx, (key, val)) in entries.iter().take(kept).enumerate() {
                assert_eq!(page.key(idx as u32), key.as_slice());
                assert_eq!(page.val(idx as u32), val.as_slice());
            }
            let mut copy = Block::reserve(page.cap());
            copy.as_mut().copy_from_slice(page.as_ref());
            assert_eq!(copy.free(), page.free());
            let used: u32 = entries
                .iter()
                .take(kept)
                .map(|(k, v)| (k.len() + v.len()) as u32)
                .sum();
            assert_eq!(
                page.free(),
                page.cap() - HEAD as u32 - kept as u32 * SLOT as u32 - used
            );
        }
    }

    #[test]
    fn test_extend_from() {
        let mut src = Block::create(1, 512);
        src.put_val(b"a", b"value-a").unwrap();
        src.put_ref(b"b", 42).unwrap();
        src.put_overflow(b"c", 7, crate::api::page::OVERFLOW_FLAG | 3)
            .unwrap();
        src.put_val(b"d", b"value-d").unwrap();

        let mut dst = Block::create(2, 512);
        dst.put_val(b"0", b"zero").unwrap();
        assert!(dst.extend_from(&src, 1..4));
        assert_eq!(dst.len(), 4);
        assert_eq!(dst.key(0), b"0");
        assert_eq!((dst.key(1), dst.slot(1).unwrap().page), (&b"b"[..], 42));
        let slot = dst.slot(2).unwrap();
        assert_eq!(dst.key(2), b"c");
        assert!(slot.is_overflow());
        assert_eq!((slot.page, slot.multiplier()), (7, 3));
        assert_eq!(dst.val(3), b"value-d");

        let mut small = Block::create(3, 64);
        assert!(
            !small.extend_from(&src, 0..4),
            "four entries do not fit 64 bytes"
        );
    }
}
