use crate::api::page::{Page, Slot};
use crate::util::bsearch::bsearch;
use bytes::{BufMut, BytesMut};
use std::mem::size_of;

pub(crate) struct Block {
    buf: BytesMut,
}

impl AsMut<[u8]> for Block {
    fn as_mut(&mut self) -> &mut [u8] {
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
const RESERVED: u32 = 0xC0DE1542;

impl Block {
    fn put_entry(&mut self, key: &[u8], val: &[u8], page: u32) -> Option<u32> {
        if !self.fits((key.len() + val.len()) as u32) {
            return None;
        }

        let ceil_opt = self.ceil(key);
        if let Some(idx) = &ceil_opt {
            if self.key(*idx) == key {
                let n = self.len() - 1;
                put_size(&mut self.buf, n);
                self.remove(*idx);
            }
        }

        let size = self.len();
        let idx = self.ceil(key).unwrap_or_else(|| size);

        let mut slots = (0..size)
            .into_iter()
            .filter_map(|idx| self.slot(idx))
            .collect::<Vec<_>>();

        let klen = key.len() as u32;
        let vlen = val.len() as u32;
        let end = slots
            .iter()
            .map(|slot| slot.offset)
            .min()
            .unwrap_or_else(|| self.cap());
        let offset = end - klen - vlen;
        let slot = Slot::new(offset, klen, vlen, page);

        slots.insert(idx as usize, slot);
        slots
            .into_iter()
            .enumerate()
            .for_each(|(idx, slot)| put_slot(&mut self.buf, idx as u32, &slot));

        let n = self.len() + 1;
        put_size(&mut self.buf, n);

        put_slice(&mut self.buf, offset as usize, key);
        if !val.is_empty() {
            put_slice(&mut self.buf, offset as usize + key.len(), val);
        }

        Some(idx)
    }
}

impl Page for Block {
    fn reserve(capacity: u32) -> Self {
        let mut buf = BytesMut::with_capacity(capacity as usize);
        buf.extend_from_slice(&vec![0u8; capacity as usize]);
        Self { buf }
    }

    fn create(id: u32, cap: u32) -> Self {
        let mut buf = BytesMut::with_capacity(cap as usize);
        buf.put_u32(id);
        buf.put_u32(cap);
        buf.put_u32(0);
        buf.put_u32(RESERVED);
        assert_eq!(buf.len(), HEAD);
        buf.extend_from_slice(&vec![0u8; cap as usize - HEAD]);
        Self { buf }
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
        let pos = HEAD + U32 * 4 * idx as usize;
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
                let at = slot.offset as usize + slot.klen as usize;
                let to = at + slot.vlen as usize;
                &self.buf[at..to]
            })
            .unwrap_or_default()
    }

    fn free(&self) -> u32 {
        let size = self.len();
        if size == 0 {
            return self.cap() - HEAD as u32;
        }
        let lo = HEAD as u32 + size * SLOT as u32;
        let hi = (0..size)
            .into_iter()
            .filter_map(|idx| self.slot(idx))
            .map(|slot| slot.offset)
            .min()
            .unwrap_or(lo);

        assert!(lo <= hi);
        hi - lo
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
        if self.key(k) == key {
            Some(k)
        } else {
            None
        }
    }

    fn ceil(&self, key: &[u8]) -> Option<u32> {
        let n = self.len();
        if n == 0 {
            return None;
        }

        let k = bsearch(key, 0, n - 1, |i| self.key(i));
        if self.key(k) >= key {
            Some(k)
        } else {
            None
        }
    }

    fn put_val(&mut self, key: &[u8], val: &[u8]) -> Option<u32> {
        self.put_entry(key, val, 0)
    }

    fn put_ref(&mut self, key: &[u8], page: u32) -> Option<u32> {
        self.put_entry(key, &[], page)
    }

    fn remove(&mut self, idx: u32) {
        let size = self.len();
        if idx >= size {
            return;
        }

        let mut slots = (0..size)
            .into_iter()
            .filter_map(|idx| self.slot(idx))
            .collect::<Vec<_>>();

        let removed = slots.remove(idx as usize);
        let blank = vec![0u8; (removed.klen + removed.vlen) as usize];
        put_slice(&mut self.buf, removed.offset as usize, &blank);

        put_size(&mut self.buf, size - 1);

        let total: u32 = slots.iter().map(|slot| slot.klen + slot.vlen).sum();
        let mut offset = self.cap() - total;

        let copy = slots
            .iter()
            .map(|slot| {
                (
                    get_key(&self.buf, slot).to_vec(),
                    get_val(&self.buf, slot).to_vec(),
                )
            })
            .collect::<Vec<_>>();

        for (i, (key, val)) in copy.iter().enumerate() {
            slots.get_mut(i).unwrap().offset = offset;
            put_slice(&mut self.buf, offset as usize, key);
            offset += key.len() as u32;
            if !val.is_empty() {
                put_slice(&mut self.buf, offset as usize, val);
                offset += val.len() as u32;
            }
        }

        slots.push(Slot::empty());
        slots
            .into_iter()
            .enumerate()
            .for_each(|(idx, slot)| put_slot(&mut self.buf, idx as u32, &slot));
    }

    fn copy(&self) -> Vec<(Vec<u8>, Vec<u8>, u32)> {
        (0..self.len())
            .into_iter()
            .filter_map(|idx| self.slot(idx))
            .map(|slot| {
                (
                    get_key(&self.buf, &slot).to_vec(),
                    get_val(&self.buf, &slot).to_vec(),
                    slot.page,
                )
            })
            .collect::<Vec<_>>()
    }

    fn clear(&mut self) {
        let len = self.cap() as usize;
        let mut tmp = BytesMut::with_capacity(len);
        tmp.put_u32(self.id());
        tmp.put_u32(self.cap());
        tmp.put_u32(0);
        tmp.put_u32(RESERVED);
        self.buf[..HEAD].copy_from_slice(tmp.as_ref());
        let blank = vec![0xFFu8; len - HEAD];
        self.buf[HEAD..].copy_from_slice(&blank);
    }
}

const U32: usize = size_of::<u32>();
const SLOT: usize = size_of::<Slot>();
const HEAD: usize = 4 * U32; // page header: id, length, size, reserved

fn get_u32(buf: &BytesMut, pos: usize) -> u32 {
    let mut src = [0u8; U32];
    src.copy_from_slice(&buf[pos..(pos + U32)]);
    u32::from_be_bytes(src)
}

fn get_key<'a>(buf: &'a BytesMut, slot: &'a Slot) -> &'a [u8] {
    &buf[(slot.offset as usize)..(slot.offset as usize + slot.klen as usize)]
}

fn get_val<'a>(buf: &'a BytesMut, slot: &'a Slot) -> &'a [u8] {
    &buf[(slot.offset as usize + slot.klen as usize)
        ..(slot.offset as usize + slot.klen as usize + slot.vlen as usize)]
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
    let pos = HEAD + idx as usize * SLOT;
    put_u32(buf, pos, slot.offset);
    put_u32(buf, pos + 4, slot.klen);
    put_u32(buf, pos + 8, slot.vlen);
    put_u32(buf, pos + 12, slot.page);
}

#[cfg(test)]
mod tests {
    use super::*;
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
            .into_iter()
            .map(|_| rng.gen::<u64>().to_be_bytes().to_vec())
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
            .into_iter()
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
            .into_iter()
            .map(|_| rng.gen::<u64>())
            .collect::<HashSet<_>>();

        let pairs = keys
            .iter()
            .map(|k| {
                (
                    k.to_be_bytes().to_vec(),
                    rng.gen::<u64>().to_be_bytes().to_vec(),
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
            .into_iter()
            .map(|_| {
                let x = rng.gen::<u64>();
                x - (x % 100)
            })
            .collect::<Vec<_>>();

        let mut page = Block::create(42, len as u32);
        for key in keys.iter() {
            page.put_ref(&key.to_be_bytes(), 42).unwrap();
        }

        for k in keys.iter() {
            let r = rng.gen::<u64>() % 100;
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
        let mut rng = thread_rng();
        let count = 32;

        let pairs = (0..count)
            .into_iter()
            .map(|_| {
                (
                    rng.next_u64().to_be_bytes().to_vec(),
                    rng.next_u64().to_be_bytes().to_vec(),
                )
            })
            .collect::<Vec<_>>();

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
            &[0, 0, 0, id as u8, 0, 0, 0, len as u8, 0, 0, 0, 0, 0xC0, 0xDE, 0x15, 0x42,]
        );

        assert_eq!(page.put_val(k1, v1), Some(0));
        assert_eq!(page.put_val(k2, v2), Some(0));
        assert_eq!(page.put_ref(k3, p3), Some(2));

        let slots = (0..page.len())
            .into_iter()
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
                .map(|(k, v, _)| (k, v))
                .collect::<Vec<_>>(),
            copy
        );

        assert_eq!(page.find(&data[0].0), Some(2));
        assert_eq!(page.find(&data[1].0), Some(0));
        assert_eq!(page.find(&data[2].0), Some(1));
    }
}
