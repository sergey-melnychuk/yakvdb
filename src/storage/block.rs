use std::mem::size_of;
use std::cmp::Ordering;
use bytes::{BytesMut, BufMut};
use crate::storage::page::{Page, Slot};
use std::fmt::Debug;

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
        self.buf.as_ref()
    }
}

impl Block {
    pub(crate) fn reserve(capacity: usize) -> Self {
        let mut buf = BytesMut::with_capacity(capacity);
        buf.extend_from_slice(&vec![0u8; capacity]);
        Self {
            buf
        }
    }

    pub(crate) fn create(id: u32, len: u32) -> Self {
        let mut buf = BytesMut::with_capacity(len as usize);
        buf.put_u32(id);
        buf.put_u32(len);
        buf.put_u32(0);
        buf.extend_from_slice(&vec![0u8; (len - 12) as usize]);
        Self {
            buf
        }
    }
}

impl Page for Block {
    fn id(&self) -> u32 {
        get_u32(&self.buf, 0)
    }

    fn len(&self) -> u32 {
        get_u32(&self.buf, 4)
    }

    fn slots(&self) -> u32 {
        get_u32(&self.buf, 8)
    }

    fn slot(&self, idx: u32) -> Option<Slot> {
        if idx > self.slots() {
            return None;
        }
        let i =  12 + U32 * 4 * idx as usize;
        let offset = get_u32(&self.buf, i);
        let klen = get_u32(&self.buf, i + 4);
        let vlen = get_u32(&self.buf, i + 8);
        let page = get_u32(&self.buf, i + 12);
        Some(Slot::new(offset, klen, vlen, page))
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

    fn find(&self, key: &[u8]) -> Option<u32> {
        let n = self.slots();
        if n == 0 {
            return None;
        }

        let n = self.slots();
        let k = bsearch(key, 0, n-1, |i| self.key(i));
        if self.key(k) == key {
            Some(k)
        } else {
            None
        }
    }

    fn ceil(&self, key: &[u8]) -> Option<u32> {
        let n = self.slots();
        if n == 0 {
            return None;
        }

        let n = self.slots();
        let k = bsearch(key, 0, n-1, |i| self.key(i));

        if self.key(k) >= key {
            Some(k)
        } else {
            None
        }
    }

    fn free(&self) -> u32 {
        let n = self.slots();
        if n == 0 {
            return self.len() - 3 * U32 as u32;
        }
        let lo = (3 + n * 4) * U32 as u32;
        let hi = (0..n).into_iter()
            .filter_map(|idx| self.slot(idx))
            .map(|slot| slot.offset)
            .min()
            .unwrap_or(lo);

        assert!(lo <= hi);
        hi - lo
    }

    fn fits(&self, len: u32) -> bool {
        self.free() >= len + 4 * U32 as u32
    }

    fn put_val(&mut self, key: &[u8], val: &[u8]) -> Option<u32> {
        if !self.fits((key.len() + val.len()) as u32) {
            return None;
        }
        self.find(key).into_iter()
            .for_each(|idx| self.remove(idx));

        let n = self.slots();
        let mut slots = (0..n).into_iter()
            .filter_map(|idx| self.slot(idx))
            .collect::<Vec<_>>();

        let klen = key.len() as u32;
        let vlen = val.len() as u32;
        let end = slots.iter()
            .map(|slot| slot.offset)
            .min()
            .unwrap_or(self.len());
        let offset = end - klen - vlen;

        put_slice(&mut self.buf, offset as usize, key);
        put_slice(&mut self.buf, offset as usize + key.len(), val);

        let slot = Slot::new(offset, klen, vlen, 0);
        slots.push(slot);
        // TODO Avoid sorting, use ceil + insert instead
        slots.sort_by_key(|slot| {
            let lo = slot.offset as usize;
            let hi = lo + slot.klen as usize;
            &self.buf[lo..hi]
        });

        put_u32(&mut self.buf, 8, n + 1);

        slots.into_iter()
            .enumerate()
            .for_each(|(idx, slot)| put_slot(&mut self.buf, idx as u32, &slot));

        (0..self.slots()).into_iter()
            .find(|i| self.key(*i as u32) == key)
            .map(|i| i as u32)
    }

    fn put_ref(&mut self, key: &[u8], page: u32) -> Option<u32> {
        if !self.fits(key.len() as u32) {
            return None;
        }
        self.find(key).into_iter()
            .for_each(|idx| self.remove(idx));

        let n = self.slots();
        let mut slots = (0..n).into_iter()
            .filter_map(|idx| self.slot(idx))
            .collect::<Vec<_>>();

        let klen = key.len() as u32;
        let end = slots.iter()
            .map(|slot| slot.offset)
            .min()
            .unwrap_or(self.len());
        let offset = end - klen;

        put_slice(&mut self.buf, offset as usize, key);

        let slot = Slot::new(offset, klen, 0, page);
        slots.push(slot);
        // TODO Avoid sorting, use ceil + insert instead
        slots.sort_by_key(|slot| {
            let lo = slot.offset as usize;
            let hi = lo + slot.klen as usize;
            &self.buf[lo..hi]
        });

        put_u32(&mut self.buf, 8, n + 1);

        slots.into_iter()
            .enumerate()
            .for_each(|(idx, slot)| put_slot(&mut self.buf, idx as u32, &slot));

        (0..self.slots()).into_iter()
            .find(|i| self.key(*i as u32) == key)
            .map(|i| i as u32)
    }

    fn remove(&mut self, idx: u32) {
        let n = self.slots();
        if idx >= n {
            return;
        }

        let mut slots = (0..n).into_iter()
            .filter_map(|idx| self.slot(idx))
            .collect::<Vec<_>>();

        slots.remove(idx as usize);

        put_u32(&mut self.buf, 8, n - 1);

        let total: u32 = slots.iter()
            .map(|slot| slot.klen + slot.vlen)
            .sum();
        let mut offset = self.len() - total;

        let copy: Vec<(Vec<u8>, Vec<u8>)> = slots.iter()
            .map(|slot| (
                get_key(&mut self.buf, slot).to_vec(),
                get_val(&mut self.buf, slot).to_vec(),
            ))
            .collect::<Vec<_>>();

        for (i, (key, val)) in copy.into_iter().enumerate().rev() {
            slots.get_mut(i).unwrap().offset = offset;
            put_slice(&mut self.buf, offset as usize, &key);
            offset += key.len() as u32;
            put_slice(&mut self.buf, offset as usize, &val);
            offset += val.len() as u32;
        }

        slots.iter().cloned().enumerate()
            .for_each(|(idx, slot)| put_slot(&mut self.buf, idx as u32, &slot));
    }

    fn copy(&self) -> Vec<(Vec<u8>, Vec<u8>, u32)> {
        (0..self.slots()).into_iter()
            .filter_map(|idx| self.slot(idx))
            .map(|slot| (
                    get_key(&self.buf, &slot).to_vec(),
                    get_val(&self.buf, &slot).to_vec(),
                    slot.page
                ))
            .collect::<Vec<_>>()
    }
}

const U32: usize = size_of::<u32>();
const SLOT: usize = size_of::<Slot>();

fn get_u32(buf: &BytesMut, pos: usize) -> u32 {
    let mut src = [0u8; U32];
    src.copy_from_slice(&buf[pos..(pos + U32)]);
    u32::from_be_bytes(src)
}

fn get_key<'a>(buf: &'a BytesMut, slot: &'a Slot) -> &'a [u8] {
    &buf[(slot.offset as usize)..(slot.offset as usize + slot.klen as usize)]
}

fn get_val<'a>(buf: &'a BytesMut, slot: &'a Slot) -> &'a [u8] {
    &buf[(slot.offset as usize + slot.klen as usize)..(slot.offset as usize + slot.klen as usize + slot.vlen as usize)]
}

fn put_u32(buf: &mut BytesMut, pos: usize, val: u32) {
    let dst = &mut buf[pos..(pos + U32)];
    dst.copy_from_slice(&val.to_be_bytes());
}

fn put_slice(buf: &mut BytesMut, pos: usize, src: &[u8]) {
    let dst = &mut buf[pos..(pos + src.len())];
    dst.copy_from_slice(src);
}

fn put_slot(buf: &mut BytesMut, idx: u32, slot: &Slot) {
    let pos = (12 + idx * SLOT as u32) as usize;
    put_u32(buf, pos + 0, slot.offset);
    put_u32(buf, pos + 4, slot.klen);
    put_u32(buf, pos + 8, slot.vlen);
    put_u32(buf, pos + 12, slot.page);
}

// unsigned int trait bound inspired by:
// https://users.rust-lang.org/t/difficulty-creating-numeric-trait/34345/4
// https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=1d5c85adec6bdc0eae9f57c74d123dd1
trait UInt: Copy + Ord + Sized + Debug
    + From<u8>
    + std::ops::Add<Output=Self>
    + std::ops::Sub<Output=Self>
    + std::ops::Div<Output=Self>
    + std::cmp::Eq
    + std::cmp::PartialEq<Self>
    {}

impl UInt for u8 {}
impl UInt for u16 {}
impl UInt for u32 {}
impl UInt for u64 {}
impl UInt for u128 {}

fn bsearch<T: Ord, I: UInt, F: Fn(I) -> T>(key: T, mut lo: I, mut hi: I, f: F) -> I {
    while lo < hi {
        let mid = lo + (hi - lo) / I::from(2);
        let mid_key = f(mid);
        match Ord::cmp(&key, &mid_key) {
            Ordering::Less => { hi = mid; }
            Ordering::Greater => { lo = mid + I::from(1); }
            Ordering::Equal => return mid
        }
    }
    assert_eq!(lo, hi);
    lo
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_size() {
        assert_eq!(size_of::<Slot>(), 4 * size_of::<u32>());
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
        assert_eq!(page.len(), len);
        assert_eq!(page.buf.len(), len as usize);
        assert_eq!(&page.buf[0..12], &[0,0,0,id as u8,0,0,0,len as u8,0,0,0,0]);

        assert_eq!(page.put_val(k1, v1), Some(0));
        assert_eq!(page.put_val(k2, v2), Some(0));
        assert_eq!(page.put_ref(k3, p3), Some(2));

        let slots = (0..page.slots())
            .into_iter()
            .filter_map(|idx| page.slot(idx))
            .collect::<Vec<_>>();

        assert_eq!(slots, vec![
            Slot::new(len
                          - k2.len() as u32 - v2.len() as u32
                          - k1.len() as u32 - v1.len() as u32,
                k2.len() as u32, v2.len() as u32, 0),
            Slot::new(len
                          - k1.len() as u32 - v1.len() as u32,
                k1.len() as u32, v1.len() as u32, 0),
            Slot::new(len
                          - k2.len() as u32 - v2.len() as u32
                          - k1.len() as u32 - v1.len() as u32
                          - k3.len() as u32,
                k3.len() as u32, 0, p3),
        ]);

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
            - 12        // header: 3 x u32
            - 3 * 16    // 3 slots: 3 x 4 x u32
            - k1.len() as u32 - v1.len() as u32
            - k2.len() as u32 - v2.len() as u32
            - k3.len() as u32;
        assert_eq!(page.free(), free);

        page.remove(1); // remove k1-v1
        assert_eq!(page.free(), free + 16 + k1.len() as u32 + v1.len() as u32);

        assert_eq!(page.find(k2).unwrap(), 0);
        assert_eq!(page.find(k3).unwrap(), 1);
        assert_eq!(page.find(b"no-such-key"), None);
    }
}