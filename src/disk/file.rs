use crate::api::page::Page;
use crate::api::tree::Tree;
use bytes::{Buf, BufMut, BytesMut};
use std::cell::{Ref, RefCell, RefMut};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::ops::Deref;
use std::path::Path;
use crate::api::error::{Error, Result};

struct File<P: Page> {
    /// Underlying file reference where all data is physically stored.
    file: RefCell<fs::File>,
    head: Head,

    /// In-memory page cache. All page access happens only through cached page representation.
    cache: RefCell<HashMap<u32, P>>,

    /// Set of pages that requires flushing to the disk for durability.
    dirty: HashSet<u32>,

    /// Min-heap of available page identifiers (this helps avoid "gaps": empty pages inside file).
    empty: BinaryHeap<Reverse<u32>>,
}

const MAGIC: &[u8] = b"YAKVDB42";

const HEAD: usize = MAGIC.len() + size_of::<Head>();
const ROOT: u32 = 1;

#[derive(Debug)]
#[repr(C)]
struct Head {
    page_bytes: u32,
    page_count: u32,
}

impl<P: Page> File<P> {
    fn make(path: &Path, page_bytes: u32) -> io::Result<Self> {
        if path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("File exists: {:?}", path),
            ));
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;

        let head = Head {
            page_bytes,
            page_count: 1,
        };

        let mut buf = BytesMut::with_capacity(HEAD + page_bytes as usize);
        buf.put_slice(&MAGIC);
        buf.put_u32(head.page_bytes);
        buf.put_u32(head.page_count);

        let root = P::create(ROOT, 0, head.page_bytes);
        buf.put_slice(root.as_ref());

        file.write_all(buf.as_ref())?;
        file.flush()?;

        Ok(Self {
            file: RefCell::new(file),
            head,
            cache: RefCell::new(HashMap::with_capacity(32)),
            dirty: HashSet::with_capacity(32),
            empty: BinaryHeap::with_capacity(16),
        })
    }

    fn open(path: &Path) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        let len = file.metadata()?.len() as usize;
        if len < HEAD {
            return Err(io::Error::new(io::ErrorKind::Other, "File too short"));
        }

        let mut buf = BytesMut::with_capacity(HEAD);
        buf.extend_from_slice(&[0u8; HEAD]);
        file.read_exact(&mut buf[..])?;

        let mut magic = [0u8; 8];
        buf.copy_to_slice(&mut magic);
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("MAGIC mismatch: {:?}", magic),
            ));
        }

        let head = Head {
            page_bytes: buf.get_u32(),
            page_count: buf.get_u32(),
        };

        if head.page_bytes > u16::MAX as u32 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Page size too large: {}", head.page_bytes),
            ));
        }

        if len < HEAD + head.page_bytes as usize {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "File does not contain one full page".to_string(),
            ));
        }

        let mut root = P::reserve(head.page_bytes);
        file.read_exact(root.as_mut())?;

        let this = Self {
            file: RefCell::new(file),
            head,
            cache: RefCell::new(HashMap::with_capacity(32)),
            dirty: HashSet::with_capacity(32),
            empty: BinaryHeap::with_capacity(16),
        };

        this.cache.borrow_mut().insert(ROOT, root);

        Ok(this)
    }

    fn load(&self, offset: usize, length: u32) -> io::Result<P> {
        let mut page = P::reserve(length as u32);
        self.file
            .borrow_mut()
            .seek(SeekFrom::Start(offset as u64))?;
        self.file.borrow_mut().read_exact(page.as_mut())?;
        Ok(page)
    }

    fn save(&self, page: &P) -> io::Result<()> {
        let offset = self.offset(page.id()) as u64;
        self.file.borrow_mut().seek(SeekFrom::Start(offset))?;
        self.file.borrow_mut().write_all(page.as_ref())
    }

    fn offset(&self, id: u32) -> usize {
        HEAD + (id - 1) as usize * self.head.page_bytes as usize
    }
}

impl<P: Page> Tree<P> for File<P> {
    fn lookup(&self, key: &[u8]) -> Result<Option<Ref<[u8]>>> {
        let mut seen = HashSet::with_capacity(8);
        let mut page = self.root();
        loop {
            let idx_opt = page.ceil(key);
            if idx_opt.is_none() {
                return Ok(None);
            }
            let idx = idx_opt.unwrap();

            let slot_opt = page.slot(idx);
            if slot_opt.is_none() {
                return Err(Error::Tree(page.id(), format!("Slot not found: {}", idx)));
            }
            let slot = slot_opt.unwrap();

            if slot.page == 0 {
                // Log how deep the lookup went into the tree depth: seen.len()
                return if key == page.key(idx) {
                    Ok(Some(Ref::map(page, |p| p.val(idx))))
                } else {
                    Ok(None)
                };
            } else {
                if seen.contains(&slot.page) {
                    return Err(Error::Tree(page.id(), "Cyclic reference detected".to_string()));
                }
                seen.insert(page.id());

                let page_opt = self.page(slot.page);
                if page_opt.is_none() {
                    return Err(Error::Tree(page.id(), format!("Page not found: {}", slot.page)));
                }
                page = page_opt.unwrap();
            }
        }
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        let mut page = self.root_mut();
        let mut seen = HashSet::with_capacity(8);
        let mut path = Vec::with_capacity(8);
        loop {
            if page.size() == 0 {
                page.put_val(key, val);
                self.flush(page.deref())?;
                return Ok(());
            }

            let idx = page.ceil(key)
                .unwrap_or_else(|| page.size() - 1);

            if let Some((parent_id, parent_idx)) = path.last().cloned() {
                // TODO test it!
                let mut parent_page = self.page_mut(parent_id).unwrap();
                let parent_key = parent_page.key(parent_idx);
                if key > parent_key {
                    let parent_key_copy = &parent_key.to_vec();
                    parent_page.put_ref(&parent_key_copy, page.id());
                    self.flush(parent_page.deref())?;
                }
            }

            let slot_opt = page.slot(idx);
            if slot_opt.is_none() {
                return Err(Error::Tree(page.id(), format!("Slot not found: {}", idx)));
            }
            let slot = slot_opt.unwrap();

            if slot.page == 0 {
                let len = (key.len() + val.len()) as u32;
                if page.fits(len) {
                    page.put_val(key, val);
                    self.flush(page.deref())?;
                    return Ok(());
                } else {
                    // TODO this page needs to be split into two
                    return Err(Error::Tree(page.id(), format!("Entry does not fit into the page: size={} free={}", len, page.free())))
                }
            } else {
                path.push((page.id(), idx));
                if seen.contains(&slot.page) {
                    return Err(Error::Tree(page.id(), "Cyclic reference detected".to_string()));
                }
                seen.insert(page.id());

                let page_opt = self.page_mut(slot.page);
                if page_opt.is_none() {
                    return Err(Error::Tree(page.id(), format!("Page not found: {}", slot.page)));
                }
                page = page_opt.unwrap();
            }
        }
    }

    fn remove(&mut self, key: &[u8]) -> Result<()>  {
        let mut page = self.root_mut();
        let mut seen = HashSet::with_capacity(8);
        let mut path = Vec::with_capacity(8);
        loop {
            let idx_opt = page.ceil(key);
            if idx_opt.is_none() {
                return Ok(());
            }
            let idx = idx_opt.unwrap();

            let slot_opt = page.slot(idx);
            if slot_opt.is_none() {
                return Err(Error::Tree(page.id(), format!("Slot not found: {}", idx)));
            }
            let slot = slot_opt.unwrap();

            if slot.page == 0 {
                page.remove(idx);

                if page.size() > 0 && idx == page.size() - 1 {
                    // TODO test it!
                    if let Some((parent_id, parent_idx)) = path.last().cloned() {
                        let mut parent_page = self.page_mut(parent_id).unwrap();
                        parent_page.remove(parent_idx);
                        parent_page.put_ref(page.max(), page.id());
                        self.flush(parent_page.deref())?;
                    }
                }

                // TODO Check if the page become too small and needs to be merged into another one
                // TODO Check if the destination page (where current one can be merged) exists
                return Ok(());
            } else {
                path.push((page.id(), idx));
                if seen.contains(&slot.page) {
                    return Err(Error::Tree(page.id(), "Cyclic reference detected".to_string()));
                }
                seen.insert(page.id());

                let page_opt = self.page_mut(slot.page);
                if page_opt.is_none() {
                    return Err(Error::Tree(page.id(), format!("Page not found: {}", slot.page)));
                }
                page = page_opt.unwrap();
            }
        }
    }

    fn root(&self) -> Ref<P> {
        self.page(ROOT).unwrap()
    }

    fn page(&self, id: u32) -> Option<Ref<P>> {
        if !self.cache.borrow().contains_key(&id) {
            let page = self.load(self.offset(id), self.head.page_bytes).ok()?;
            self.cache.borrow_mut().insert(id, page);
        }
        let page = Ref::map(self.cache.borrow(), |cache| cache.get(&id).unwrap());
        Some(page)
    }

    fn root_mut(&self) -> RefMut<P> {
        self.page_mut(ROOT).unwrap()
    }

    fn page_mut(&self, id: u32) -> Option<RefMut<P>> {
        if !self.cache.borrow().contains_key(&id) {
            let page = self.load(self.offset(id), self.head.page_bytes).ok()?;
            self.cache.borrow_mut().insert(id, page);
        }
        let page = RefMut::map(self.cache.borrow_mut(), |cache| cache.get_mut(&id).unwrap());
        Some(page)
    }

    fn flush(&self, page: &P) -> crate::api::error::Result<()> {
        self.save(page).map_err(|e| e.into())
    }

    fn next_id(&mut self) -> u32 {
        // TODO Append new page if necessary (and initialize it)!
        self.empty.peek().cloned().unwrap().0
    }

    fn free_id(&mut self, id: u32) {
        self.empty.push(Reverse(id))
    }

    // TODO test it!
    fn split(&mut self, page: &mut P) -> Result<()> {
        let id = page.id();
        if id == ROOT {
            let lo_id = self.next_id();
            let hi_id = self.next_id();
            let mut lo = self.page_mut(lo_id).unwrap();
            let mut hi = self.page_mut(hi_id).unwrap();

            let size = page.size();
            let half = size / 2;

            (0..half).for_each(|idx| {
                let p = page.slot(idx).unwrap().page;
                let k = page.key(idx);
                if p == 0 {
                    let v = page.val(idx);
                    lo.put_val(k, v);
                } else {
                    lo.put_ref(k, p);
                }
            });

            (half..size).for_each(|idx| {
                let p = page.slot(idx).unwrap().page;
                let k = page.key(idx);
                if p == 0 {
                    let v = page.val(idx);
                    hi.put_val(k, v);
                } else {
                    hi.put_ref(k, p);
                }
            });

            page.clear();
            page.put_ref(lo.max(), lo_id);
            page.put_ref(hi.max(), hi_id);

            self.flush(lo.deref())?;
            self.flush(hi.deref())?;
            self.flush(page.deref())?;
            Ok(())
        } else {
            let peer_id = self.next_id();
            let mut peer = self.page_mut(peer_id).unwrap();

            let size = page.size();
            let half = size / 2;

            (half..size).for_each(|idx| {
               let p = page.slot(idx).unwrap().page;
                let k = page.key(idx);
                if p == 0 {
                    let v = page.val(idx);
                    peer.put_val(k, v);
                } else {
                    peer.put_ref(k, p);
                }
                page.remove(idx);
            });

            let parent_id = page.parent();
            let mut parent = self.page_mut(parent_id).unwrap();

            parent.put_ref(page.max(), page.id());
            parent.put_ref(peer.max(), peer_id);

            self.flush(parent.deref())?;
            self.flush(peer.deref())?;
            self.flush(page.deref())?;

            Ok(())
        }
    }

    // TODO test it!
    fn merge(&mut self, this: &mut P, that: &mut P) -> Result<()> {
        (0..that.size()).into_iter()
            .for_each(|idx| {
                let slot = that.slot(idx).unwrap();
                let k = that.key(idx);
                if slot.page == 0 {
                    let v = that.val(idx);
                    this.put_val(k, v);
                } else {
                    let p = slot.page;
                    this.put_ref(k, p);
                }
            });
        self.free_id(that.id());
        that.clear();

        self.flush(this.deref())?;
        self.flush(that.deref())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::block::Block;
    use std::ops::Deref;

    fn get<P: Page>(page: &P, key: &[u8]) -> Option<(Vec<u8>, u32)> {
        page.find(key)
            .map(|idx| (page.val(idx).to_vec(), page.slot(idx).unwrap().page))
    }

    #[test]
    fn test_page() {
        let path = Path::new("target/page_test.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let size: u32 = 256;

        let data = vec![
            (b"aaa".to_vec(), b"zxczxczxc".to_vec(), 0),
            (b"bbb".to_vec(), b"asdasdasd".to_vec(), 0),
            (b"ccc".to_vec(), b"qweqweqwe".to_vec(), 0),
            (b"ddd".to_vec(), b"123123123".to_vec(), 0),
            (b"xxx".to_vec(), vec![], 3333),
            (b"yyy".to_vec(), vec![], 2222),
            (b"zzz".to_vec(), vec![], 1111),
        ];

        {
            let file: File<Block> = File::make(path, size).unwrap();
            {
                let mut page = file.root_mut();
                for (k, v, p) in data.iter() {
                    if *p == 0 {
                        page.put_val(k, v);
                    } else {
                        page.put_ref(k, *p);
                    }
                }
            };
            let page = file.root();
            file.save(page.deref()).unwrap();
        }

        let file: File<Block> = File::open(path).unwrap();
        let mut page = file.load(file.offset(ROOT), size).unwrap();

        assert_eq!(page.copy(), data);

        for (k, v, p) in data.iter() {
            assert_eq!(get(&page, k), Some((v.to_vec(), *p)));
        }

        page.remove(page.find(b"aaa").unwrap());
        assert_eq!(get(&page, b"aaa"), None);

        page.remove(page.find(b"zzz").unwrap());
        assert_eq!(get(&page, b"zzz"), None);
    }

    #[test]
    fn test_file() {
        let path = Path::new("target/file_test.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let size: u32 = 256;

        let data = vec![
            (b"uno".to_vec(), b"la squadra azzurra".to_vec()),
            (b"due".to_vec(), b"it's coming home".to_vec()),
            (b"tre".to_vec(), b"red devils".to_vec()),
        ];

        let mut file: File<Block> = File::make(path, size).unwrap();

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
    }
}
