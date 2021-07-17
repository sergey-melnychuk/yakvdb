use crate::api::error::{Error, Result};
use crate::api::page::Page;
use crate::api::tree::Tree;
use crate::util::hex::hex;
use bytes::{Buf, BufMut, BytesMut};
use std::cell::{Ref, RefCell, RefMut};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::ops::Deref;
use std::path::Path;
use log::{debug, trace};

pub(crate) struct File<P: Page> {
    /// Underlying file reference where all data is physically stored.
    file: RefCell<fs::File>,
    head: Head,

    /// In-memory page cache. All page access happens only through cached page representation.
    cache: RefCell<HashMap<u32, P>>,

    /// Min-heap of available page identifiers (this helps avoid "gaps": empty pages inside file).
    empty: RefCell<BinaryHeap<Reverse<u32>>>,
}

const MAGIC: &[u8] = b"YAKVDB42";

const HEAD: usize = MAGIC.len() + size_of::<Head>();
const ROOT: u32 = 1;

const SPLIT_THRESHOLD: u8 = 80;
const MERGE_THRESHOLD: u8 = 30;

#[derive(Debug)]
#[repr(C)]
struct Head {
    page_bytes: u32,
    page_count: u32,
}

impl<P: Page> File<P> {
    pub(crate) fn make(path: &Path, page_bytes: u32) -> io::Result<Self> {
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
            empty: RefCell::new(BinaryHeap::with_capacity(32)),
        })
    }

    #[allow(dead_code)] // TODO FIXME
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
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
            empty: RefCell::new(BinaryHeap::with_capacity(16)),
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
                    return Err(Error::Tree(
                        page.id(),
                        "Cyclic reference detected".to_string(),
                    ));
                }
                seen.insert(page.id());

                let page_opt = self.page(slot.page);
                if page_opt.is_none() {
                    return Err(Error::Tree(
                        page.id(),
                        format!("Page not found: {}", slot.page),
                    ));
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
            let id = page.id();

            if page.size() == 0 {
                page.put_val(key, val);
                drop(page);
                self.flush(id)?;
                return Ok(());
            }

            let idx = page.ceil(key).unwrap_or_else(|| page.size() - 1);

            drop(page);
            if let Some((parent_id, parent_idx)) = path.last().cloned() {
                let mut parent_page = self.page_mut(parent_id).unwrap();
                let parent_key = parent_page.key(parent_idx);
                if key > parent_key {
                    parent_page.remove(parent_idx);
                    parent_page.put_ref(key, id);
                    drop(parent_page);
                    self.flush(parent_id)?;
                }
            }
            page = self.page_mut(id).unwrap();

            let slot_opt = page.slot(idx);
            if slot_opt.is_none() {
                return Err(Error::Tree(page.id(), format!("Slot not found: {}", idx)));
            }
            let slot = slot_opt.unwrap();

            if slot.page == 0 {
                let len = (key.len() + val.len()) as u32;
                if !page.fits(len) {
                    return Err(Error::Tree(
                        page.id(),
                        format!(
                            "Entry does not fit into the page: size={} free={}",
                            len,
                            page.free()
                        ),
                    ));
                }
                page.put_val(key, val);

                let full = page.full();
                drop(page);
                if full > SPLIT_THRESHOLD {
                    self.split(id)?;
                } else {
                    self.flush(id)?;
                }

                return Ok(());
            } else {
                path.push((id, idx));
                seen.insert(id);
                if seen.contains(&slot.page) {
                    return Err(Error::Tree(id, "Cyclic reference detected".to_string()));
                }

                drop(page);
                let page_opt = self.page_mut(slot.page);
                if page_opt.is_none() {
                    return Err(Error::Tree(
                        slot.page,
                        format!("Page not found: {}", slot.page),
                    ));
                }
                page = page_opt.unwrap();
            }
        }
    }

    fn remove(&mut self, key: &[u8]) -> Result<()> {
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

            let id = page.id();
            if slot.page == 0 {
                debug!("remove: key={} page={} idx={}", hex(key), id, idx);
                page.remove(idx);
                drop(page);

                // Navigate up-tree and remove/update references if needed
                let mut page_id = id;
                for (parent_id, mut idx) in path.iter().cloned().rev() {
                    let full = self.page(page_id).unwrap().full();
                    if full < MERGE_THRESHOLD {
                        let peer_id = {
                            let parent = self.page(parent_id).unwrap();
                            let mut peers = Vec::with_capacity(2);
                            if idx > 0 {
                                let peer = parent.slot(idx - 1).unwrap().page;
                                peers.push(peer);
                            }
                            if idx < parent.size() - 1 {
                                let peer = parent.slot(idx + 1).unwrap().page;
                                peers.push(peer);
                            }
                            drop(parent);

                            peers
                                .into_iter()
                                .filter_map(|peer_id| {
                                    let peer = self.page(peer_id).unwrap();
                                    let full = peer.full();
                                    if peer.size() > 0 && full < MERGE_THRESHOLD {
                                        Some((peer_id, full))
                                    } else {
                                        None
                                    }
                                })
                                .min_by_key(|(_, full)| *full)
                                .map(|(peer_id, _)| peer_id)
                        };
                        if let Some(peer_id) = peer_id {
                            trace!("merge: found peer_id={} to merge page_id={} (parent_id={})", peer_id, page_id, parent_id);
                            let peer_max = {
                                let peer = self.page(peer_id).unwrap();
                                peer.max().to_vec()
                            };
                            trace!("\t merge: peer_max={}", hex(&peer_max));
                            let mut parent = self.page_mut(parent_id).unwrap();
                            parent.remove(idx);
                            let peer_idx = parent.ceil(&peer_max).unwrap();
                            trace!("\t merge: parent remove: peer_idx={} idx={}", peer_idx, idx);
                            parent.remove(peer_idx);
                            drop(parent);

                            self.merge(page_id, peer_id)?;
                            let page_max = {
                                let peer = self.page(peer_id).unwrap();
                                peer.max().to_vec()
                            };
                            trace!("\t merge: page_max={}", hex(&page_max));
                            let mut parent = self.page_mut(parent_id).unwrap();
                            trace!("\t merge: parent insert: page_max={}, peer_id={}", hex(&page_max), peer_id);
                            parent.put_ref(&page_max, peer_id);
                            idx = parent.ceil(&page_max).unwrap();
                            page_id = peer_id;
                        }
                    }

                    let max_opt = {
                        let page = self.page(page_id).unwrap();
                        if page.size() > 0 {
                            Some(page.max().to_vec())
                        } else {
                            None
                        }
                    };

                    let mut parent = self.page_mut(parent_id).unwrap();
                    if let Some(max) = max_opt {
                        if max < parent.key(idx).to_vec() {
                            parent.remove(idx);
                            parent.put_ref(&max, page_id);
                        }
                    } else {
                        parent.remove(idx);
                    }
                    drop(parent);
                    self.flush(parent_id)?;
                    page_id = parent_id;
                }

                return Ok(());
            } else {
                path.push((id, idx));
                seen.insert(id);
                if seen.contains(&slot.page) {
                    return Err(Error::Tree(id, "Cyclic reference detected".to_string()));
                }
                drop(page);

                let page_opt = self.page_mut(slot.page);
                if page_opt.is_none() {
                    return Err(Error::Tree(id, format!("Page not found: {}", slot.page)));
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

    fn flush(&self, id: u32) -> crate::api::error::Result<()> {
        if let Some(page) = self.page(id) {
            self.save(page.deref()).map_err(|e| e.into())
        } else {
            Err(Error::Tree(id, "Page not found".to_string()))
        }
    }

    fn next_id(&self, parent_id: u32) -> u32 {
        if !self.empty.borrow().is_empty() {
            let id = self.empty.borrow_mut().pop().unwrap().0;
            let temp = P::create(id, parent_id, self.head.page_bytes);
            let mut page = self.page_mut(id).unwrap();
            page.as_mut().copy_from_slice(temp.as_ref());
            return id;
        }

        let len = self.file.borrow_mut().metadata().unwrap().len();
        let id = 1 + ((len - HEAD as u64) / self.head.page_bytes as u64) as u32;
        let page = P::create(id, parent_id, self.head.page_bytes);
        {
            let mut f = self.file.borrow_mut();
            f.seek(SeekFrom::End(0)).unwrap(); // TODO deal with possible panic
            f.write_all(page.as_ref()).unwrap(); // TODO deal with possible panic
        }

        id
    }

    fn free_id(&self, id: u32) {
        self.empty.borrow_mut().push(Reverse(id))
    }

    fn split(&self, id: u32) -> Result<()> {
        if id == ROOT {
            let lo_id = self.next_id(ROOT);
            let hi_id = self.next_id(ROOT);

            let (copy, lo_max, hi_max) = {
                let page = self.page(id).unwrap();
                let copy = page.copy();
                let half = page.size() as usize / 2;
                let lo_max = copy.get(half - 1).map(|(k, _, _)| k).cloned().unwrap();
                let hi_max = copy.last().map(|(k, _, _)| k).cloned().unwrap();
                (copy, lo_max, hi_max)
            };
            let half = copy.len() / 2;

            {
                let mut lo = self.page_mut(lo_id).unwrap();
                copy.iter().take(half).for_each(|(key, val, page)| {
                    trace!("split: move k={} v={} p={} from {} to {}", hex(key), hex(val), *page, id, lo_id);
                    if *page == 0 {
                        lo.put_val(key, val);
                    } else {
                        lo.put_ref(key, *page);
                    }
                });
            }

            {
                let mut hi = self.page_mut(hi_id).unwrap();
                copy.iter().skip(half).for_each(|(key, val, page)| {
                    trace!("split: move k={} v={} p={} from {} to {}", hex(key), hex(val), *page, id, hi_id);
                    if *page == 0 {
                        hi.put_val(key, val);
                    } else {
                        hi.put_ref(key, *page);
                    }
                });
            }

            {
                let mut page = self.page_mut(id).unwrap();
                page.clear();
                page.put_ref(&lo_max, lo_id);
                page.put_ref(&hi_max, hi_id);
            }

            self.flush(id)?;
            self.flush(lo_id)?;
            self.flush(hi_id)?;
            Ok(())
        } else {
            let (copy, max, parent_id) = {
                let page = self.page(id).unwrap();
                (page.copy(), page.max().to_vec(), page.parent())
            };
            let half = copy.len() / 2;
            let peer_id = self.next_id(parent_id);

            let page_max = {
                let mut page = self.page_mut(id).unwrap();
                copy.iter().skip(half).for_each(|(key, _, _)| {
                    let idx = page.find(key).unwrap();
                    page.remove(idx);
                });
                page.max().to_vec()
            };

            let peer_max = {
                let mut peer = self.page_mut(peer_id).unwrap();
                copy.iter().skip(half).for_each(|(key, val, p)| {
                    trace!("split: move k={} v={} p={} from {} to {}", hex(key), hex(val), *p, id, peer_id);
                    if *p == 0 {
                        peer.put_val(key, val);
                    } else {
                        peer.put_ref(key, *p);
                    }
                });
                peer.max().to_vec()
            };

            {
                let mut parent = self.page_mut(parent_id).unwrap();
                let idx= parent.find(&max).unwrap();
                parent.remove(idx);
                parent.put_ref(&page_max, id);
                parent.put_ref(&peer_max, peer_id);
            }

            self.flush(parent_id)?;
            self.flush(peer_id)?;
            self.flush(id)?;

            Ok(())
        }
    }

    fn merge(&self, src_id: u32, dst_id: u32) -> Result<()> {
        let (src_copy, parent_id) = {
            let page = self.page(src_id).unwrap();
            (page.copy(), page.parent())
        };

        {
            let mut page = self.page_mut(dst_id).unwrap();
            for (key, val, p) in src_copy {
                trace!("merge: move k={} v={} p={} from {} to {}", hex(&key), hex(&val), p, src_id, dst_id);
                if p == 0 {
                    page.put_val(&key, &val);
                } else {
                    page.put_ref(&key, p);
                }
            }
            page.max().to_vec()
        };

        {
            let mut page = self.page_mut(src_id).unwrap();
            page.clear();
        }

        self.flush(parent_id)?;
        self.flush(dst_id)?;
        self.flush(src_id)?;

        self.free_id(src_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::block::Block;
    use rand::prelude::StdRng;
    use rand::{RngCore, SeedableRng, thread_rng};
    use std::ops::Deref;
    use std::borrow::Borrow;
    use crate::util::hex::hex;
    use rand::seq::SliceRandom;

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
        let path = Path::new("target/test_file.tmp");
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

        let root = file.root();
        assert_eq!(root.copy(), vec![]);
    }

    #[test]
    fn test_split() {
        let path = Path::new("target/test_split.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let size: u32 = 256;
        let mut file: File<Block> = File::make(path, size).unwrap();

        let count = 25;
        let data = (0..count)
            .into_iter()
            .map(|i| {
                let c = 'a' as u8 + (i % ('z' as u8 - 'a' as u8 + 1) as u8 as u64) as u8;
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
        let mut file: File<Block> = File::make(path, size).unwrap();

        let count = 25;
        let data = {
            let mut rng = StdRng::seed_from_u64(3);
            let mut result = (0..count)
                .into_iter()
                .map(|i| {
                    let c = 'a' as u8 + (i % ('z' as u8 - 'a' as u8 + 1) as u8 as u64) as u8;
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
        debug!("{}", dump(&file));

        let keys = {
            let mut rng = StdRng::seed_from_u64(3);
            let mut result = data.iter()
                .map(|(k, _)| k)
                .cloned()
                .collect::<Vec<_>>();
            result.shuffle(&mut rng);
            result
        };

        let mut removed = HashSet::with_capacity(size as usize);
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
        debug!("{}", dump(&file));

        let root = file.root();
        let copy = root.copy();
        assert_eq!(copy, vec![]);
    }

    #[test]
    fn test_1k() {
        let mut rng = thread_rng();

        let path = Path::new("target/test_1k.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }

        let size: u32 = 4096;
        let mut file: File<Block> = File::make(path, size).unwrap();

        let count = 1000;
        let data = (0..count)
            .into_iter()
            .map(|_| {
                (
                    rng.next_u64().to_be_bytes().to_vec(),
                    rng.next_u64().to_be_bytes().to_vec(),
                )
            })
            .collect::<Vec<_>>();

        for (i, (k, v)) in data.iter().enumerate() {
            debug!("({:05}) insert: key={} val={}", i, hex(k), hex(v));
            file.insert(k, v).unwrap();
        }

        for (k, v) in data.iter() {
            assert_eq!(file.lookup(k).unwrap().unwrap().deref(), v);
        }

        for (i, (key, _)) in data.iter().enumerate() {
            debug!("({:05}) remove: key={}", i, hex(key));
            file.remove(key).unwrap();
        }

        for (i, (key, _)) in data.iter().enumerate() {
            debug!("({:05}) lookup: key={}", i, hex(key));
            let found = file.lookup(key).unwrap()
                .map(|v| v.borrow().to_vec())
                .map(|v| hex(&v));
            assert_eq!(found, None);
        }

        let copy = file.root().copy();
        debug!("{}", dump(&file));
        assert!(copy.is_empty());
    }

    fn dump<P: Page>(file: &File<P>) -> String {
        fn dump_page<P: Page>(file: &File<P>, id: u32, acc: &mut String, prefix: String, tab: String) {
            if id == 0 {
                return;
            } else {
                let page = file.page(id).unwrap();
                let copy = page.copy();

                acc.push_str(&if copy.is_empty() {
                    format!("{}page={}: empty", prefix, id)
                } else {
                    let entries = copy.iter()
                        .map(|(k, v, p)| format!("{}{}, {}, {}", prefix, hex(&k), hex(&v), p))
                        .collect::<Vec<_>>()
                        .join("\n");
                    format!("{}page={}:\n{}", prefix, id, entries)
                });

                acc.push('\n');
                let links = copy.iter()
                    .map(|(_, _, p)| p)
                    .cloned()
                    .collect::<Vec<_>>();

                links.into_iter()
                    .for_each(|id| {
                        let mut p = prefix.clone();
                        p.push_str(&tab);
                        dump_page(file, id, acc, p, tab.clone());
                    });
            }
        }

        let mut acc = String::with_capacity(1024);
        dump_page(file,ROOT, &mut acc, "".to_string(), "\t".to_string());
        acc
    }

    // TODO Test with key bigger than page size (won't fit any page)
}
