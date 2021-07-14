use crate::api::page::Page;
use crate::api::tree::Tree;
use crate::disk::block::Block;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::collections::{HashSet, HashMap, BinaryHeap};
use std::cmp::Reverse;
use std::mem::size_of;
use bytes::{BytesMut, Buf, BufMut};
use std::cell::RefCell;

struct File {
    /// Underlying file reference where all data is physically stored.
    file: RefCell<fs::File>,
    head: Head,

    /// In-memory page cache. All page access happens only through cached page representation.
    cache: HashMap<u32, Block>,

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

impl File {
    fn make(path: &Path, page_bytes: u32) -> io::Result<Self> {
        if path.exists() {
            return Err(io::Error::new(io::ErrorKind::Other, format!("File exists: {:?}", path)));
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

        let root = Block::create(ROOT, head.page_bytes);
        buf.put_slice(root.as_ref());

        file.write_all(buf.as_ref())?;
        file.flush()?;

        Ok(Self {
            file: RefCell::new(file),
            head,
            cache: HashMap::with_capacity(32),
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
            return Err(io::Error::new(io::ErrorKind::Other, format!("MAGIC mismatch: {:?}", magic)));
        }

        let head = Head {
            page_bytes: buf.get_u32(),
            page_count: buf.get_u32(),
        };

        if head.page_bytes > u16::MAX as u32 {
            return Err(io::Error::new(io::ErrorKind::Other, format!("Page size too large: {}", head.page_bytes)));
        }

        if len < HEAD + head.page_bytes as usize {
            return Err(io::Error::new(io::ErrorKind::Other, format!("File does not contain one full page")));
        }

        let mut root = Block::reserve(head.page_bytes);
        file.read_exact(root.as_mut())?;

        let mut this = Self {
            file: RefCell::new(file),
            head,
            cache: HashMap::with_capacity(32),
            dirty: HashSet::with_capacity(32),
            empty: BinaryHeap::with_capacity(16),
        };

        this.cache.insert(ROOT, root);

        Ok(this)
    }

    fn load(&self, offset: usize, length: u32) -> io::Result<Block> {
        let mut page = Block::reserve(length as u32);
        self.file.borrow_mut().seek(SeekFrom::Start(offset as u64))?;
        self.file.borrow_mut().read_exact(page.as_mut())?;
        Ok(page)
    }

    fn save(&self, page: &dyn Page) -> io::Result<()> {
        let offset = self.offset(page.id()) as u64;
        self.file.borrow_mut().seek(SeekFrom::Start(offset))?;
        self.file.borrow_mut().write_all(page.as_ref())
    }

    fn offset(&self, id: u32) -> usize {
        HEAD + (id - 1) as usize * self.head.page_bytes as usize
    }
}

impl Tree for File {
    fn lookup(&self, key: &[u8]) -> Option<&[u8]> {
        // Keeps track of visited pages to avoid possible circular reference navigation.
        let mut seen = HashSet::with_capacity(8);
        let mut page = self.root();
        seen.insert(page.id());
        loop {
            let idx = page.ceil(key)?;

            let slot = page.slot(idx)?;
            if slot.page == 0 {
                // Log how deep the lookup went into the tree depth: seen.len()
                return page.find(key)
                    .map(|idx| page.key(idx));
            } else {
                if seen.contains(&slot.page) {
                    // TODO log error: circular reference is detected between pages
                    return None;
                }
                seen.insert(slot.page);
                page = self.page(slot.page)?;
            }
        }
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) {
        let mut page = self.root_mut();
        let mut path = Vec::with_capacity(8);
        path.push(page.id());
        loop {
            let idx_opt = page.ceil(key);
            if idx_opt.is_none() {
                // TODO inserting max value - page needs to be updated
                return;
            }

            let idx = idx_opt.unwrap();

            let slot = page.slot(idx).unwrap();
            if slot.page == 0 {
                let fits = page.fits((key.len() + val.len()) as u32);
                if !fits {
                    // TODO this page needs to be split into two
                }
                page.put_val(key, val);
            } else {
                path.push(page.id());
                let page_opt = self.page_mut(slot.page);
                if page_opt.is_none() {
                    // TODO log error: referenced child page not found!
                    return;
                }

                page = page_opt.unwrap();
            }
        }
    }

    fn remove(&mut self, key: &[u8]) {
        let mut page = self.root_mut();
        let mut path = Vec::with_capacity(8);
        path.push(page.id());
        loop {
            let idx_opt = page.ceil(key);
            if idx_opt.is_none() {
                return;
            }

            let idx = idx_opt.unwrap();

            if page.key(idx) == page.max() {
                // TODO Override page's highest key, as current highest is being removed
            }

            let slot = page.slot(idx).unwrap();
            if slot.page == 0 {
                // TODO Check if the page become too small and needs to be merged into another one
                // TODO Check if the destination page (where current one can be merged) exists first
                page.remove(idx);
            } else {
                path.push(page.id());
                let page_opt = self.page_mut(slot.page);
                if page_opt.is_none() {
                    // TODO log error: referenced child page not found!
                    return;
                }

                page = page_opt.unwrap();
            }
        }
    }

    fn root(&self) -> &dyn Page {
        self.page(ROOT).unwrap()
    }

    fn page(&self, id: u32) -> Option<&dyn Page> {
        if !self.cache.contains_key(&id) {
            let mut page = Block::reserve(self.head.page_bytes);
            self.file.borrow_mut().read_exact(page.as_mut()).ok()?;
        }
        let page: &dyn Page = self.cache.get(&id).unwrap();
        // TODO Find a way to insert the page into a cache!
        Some(page)
    }

    fn root_mut(&mut self) -> &mut dyn Page {
        self.page_mut(ROOT).unwrap()
    }

    fn page_mut(&mut self, id: u32) -> Option<&mut dyn Page> {
        if !self.cache.contains_key(&id) {
            let page = self.load(self.offset(id), self.head.page_bytes).ok()?;
            self.cache.insert(id, page);
        }
        let page: &mut dyn Page = self.cache.get_mut(&id).unwrap();
        Some(page)
    }

    fn flush<P: Page>(&mut self, page: &P) -> crate::api::error::Result<()> {
        self.save(page).map_err(|e| e.into())
    }

    fn next_id(&mut self) {
        self.empty.peek().cloned().unwrap().0;
    }

    fn free_id(&mut self, id: u32) {
        self.empty.push(Reverse(id))
    }

    fn split<P: Page>(&mut self, _page: &P) -> (u32, u32) {
        todo!()
    }

    fn merge<P: Page>(&mut self, _this: &mut P, _that: &P) -> u32 {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(page: &dyn Page, key: &[u8]) -> Option<(Vec<u8>, u32)> {
        page.find(key)
            .map(|idx| (page.val(idx).to_vec(), page.slot(idx).unwrap().page))
    }

    #[test]
    fn test_file() {
        let path = Path::new("target/file_test.tmp");
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        let size: u32 = 256;

        {
            let mut file = File::make(path, size).unwrap();
            {
                let page = file.root_mut();
                page.put_val(b"ddd", b"123123123");
                page.put_val(b"ccc", b"qweqweqwe");
                page.put_val(b"bbb", b"asdasdasd");
                page.put_val(b"aaa", b"zxczxczxc");
                page.put_ref(b"zzz", 1111);
                page.put_ref(b"yyy", 2222);
                page.put_ref(b"xxx", 3333);
            };
            let page = file.root();
            file.save(page).unwrap();
        }

        let mut page = {
            let file = File::open(path).unwrap();
            file.load(file.offset(ROOT), size).unwrap()
        };

        assert_eq!(
            page.copy(),
            vec![
                (b"aaa".to_vec(), b"zxczxczxc".to_vec(), 0),
                (b"bbb".to_vec(), b"asdasdasd".to_vec(), 0),
                (b"ccc".to_vec(), b"qweqweqwe".to_vec(), 0),
                (b"ddd".to_vec(), b"123123123".to_vec(), 0),
                (b"xxx".to_vec(), vec![], 3333),
                (b"yyy".to_vec(), vec![], 2222),
                (b"zzz".to_vec(), vec![], 1111),
            ]
        );

        assert_eq!(get(&page, b"aaa"), Some((b"zxczxczxc".to_vec(), 0)));
        assert_eq!(get(&page, b"bbb"), Some((b"asdasdasd".to_vec(), 0)));
        assert_eq!(get(&page, b"ccc"), Some((b"qweqweqwe".to_vec(), 0)));
        assert_eq!(get(&page, b"ddd"), Some((b"123123123".to_vec(), 0)));
        assert_eq!(get(&page, b"xxx"), Some((vec![], 3333)));
        assert_eq!(get(&page, b"yyy"), Some((vec![], 2222)));
        assert_eq!(get(&page, b"zzz"), Some((vec![], 1111)));

        page.remove(page.find(b"aaa").unwrap());
        assert_eq!(get(&page, b"aaa"), None);

        page.remove(page.find(b"zzz").unwrap());
        assert_eq!(get(&page, b"zzz"), None);
    }
}
