use crate::storage::block::Block;
use crate::storage::page::Page;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

struct File {
    file: fs::File,
}

impl File {
    fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        Ok(Self { file })
    }

    fn load(&mut self, offset: usize, length: usize) -> io::Result<impl Page> {
        let mut page = Block::reserve(length);
        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.read_exact(page.as_mut())?;
        Ok(page)
    }

    fn save<P: Page + AsRef<[u8]>>(&mut self, page: &P) -> io::Result<()> {
        let offset = page.id() as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(page.as_ref())
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
    fn test_file() -> io::Result<()> {
        let path = Path::new("target/file_test.tmp");
        if path.exists() {
            fs::remove_file(path)?;
        }
        let size: u32 = 256;

        {
            let mut page = Block::create(0, size);
            page.put_val(b"ddd", b"123123123");
            page.put_val(b"ccc", b"qweqweqwe");
            page.put_val(b"bbb", b"asdasdasd");
            page.put_val(b"aaa", b"zxczxczxc");
            page.put_ref(b"zzz", 1111);
            page.put_ref(b"yyy", 2222);
            page.put_ref(b"xxx", 3333);

            let mut file = File::open(path)?;
            file.save(&page).unwrap();
        }

        let mut page = {
            let mut file = File::open(path)?;
            file.load(0, size as usize).unwrap()
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

        Ok(())
    }
}
