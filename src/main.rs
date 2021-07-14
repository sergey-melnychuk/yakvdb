#![allow(dead_code)]

pub(crate) mod storage;
pub(crate) mod util;
pub(crate) mod api;

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
struct FileId(u32);

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Location {
    file_id: FileId,
    pos: u32,
    len: u32,
}

impl Location {
    fn of(file_id: FileId, pos: u32, len: u32) -> Self {
        Self { file_id, pos, len }
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
enum TableEntry {
    Insert { key: Vec<u8>, val: Vec<u8> },
    Remove { key: Vec<u8> },
}

impl TableEntry {
    fn insert(key: Vec<u8>, val: Vec<u8>) -> Self {
        Self::Insert { key, val }
    }

    fn remove(key: Vec<u8>) -> Self {
        Self::Remove { key }
    }
}

#[derive(Serialize, Deserialize)]
struct IndexEntry {
    key: Vec<u8>,
    loc: Location,
}

impl IndexEntry {
    fn new(key: Vec<u8>, loc: Location) -> Self {
        Self { key, loc }
    }
}

trait KVStore {
    fn lookup(&mut self, key: &[u8]) -> Option<Vec<u8>>;
    fn insert(&mut self, key: &[u8], val: &[u8]);
    fn remove(&mut self, key: &[u8]);
}

trait KVIterator {
    fn keys(&self, bound: Bound<&[u8]>) -> dyn Iterator<Item = Vec<u8>>;
    fn entries(&self, bound: Bound<&[u8]>) -> dyn Iterator<Item = (Vec<u8>, Vec<u8>)>;
}

struct Durable<S: Storage, I: Index> {
    storage: S,
    index: I,
}

impl<S: Storage, I: Index> KVStore for Durable<S, I> {
    fn lookup(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let loc = self.index.lookup(key)?;
        self.storage.get(&loc).ok()
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) {
        let entry = TableEntry::insert(key.to_vec(), val.to_vec());
        let buf = bincode::serialize(&entry).unwrap();
        let mut loc = self.storage.put(&buf).unwrap();
        loc.pos += 8 + key.len() as u32 + 8 + 4;
        loc.len = val.len() as u32;
        self.index.insert(key, loc);
    }

    fn remove(&mut self, key: &[u8]) {
        let entry = TableEntry::remove(key.to_vec());
        let buf = bincode::serialize(&entry).unwrap();
        self.storage.put(&buf).unwrap();
        self.index.remove(key);
    }
}

impl<S: Storage, I: Index> Durable<S, I> {
    fn new(storage: S, index: I) -> Self {
        Self { storage, index }
    }
}

trait Storage {
    fn get(&mut self, loc: &Location) -> io::Result<Vec<u8>>;
    fn put(&mut self, val: &[u8]) -> io::Result<Location>;
}

trait Index {
    fn lookup(&mut self, key: &[u8]) -> Option<Location>;
    fn insert(&mut self, key: &[u8], loc: Location);
    fn remove(&mut self, key: &[u8]);
}

#[derive(Default)]
struct InMemory {
    index: BTreeMap<Vec<u8>, Location>,
}

impl Index for InMemory {
    fn lookup(&mut self, key: &[u8]) -> Option<Location> {
        self.index.get(key).cloned()
    }

    fn insert(&mut self, key: &[u8], loc: Location) {
        self.index.insert(key.to_vec(), loc);
    }

    fn remove(&mut self, key: &[u8]) {
        self.index.remove(key);
    }
}

struct OnDisk {
    base: String,
    files: HashMap<FileId, File>,
}

impl Default for OnDisk {
    fn default() -> Self {
        Self {
            base: ".".to_string(),
            files: HashMap::with_capacity(16),
        }
    }
}

impl OnDisk {
    fn new(base: &str) -> io::Result<Self> {
        let path = Path::new(base);
        let files = if !path.exists() {
            fs::create_dir_all(path)?;
            HashMap::with_capacity(16)
        } else {
            OnDisk::list(path, ".dat")?
                .map(|p| {
                    let id = OnDisk::id(&p);
                    let file = OnDisk::open(&p).unwrap();
                    (id, file)
                })
                .collect::<HashMap<_, _>>()
        };

        Ok(Self {
            base: base.to_string(),
            files,
        })
    }

    fn name(file_id: &FileId) -> String {
        format!("{:08x}.dat", file_id.0)
    }

    fn id(path: &PathBuf) -> FileId {
        let name = path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split(".")
            .next()
            .unwrap();
        let id: u32 = u32::from_str_radix(name, 16).unwrap();
        FileId(id)
    }

    fn list(path: &Path, suffix: &str) -> io::Result<impl Iterator<Item = PathBuf>> {
        let owned_suffix = suffix.to_owned();
        Ok(fs::read_dir(path)?
            .into_iter()
            .filter_map(|r| r.ok().map(|d| d.path()))
            .filter(move |p| {
                p.is_file()
                    && p.file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .ends_with(&owned_suffix)
            }))
    }

    fn append(file: &mut File, val: &[u8]) -> io::Result<u32> {
        let offset = file.stream_position()? as u32;
        file.write_all(val)?;
        Ok(offset)
    }

    fn read(file: &mut File, buf: &mut [u8], offset: u32) -> io::Result<()> {
        file.seek(SeekFrom::Start(offset as u64))?;
        file.read_exact(buf)
    }

    fn open(path: &Path) -> io::Result<File> {
        let mut opt = OpenOptions::new();
        let file = if path.exists() {
            let mut old = opt.read(true).write(true).open(path)?;
            old.seek(SeekFrom::End(0))?;
            old
        } else {
            let mut new = opt.read(true).write(true).create(true).open(path)?;

            // Placeholder for number of entries in the file - required by bincode to deserialize Vec<*Entry>
            new.write_all(&[0, 0, 0, 0, 0, 0, 0, 0])?;

            new
        };
        Ok(file)
    }

    fn file(&mut self, file_id: &FileId) -> io::Result<&mut File> {
        let path = Path::new(&self.base).join(OnDisk::name(file_id));
        if !self.files.contains_key(file_id) {
            let file = OnDisk::open(&path)?;
            self.files.insert(file_id.clone(), file);
        }
        Ok(self.files.get_mut(file_id).unwrap())
    }

    fn entries<T: DeserializeOwned>(
        path: &Path,
        suffix: &str,
    ) -> io::Result<impl Iterator<Item = T>> {
        Ok(OnDisk::list(path, suffix)?.flat_map(|p| {
            let br = BufReader::new(File::open(p).unwrap());
            let entries: Vec<T> = bincode::deserialize_from(br).unwrap();
            entries
        }))
    }
}

impl Storage for OnDisk {
    fn get(&mut self, loc: &Location) -> io::Result<Vec<u8>> {
        let file = self.file(&loc.file_id)?;
        let mut buf = vec![0; loc.len as usize];
        OnDisk::read(file, &mut buf[..], loc.pos)?;
        Ok(buf)
    }

    fn put(&mut self, val: &[u8]) -> io::Result<Location> {
        if val.len() > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("value too long (max length is {} bytes)", u16::MAX),
            ));
        }

        let file_id = FileId(0);
        let file = self.file(&file_id)?;
        let offset = OnDisk::append(file, val)?;
        let length = val.len() as u32;
        Ok(Location::of(file_id, offset, length))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_file_name() {
        assert_eq!(OnDisk::name(&FileId(42)), "0000002a.dat");
    }

    #[test]
    fn test_table_entry() {
        let key = b"what is the answer to life?";
        let val = b"it's 42";
        let entry = TableEntry::insert(key.to_vec(), val.to_vec());

        let buf = bincode::serialize(&entry).unwrap();
        assert_eq!(&buf[12..16], b"what");

        // 8 bytes for key len
        // 8 bytes for val len
        // 4 bytes for enum variant (insert/remove)
        let n = 8 + key.len() + 8 + 4;
        assert_eq!(&buf[n..(n + 7)], b"it's 42");

        if let TableEntry::Insert { key: k, val: v } = bincode::deserialize(&buf).unwrap() {
            assert_eq!(k, key);
            assert_eq!(v, val);
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_index_entry() {
        let entry = IndexEntry::new(
            b"what is the answer to life?".to_vec(),
            Location::of(FileId(42), 123, 456),
        );

        let buf = bincode::serialize(&entry).unwrap();
        let e: IndexEntry = bincode::deserialize(&buf).unwrap();

        assert_eq!(e.key, entry.key);
        assert_eq!(e.loc, entry.loc);
    }

    #[test]
    fn test_durable() {
        let disk = OnDisk::new("target/db").unwrap();
        let mem = InMemory::default();
        let mut db = Durable::new(disk, mem);

        let key = b"sup, doc?";
        let val = b"not much, thanks";
        assert!(db.lookup(key).is_none());

        db.insert(key, val);
        assert_eq!(db.lookup(key), Some(val.to_vec()));

        db.remove(key);
        assert!(db.lookup(key).is_none());
    }

    #[test]
    fn test_table_entries() {
        let entries = vec![
            TableEntry::Insert {
                key: b"qweqwe-asdasd-zxcxzc".to_vec(),
                val: b"123123-567576-8890890".to_vec(),
            },
            TableEntry::Insert {
                key: b"asdasd-qweqwe-zxcxzc".to_vec(),
                val: b"567576-123123-8890890".to_vec(),
            },
            TableEntry::Insert {
                key: b"zxcxzc-qweqwe-asdasd".to_vec(),
                val: b"8890890-123123-567576".to_vec(),
            },
            TableEntry::Remove {
                key: b"asdasd-qweqwe-zxcxzc".to_vec(),
            },
        ];

        let path = Path::new("target/test_table_entries");
        if !path.exists() {
            fs::create_dir_all(path).unwrap();
        }

        OnDisk::list(path, ".dat")
            .unwrap()
            .for_each(|d| fs::remove_file(d.as_path()).unwrap());

        let disk = OnDisk::new("target/test_table_entries").unwrap();
        let mem = InMemory::default();
        let mut db = Durable::new(disk, mem);

        for e in &entries {
            match e {
                TableEntry::Insert { key, val } => db.insert(key, val),
                TableEntry::Remove { key } => db.remove(key),
            }
        }

        {
            // Manually patch the length of entries vector to deserialize
            // Bincode writes collection length prefix as u64, does not provide an easy way to just parse stream of entries from file
            let mut f = OpenOptions::new()
                .write(true)
                .open(Path::new("target/test_table_entries/00000000.dat"))
                .unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&[entries.len() as u8]).unwrap();
        }

        let deserialized: Vec<TableEntry> =
            OnDisk::entries(path, ".dat").unwrap().into_iter().collect();

        assert_eq!(deserialized, entries);
    }
}

fn main() {
    println!("Work In Progress");
}
