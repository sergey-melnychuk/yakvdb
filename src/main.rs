use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::path::Path;

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

#[derive(Serialize, Deserialize)]
struct TableEntry {
    key: Vec<u8>,
    val: Vec<u8>,
}

impl TableEntry {
    fn new(key: Vec<u8>, val: Vec<u8>) -> Self {
        Self {
            key,
            val,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct IndexEntry {
    key: Vec<u8>,
    loc: Location,
}

impl IndexEntry {
    fn new(key: Vec<u8>, loc: Location) -> Self {
        Self {
            key,
            loc
        }
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

struct InMemory {
    index: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl KVStore for InMemory {
    fn lookup(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.index.get(key).map(|val| val.clone())
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) {
        self.index.insert(key.to_vec(), val.to_vec());
    }

    fn remove(&mut self, key: &[u8]) {
        self.index.remove(key);
    }
}

struct Durable<S: Storage> {
    index: BTreeMap<Vec<u8>, Location>,
    storage: S,
}

impl<S: Storage> KVStore for Durable<S> {
    fn lookup(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let loc = self.index.get(key)?;
        self.storage.get(loc).ok()
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) {
        let entry = TableEntry::new(key.to_vec(), val.to_vec());
        let buf = bincode::serialize(&entry).unwrap();
        let mut loc = self.storage.put(&buf).unwrap();
        loc.pos += 8*2 + key.len() as u32;
        loc.len = val.len() as u32;
        self.index.insert(key.to_vec(), loc);
    }

    fn remove(&mut self, key: &[u8]) {
        // TODO create TableEntry representing removal of the key
        self.index.remove(key);
    }
}

impl<S: Storage> Durable<S> {
    fn new(storage: S) -> Self {
        Self {
            index: BTreeMap::new(),
            storage,
        }
    }
}

trait Storage {
    fn get(&mut self, loc: &Location) -> io::Result<Vec<u8>>;
    fn put(&mut self, val: &[u8]) -> io::Result<Location>;
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
            let existing_files: HashMap<FileId, File> = fs::read_dir(path)?
                .into_iter()
                .filter_map(|r| r.ok().map(|d| d.path()))
                .filter(|p| p.is_file() && p.ends_with(".dat"))
                .map(|p| {
                    let name = p
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .split(".")
                        .next()
                        .unwrap();
                    let id: u32 = u32::from_str_radix(name, 16).unwrap();
                    let file = OpenOptions::new().read(true).write(true).open(p).unwrap();
                    (FileId(id), file)
                })
                .collect();

            // TODO ingest data from discovered .idx files

            existing_files
        };

        Ok(Self {
            base: base.to_string(),
            files,
        })
    }

    fn name(file_id: &FileId) -> String {
        format!("{:08x}.dat", file_id.0)
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

    // TODO Impl merging non-active (immutable) files into single one with extra index file next to it
    fn merge() {
        todo!()
    }

    fn file(&mut self, file_id: &FileId) -> io::Result<&mut File> {
        let path = Path::new(&self.base).join(OnDisk::name(file_id));
        if !self.files.contains_key(file_id) {
            let mut opt = OpenOptions::new();
            let file = if path.exists() {
                let mut old = opt.read(true).write(true).open(path)?;
                old.seek(SeekFrom::End(0))?;
                old
            } else {
                let mut new = opt.read(true).write(true).create(true).open(path)?;
                new.seek(SeekFrom::Start(0))?;
                new
            };
            self.files.insert(file_id.clone(), file);
        }
        Ok(self.files.get_mut(file_id).unwrap())
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
                format!("too long value (max length is {} bytes)", u16::MAX),
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
        let entry = TableEntry::new(
            b"what is the answer to life?".to_vec(),
            b"it's 42".to_vec());

        let buf = bincode::serialize(&entry).unwrap();
        assert_eq!(&buf[8..12], b"what");
        assert_eq!(&buf[(8 + entry.key.len() + 8)..(8 + entry.key.len() + 8 + 7)], b"it's 42");
        let e: TableEntry = bincode::deserialize(&buf).unwrap();

        assert_eq!(e.key, entry.key);
        assert_eq!(e.val, entry.val);
    }

    #[test]
    fn test_index_entry() {
        let entry = IndexEntry::new(
            b"what is the answer to life?".to_vec(),
            Location::of(FileId(42), 123, 456));

        let buf = bincode::serialize(&entry).unwrap();
        let e: IndexEntry = bincode::deserialize(&buf).unwrap();

        assert_eq!(e.key, entry.key);
        assert_eq!(e.loc, entry.loc);
    }

    #[test]
    fn test_durable() {
        let disk = OnDisk::new("target/db").unwrap();
        let mut db = Durable::new(disk);
        let key = b"sup, doc?";
        let val = b"not much, thanks";

        assert!(db.lookup(key).is_none());

        db.insert(key, val);
        assert_eq!(db.lookup(key), Some(val.to_vec()));

        db.remove(key);
        assert!(db.lookup(key).is_none());
    }
}

fn main() {
    println!("Work In Progress");
}
