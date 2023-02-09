use log::{debug, error, info, trace};
use pickledb::{PickleDb, PickleDbDumpPolicy, SerializationMethod};
use rusqlite::Connection;
use redb::{Database, ReadableTable, TableDefinition};
use sled::Db;
use std::convert::TryInto;
use std::path::Path;
use std::time::SystemTime;

use yakvdb::api::tree::Tree;
use yakvdb::disk::block::Block;
use yakvdb::disk::file::File;
use yakvdb::util::{self, hex::hex};

trait Storage {
    fn insert(&mut self, key: &[u8], val: &[u8]);
    fn remove(&mut self, key: &[u8]);
    fn lookup(&self, key: &[u8]) -> Option<Vec<u8>>;
    // fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<Vec<u8>>;
    // fn min(&self) -> Vec<u8>;
    // fn max(&self) -> Vec<u8>;
    // fn len(&self) -> usize;
}

struct LiteStorage(Connection);

impl Storage for LiteStorage {
    fn insert(&mut self, key: &[u8], val: &[u8]) {
        self.0
            .execute("INSERT INTO db (key, val) VALUES (?1, ?2)", [key, val])
            .unwrap();
    }

    fn remove(&mut self, key: &[u8]) {
        self.0
            .execute("DELETE FROM db WHERE key = ?1", [key])
            .unwrap();
    }

    fn lookup(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut stmt = self.0.prepare("SELECT val FROM db WHERE key = ?1").unwrap();
        let mut rows = stmt.query([key]).unwrap();
        rows.next()
            .unwrap()
            .map(|row| row.get_ref_unwrap(0).as_blob().unwrap().to_vec())
    }
}

struct SledStorage(sled::Db);

impl Storage for SledStorage {
    fn insert(&mut self, key: &[u8], val: &[u8]) {
        self.0.insert(key, val).unwrap();
        //self.0.flush().unwrap();
    }

    fn remove(&mut self, key: &[u8]) {
        self.0.remove(key).unwrap();
        //self.0.flush().unwrap();
    }

    fn lookup(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.get(key).unwrap().map(|x| x.as_ref().to_vec())
    }
}

struct SelfStorage(File<Block>);

impl Storage for SelfStorage {
    fn insert(&mut self, key: &[u8], val: &[u8]) {
        self.0.insert(key, val).unwrap();
    }

    fn remove(&mut self, key: &[u8]) {
        self.0.remove(key).unwrap();
    }

    fn lookup(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.lookup(key).unwrap().map(|x| x.to_vec())
    }
}

struct PickleStorage(PickleDb);

// TODO FIXME (too slow!) redo with pickle-db best practices in mind
impl Storage for PickleStorage {
    fn insert(&mut self, key: &[u8], val: &[u8]) {
        self.0
            .set(&String::from_utf8_lossy(key), &val.to_vec())
            .unwrap();
    }

    fn remove(&mut self, key: &[u8]) {
        self.0.rem(&String::from_utf8_lossy(key)).unwrap();
    }

    fn lookup(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.get::<Vec<u8>>(&String::from_utf8_lossy(key))
    }
}

struct RedbStorage<'a> {
    db: Database, 
    td: TableDefinition<'a, u64, u64>,
}

// TODO FIXME (too slow!) redo with redb best practices in mind
impl Storage for RedbStorage<'_> {
    fn insert(&mut self, key: &[u8], val: &[u8]) {
        let key = u64::from_le_bytes(key.try_into().unwrap());
        let val = u64::from_le_bytes(val.try_into().unwrap());

        let txn = self.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(self.td).unwrap();
            table.insert(key, val).unwrap();
        }
        txn.commit().unwrap();
    }

    fn remove(&mut self, key: &[u8]) {
        let key = u64::from_le_bytes(key.try_into().unwrap());

        let txn = self.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(self.td).unwrap();
            table.remove(key).unwrap();
        }
        txn.commit().unwrap();
    }

    fn lookup(&self, key: &[u8]) -> Option<Vec<u8>> {
        let key = u64::from_le_bytes(key.try_into().unwrap());

        let txn = self.db.begin_read().unwrap();
        let table = txn.open_table(self.td).unwrap();
        table.get(&key).unwrap()
            .map(|g| g.value().to_le_bytes().to_vec())
    }
}

fn benchmark<S: Storage>(mut storage: S, count: usize) {
    let data = util::data(count, 42);

    let mut now = SystemTime::now();
    for (k, v) in data.iter() {
        debug!("insert: key='{}' val='{}'", hex(k), hex(v));
        storage.insert(k, v);
    }
    let mut millis = now.elapsed().unwrap_or_default().as_millis();
    info!(
        "insert: {} ms (rate={} op/s)",
        millis,
        count as u128 * 1000 / millis.max(1)
    );

    now = SystemTime::now();
    let mut found = Vec::with_capacity(data.len());
    for (k, _) in data.iter() {
        if let Some(r) = storage.lookup(k) {
            let val = r.to_vec();
            found.push(val);
        } else {
            error!("key='{}' not found", hex(k));
        }
    }
    millis = now.elapsed().unwrap_or_default().as_millis();
    info!(
        "lookup: {} ms (rate={} op/s)",
        millis,
        count as u128 * 1000 / millis.max(1)
    );

    let mut errors = 0;
    for ((k, v), r) in data.iter().zip(found.iter()) {
        if v != r {
            trace!(
                "ERROR: key='{}': expected '{}' but got '{}'",
                hex(k),
                hex(v),
                hex(r)
            );
            errors += 1;
        }
    }
    if errors > 0 {
        error!("lookup errors: {}", errors);
    }

    /*
    let min = storage.min();
    let max = storage.max();

    now = SystemTime::now();
    info!("iter: min={} max={}", hex(&min), hex(&max));
    let mut this = min.clone();
    let mut n = 1usize;
    loop {
        if let Ok(Some(r)) = file.above(&this) {
            n += 1;
            let next = r.to_vec();
            if next <= this {
                error!(
                    "iter:  asc order violated: {} comes before {} (n={})",
                    hex(&this),
                    hex(&next),
                    n
                );
                break;
            }
            this = next;
        } else if this < max || n < data.len() {
            error!("iter: failed to call above={} (n={})", hex(&this), n);
            break;
        } else {
            break;
        }
    }
    if let Some(x) = file.below(&min).unwrap() {
        error!("below min returned {}", hex(x.as_ref()));
    }
    millis = now.elapsed().unwrap_or_default().as_millis();
    info!(
        "iter:  asc {} ms (rate={} op/s) n={}",
        millis,
        count as u128 * 1000 / millis,
        n
    );

    now = SystemTime::now();
    let mut this = max.clone();
    let mut n = 1usize;
    loop {
        if let Ok(Some(r)) = storage.below(&this) {
            n += 1;
            let next = r.to_vec();
            if next >= this {
                error!(
                    "iter: desc order violated: {} comes before {} (n={})",
                    hex(&this),
                    hex(&next),
                    n
                );
                break;
            }
            this = next;
        } else if this > min || n < data.len() {
            error!("iter: failed to call below={} (n={})", hex(&this), n);
            break;
        } else {
            break;
        }
    }
    if let Some(x) = file.above(&max).unwrap() {
        error!("above max returned {}", hex(x.as_ref()));
    }
    millis = now.elapsed().unwrap_or_default().as_millis();
    info!(
        "iter: desc {} ms (rate={} op/s) n={}",
        millis,
        count as u128 * 1000 / millis,
        n
    );
    */

    now = SystemTime::now();
    for (key, _) in util::shuffle(data, 42).iter() {
        storage.remove(key);
        let opt = storage.lookup(key);
        if let Some(r) = opt {
            error!("key='{}' not removed", hex(r.as_ref()));
        }
    }
    millis = now.elapsed().unwrap_or_default().as_millis();
    info!(
        "remove: {} ms (rate={} op/s)",
        millis,
        count as u128 * 1000 / millis.max(1)
    );

    // if !storage.len() > 0 {
    //     error!("non-empty file");
    // }
}

fn main() {
    env_logger::init();
    let mut it = std::env::args().skip(1);
    let target = it.next().unwrap();
    let count = it
        .next()
        .and_then(|x| x.parse::<usize>().ok())
        .unwrap_or(1000);

    if target == "self" {
        let path = Path::new("target/main_1M.tmp");
        let size: u32 = 4096;
        let file: File<Block> = if path.exists() {
            File::open(path).unwrap()
        } else {
            File::make(path, size).unwrap()
        };
        info!(
            "target={} file={:?} count={} page={}",
            target, path, count, size
        );

        benchmark(SelfStorage(file), count);
    }

    if target == "sled" {
        let path = "target/sled_1M";
        let db: Db = sled::open(path).unwrap();
        info!("target={} file={} count={}", target, path, count);

        benchmark(SledStorage(db), count);
    }

    if target == "lite" {
        let path = "target/lite_1M";
        let db = Connection::open(path).unwrap();
        info!("target={} file={} count={}", target, path, count);

        db.execute("DROP TABLE IF EXISTS db", ()).unwrap();
        db.execute(
            "CREATE TABLE IF NOT EXISTS db (key BLOB PRIMARY KEY, val BLOB)",
            (),
        )
        .unwrap();

        benchmark(LiteStorage(db), count);
    }

    if target == "pickle" {
        // https://github.com/seladb/pickledb-rs
        let path = "target/pickle_1M.db";
        let db = PickleDb::new(path, PickleDbDumpPolicy::AutoDump, SerializationMethod::Bin);
        info!("target={} file={} count={}", target, path, count);

        benchmark(PickleStorage(db), count);
    }

    if target == "redb" {
        // https://github.com/cberner/redb
        let path = "target/redb_1M.db";
        let db = Database::create("target/redb_1M.bin").unwrap();
        let td: TableDefinition<u64, u64> = TableDefinition::new("data");

        info!("target={} file={} count={}", target, path, count);
        benchmark(RedbStorage { db, td }, count);
    }
}
