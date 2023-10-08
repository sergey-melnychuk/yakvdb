use log::{debug, error, info, trace};
use sled::Db;
use std::cell::RefCell;
use std::path::Path;
use std::time::SystemTime;
use yakvdb::api::Store;

use yakvdb::disk::block::Block;
use yakvdb::disk::file::File;
use yakvdb::util::{self, hex::hex};

trait Storage {
    fn insert(&self, key: &[u8], val: &[u8]);
    fn remove(&self, key: &[u8]);
    fn lookup(&self, key: &[u8]) -> Option<Vec<u8>>;
    // fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<Vec<u8>>;
    // fn min(&self) -> Vec<u8>;
    // fn max(&self) -> Vec<u8>;
    // fn len(&self) -> usize;
}

struct SledStorage(sled::Db);

impl Storage for SledStorage {
    fn insert(&self, key: &[u8], val: &[u8]) {
        self.0.insert(key, val).unwrap();
        //self.0.flush().unwrap();
    }

    fn remove(&self, key: &[u8]) {
        self.0.remove(key).unwrap();
        //self.0.flush().unwrap();
    }

    fn lookup(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.get(key).unwrap().map(|x| x.as_ref().to_vec())
    }
}

struct SelfStorage(File<Block>);

impl Storage for SelfStorage {
    fn insert(&self, key: &[u8], val: &[u8]) {
        self.0.insert(key, val).unwrap();
    }

    fn remove(&self, key: &[u8]) {
        self.0.remove(key).unwrap();
    }

    fn lookup(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.lookup(key).unwrap().map(|x| x.to_vec())
    }
}

struct LSKV(RefCell<yalskv::Store>);

impl Storage for LSKV {
    fn insert(&self, key: &[u8], val: &[u8]) {
        self.0.borrow_mut().insert(key, val).ok();
    }

    fn remove(&self, key: &[u8]) {
        self.0.borrow_mut().remove(key).ok();
    }

    fn lookup(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.borrow_mut().lookup(key).ok().flatten()
    }
}

fn benchmark<S: Storage>(storage: S, count: usize) {
    let mut now = SystemTime::now();
    let data = util::data(count, 42);
    let mut millis = now.elapsed().unwrap_or_default().as_millis();
    info!("values: {millis} ms");

    now = SystemTime::now();
    for (k, v) in data.iter() {
        debug!("insert: key='{}' val='{}'", hex(k), hex(v));
        storage.insert(k, v);
    }
    millis = now.elapsed().unwrap_or_default().as_millis();
    info!(
        "insert: {} ms (rate={} op/s)",
        millis,
        count as u128 * 1000 / millis.max(1)
    );

    now = SystemTime::now();
    let mut found = Vec::with_capacity(data.len());
    for (k, _) in data.iter() {
        let val = storage.lookup(k).unwrap_or_default();
        found.push(val);
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

mod sharded {
    use std::io;

    use yakvdb::{disk::{file::File, block::Block}, api::{error::Error, Store}};

    struct Shard {
        file: File<Block>,
    }

    impl Shard {
        fn new(path: &str) -> io::Result<Self> {
            let path = std::path::Path::new(path);
            let file = if path.exists() {
                File::open(path)?
            } else {
                File::make(path, 4096)?
            };
            Ok(Self { file })
        }

        fn lookup(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
            self.file.lookup(key)
        }

        fn insert(&self, key: &[u8], val: &[u8]) -> Result<(), Error> {
            self.file.insert(key, val)
        }

        fn remove(&self, key: &[u8]) -> Result<(), Error> {
            self.file.remove(key)
        }
    }

    pub struct ShardedStore {
        num_shards: u8,
        shards: Vec<Shard>,
    }

    impl ShardedStore {
        pub fn new(num_shards: u8, base_path: &str) -> Self {
            let shards = (0..num_shards).into_iter()
                .map(|id| format!("{base_path}/{id:#04x}.db"))
                .map(|path| Shard::new(&path).unwrap())
                .collect();

            Self {
                num_shards,
                shards,
            }
        }

        fn shard(&self, key: &[u8]) -> &Shard {
            let id = key.last().cloned().unwrap_or_default() % self.num_shards;
            self.shards.get(id as usize).unwrap()
        }

        pub fn lookup(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
            self.shard(key).lookup(key)
        }

        pub fn insert(&self, key: &[u8], val: &[u8]) -> Result<(), Error> {
            self.shard(key).insert(key, val)
        }

        pub fn remove(&self, key: &[u8]) -> Result<(), Error> {
            self.shard(key).remove(key)
        }
    }
}

struct Sharded(sharded::ShardedStore);

impl Storage for Sharded {
    fn insert(&self, key: &[u8], val: &[u8]) {
        self.0.insert(key, val).ok();
    }

    fn remove(&self, key: &[u8]) {
        self.0.remove(key).ok();
    }

    fn lookup(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.lookup(key).unwrap_or_default()
    }
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
        std::fs::remove_file(path).ok();
        let size: u32 = 4096;
        let file: File<Block> = File::make(path, size).unwrap();
        info!(
            "target={} file={:?} count={} page={}",
            target, path, count, size
        );

        benchmark(SelfStorage(file), count);
        std::fs::remove_file(path).ok();
    }

    if target == "lskv" {
        let path = "target/yalskv";
        std::fs::remove_dir_all(path).ok();
        std::fs::create_dir(path).ok();

        let db = yalskv::Store::open("target/yalskv").unwrap();
        info!(
            "target={} file={:?} count={}",
            target, path, count
        );

        benchmark(LSKV(RefCell::new(db)), count);
        std::fs::remove_dir_all(path).ok();
    }

    if target == "shrd" {
        let path = "target/shards";
        std::fs::remove_dir_all(path).ok();
        std::fs::create_dir(path).ok();
        let num_shards: u8 = std::env::var("SHARDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16);
        let sharded = sharded::ShardedStore::new(num_shards, path);
        info!(
            "target={} file={:?} count={} shards={}",
            target, path, count, num_shards
        );

        benchmark(Sharded(sharded), count);
        std::fs::remove_dir_all(path).ok();
    }

    if target == "sled" {
        let path = "target/sled_1M";
        std::fs::remove_dir_all(path).ok();
        std::fs::create_dir(path).ok();
        let db: Db = sled::open(path).unwrap();
        info!("target={} file={} count={}", target, path, count);

        benchmark(SledStorage(db), count);
        std::fs::remove_dir_all(path).ok();
    }
}
