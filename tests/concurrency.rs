//! `KV` is `Send + Sync` and mutates through `&self`, so the compiler permits
//! sharing one handle across threads. These tests hold it to that promise.
//!
//! Before the operation-level lock, `test_concurrent_inserts_keep_every_key`
//! panicked inside `File::split` ("called `Option::unwrap()` on a `None` value",
//! disk/file.rs) and lost the majority of its keys.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use yakvdb::api::{Store, KV};

const PAGE: u32 = 4096;

/// The promise this whole test file rests on: a future refactor must not be able
/// to silently revoke it.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<yakvdb::api::KV>();
};

fn tmp(name: &str) -> PathBuf {
    let path = PathBuf::from("target").join(format!("test-concurrency-{name}.tmp"));
    let _ = std::fs::remove_file(&path);
    path
}

/// 60-byte keys with 32-byte values: many entries per page, so splits are frequent.
fn key(i: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(60);
    key.extend_from_slice(&(i as u64).to_be_bytes());
    key.extend_from_slice(&[0u8; 44]);
    key.extend_from_slice(&(i as u64).to_be_bytes());
    key
}

fn val(i: usize) -> Vec<u8> {
    let mut val = vec![0u8; 24];
    val.extend_from_slice(&(i as u64).to_be_bytes());
    val
}

#[test]
fn test_concurrent_inserts_keep_every_key() {
    let path = tmp("insert");
    let db = Arc::new(KV::make(Path::new(&path), PAGE).unwrap());

    let n = 4000usize;
    let threads = 8usize;
    let failed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(threads);
    for w in 0..threads {
        let (db, failed) = (db.clone(), failed.clone());
        handles.push(thread::spawn(move || {
            let mut i = w;
            while i < n {
                if db.insert(&key(i), &val(i)).is_err() {
                    failed.fetch_add(1, Ordering::SeqCst);
                }
                i += threads;
            }
        }));
    }
    for h in handles {
        h.join().expect("worker thread panicked");
    }

    assert_eq!(failed.load(Ordering::SeqCst), 0, "inserts returned errors");
    for i in 0..n {
        assert_eq!(
            db.lookup(&key(i)).unwrap(),
            Some(val(i)),
            "key {i} lost or corrupted"
        );
    }
}

#[test]
fn test_concurrent_readers_and_writers() {
    let path = tmp("mixed");
    let db = Arc::new(KV::make(Path::new(&path), PAGE).unwrap());

    let n = 2000usize;
    // seed half the keys so readers have something to find from the start
    for i in (0..n).step_by(2) {
        db.insert(&key(i), &val(i)).unwrap();
    }

    let mut handles = Vec::with_capacity(8);
    for w in 0..4 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            let mut i = 1 + w * 2;
            while i < n {
                db.insert(&key(i), &val(i)).unwrap();
                i += 8;
            }
        }));
    }
    for _ in 0..4 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for i in (0..n).step_by(2) {
                // seeded keys must stay visible while writers work
                assert_eq!(db.lookup(&key(i)).unwrap(), Some(val(i)), "key {i} vanished");
            }
        }));
    }
    for h in handles {
        h.join().expect("worker thread panicked");
    }

    for i in 0..n {
        assert_eq!(db.lookup(&key(i)).unwrap(), Some(val(i)), "key {i} lost");
    }
}

#[test]
fn test_concurrent_insert_and_remove() {
    let path = tmp("remove");
    let db = Arc::new(KV::make(Path::new(&path), PAGE).unwrap());

    let n = 1500usize;
    for i in 0..n {
        db.insert(&key(i), &val(i)).unwrap();
    }

    let mut handles = Vec::with_capacity(4);
    for w in 0..4 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            let mut i = w;
            while i < n {
                if i % 2 == 0 {
                    db.remove(&key(i)).unwrap();
                } else {
                    db.insert(&key(i), &val(i + 1)).unwrap();
                }
                i += 4;
            }
        }));
    }
    for h in handles {
        h.join().expect("worker thread panicked");
    }

    for i in 0..n {
        let expected = if i % 2 == 0 { None } else { Some(val(i + 1)) };
        assert_eq!(db.lookup(&key(i)).unwrap(), expected, "key {i} mismatch");
    }
}

#[test]
fn test_concurrent_readers_only() {
    // More pages than the 32-entry page cache holds, so readers evict each
    // other's pages and hammer the recency list from several threads at once.
    let path = tmp("readers");
    let db = Arc::new(KV::make(Path::new(&path), PAGE).unwrap());

    let n = 5000usize;
    for i in 0..n {
        db.insert(&key(i), &val(i)).unwrap();
    }

    let mut handles = Vec::with_capacity(8);
    for w in 0..8 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for round in 0..3 {
                let mut i = (w * 37 + round) % n;
                for _ in 0..500 {
                    assert_eq!(db.lookup(&key(i)).unwrap(), Some(val(i)), "key {i} lost");
                    i = (i + 977) % n;
                }
            }
        }));
    }
    for h in handles {
        h.join().expect("worker thread panicked");
    }
}

#[test]
fn test_concurrent_iteration() {
    // `above`/`below` walk back up to a parent page part-way through a
    // traversal. `above` used to fetch that parent while still holding a read
    // guard on the page cache, which deadlocks the moment another thread wants
    // the write guard to load a page of its own.
    let path = tmp("iterate");
    let db = Arc::new(KV::make(Path::new(&path), PAGE).unwrap());

    let n = 2000usize;
    for i in 0..n {
        db.insert(&key(i), &val(i)).unwrap();
    }

    let min = db.min().unwrap().expect("min");
    let max = db.max().unwrap().expect("max");
    assert_eq!(min, key(0));
    assert_eq!(max, key(n - 1));

    let mut handles = Vec::with_capacity(8);
    for w in 0..8 {
        let db = db.clone();
        let (min, max) = (min.clone(), max.clone());
        handles.push(thread::spawn(move || {
            let mut count = 1usize;
            if w % 2 == 0 {
                let mut cur = min;
                while let Some(next) = db.above(&cur).unwrap() {
                    assert!(next > cur, "above went backwards");
                    cur = next;
                    count += 1;
                }
                assert_eq!(cur, max, "ascending walk ended early");
            } else {
                let mut cur = max;
                while let Some(prev) = db.below(&cur).unwrap() {
                    assert!(prev < cur, "below went forwards");
                    cur = prev;
                    count += 1;
                }
                assert_eq!(cur, min, "descending walk ended early");
            }
            assert_eq!(count, n, "walk visited the wrong number of keys");
        }));
    }
    for h in handles {
        h.join().expect("worker thread panicked");
    }
}
