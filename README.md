Yet Another Kev-Value DataBase
==============================

PLAN:
- [x] make yakvdb thread-safe
  - distinct RW locks on pages in the pool?
  - cannot use it in async context now:
    - the trait `Sync` is not implemented for `RefCell<...>`
  - the guarantee: every `Store` operation takes an operation-level lock, so a
    shared `Arc<KV>` is safe to use from many threads. `insert`/`remove` take it
    exclusively and never overlap with anything; `lookup`/`is_empty`/`min`/`max`/
    `above`/`below` share it and do run at the same time as each other
  - `Tree` is crate-private: its methods assume the caller already holds that
    lock, so reaching them from outside would bypass it
  - the `next_id`/`alloc_overflow` races are subsumed by the operation lock:
    do not add separate locks for them
- [x] split `Tree` trait into pub KV-only and internal page-aware
  - to avoid leaking impl details leak into public API
- [X] CLI
  - connect to a file and explore it
  - `lookup X64'00cafebabe'`
  - `insert X64'00cafebabe' X64'00deadbeef'`
  - `remove X64'00cafebabe'`
  - `above X64'00cafebabe'`
  - `below X64'00cafebabe'`
  - `min`
  - `max`
  - `len` (iterate from `min` to `max`)
  - basic defragment/restore utilities
- [x] let reads run concurrently instead of one at a time
  - `ops` is an `RwLock`: `insert`/`remove` take `write()`, the read-only
    operations take `read()` and run at the same time as each other
  - `page()`/`page_mut()` retry when a page is evicted between `cache()` putting
    it there and the borrow that hands it to the caller
  - page reads are positional (`FileExt::read_exact_at` on unix), so they need
    only a shared lock on the file handle instead of `&mut File` for `seek`
  - measured on 20k keys, 40k lookups, release: 300k op/s flat at any thread
    count before, 487k (1 thread) / 666k (4 threads) after
- [x] cut page-cache contention between concurrent readers
  - the LRU list is replaced by CLOCK (second chance): a read sets the frame's
    `referenced` bit with an atomic store instead of moving the key under
    `lru.write()`, so readers no longer exclude each other, and every access is
    O(1) instead of a linear scan of the recency list
  - that made a large cache affordable: the default limit is 256 MiB (65536
    pages of 4 KiB) instead of 32 pages, set with `make_with_cache`/
    `open_with_cache`; pages are cached as they are read, so memory follows
    the size of the tree up to that limit instead of being reserved up front
  - measured on 200k keys, 400k lookups, release: 490k/430k/456k/241k op/s at
    1/2/4/8 threads before, 1.4M/2.9M/2.1M/1.4M after
- [ ] let reads scale past 2 threads
  - every lookup acquires `ops` and `cache` read locks about five times, each an
    atomic RMW on a word all threads share; that line bouncing is now the limit
  - wants striped locks (shard the cache by page id) or per-page latches
- [x] write only the pages that change
  - `insert`/`remove` walked down through `page_mut`, which marks a page dirty
    on access, so every page on the root-to-leaf path was rewritten by the flush
  - they now walk down through read guards and take a page mutably only to
    change it: 3.0 page writes per insert before, 1.03 after
- [x] tidy up page operations
  - `put_entry`/`remove` shift slots and data in place with `copy_within`
    instead of collecting them into `Vec`s and writing them all back
  - the lowest data offset is cached on `Block` (not on disk: the format is
    unchanged), so `free`/`fits`/`full` are arithmetic instead of slot scans
  - measured on 1M keys, release: insert 61k -> 265k op/s, lookup 410k -> 1.19M,
    remove 65k -> 230k
- [ ] write-ahead log with group commit - see [doc/wal.md](doc/wal.md)
  - nothing calls `fsync` today: a write survives a process crash, not a power
    loss, and a crash mid-split can leave the tree inconsistent
- [x] sweep `page(id).unwrap()`/`page_mut(id).unwrap()` into typed errors
  - a corrupted file should fail one operation, not the process
  - `try_page`/`try_page_mut` return `Error::Tree` instead of `None`, and all
    38 call sites in `Result`-returning functions use `?`
  - `root()`/`root_mut()` return `Result` too, which was free to do once the
    `Tree` trait stopped being public
- [x] make the `Tree` trait crate-private
  - a caller reaching `Tree` directly bypassed `ops` altogether, and sealing the
    trait would only have blocked outside *implementations*, not outside *calls*
  - `File` gained safe equivalents that take the lock: `read_page`/`read_root`
    hand back an owned page copy rather than a guard, plus `sync` and `dump`
  - this also stops `parking_lot` guard types leaking into the public API
  - a public API break, hence 0.7.0: `yakvdb::api::tree` is gone

---

Extremely simple (simplest possible?) single-file BTree-based key-value database. 

Built for fun and learning: goal is to "demystify" the "database".

Operations amortized runtime complexity:
* insert/remove: O(log(N) * log(K) + K)
* lookup/min/max/above/below: O(log(N) * log(K))

Where:
* N - number of entries in a tree
* K - number of entries in a page

Binary search is run for each page (log(K)) and touches at most log(N) pages.

On insert/remove each page performs O(K) cleanup to keep keys ordered, as well as extra housekeeping is performed if necessary (split or merge of pages).

Each insert/remove writes the pages it changed to the file before returning. That hands them to the OS (they survive a process crash), but nothing calls `fsync` yet, so they do not necessarily survive a power loss: see [doc/wal.md](doc/wal.md) for the plan.

### API
* [Store](src/api/mod.rs) is the public key-value API (impl: [File](src/disk/file.rs))
* [Page](src/api/page.rs) defines BTree node (impl: [Block](src/disk/block.rs))
* [Tree](src/api/tree.rs) defines the page-aware BTree internals (crate-private)

### Demo

Just `cargo run --release --example main` to run the example in [examples/main.rs](examples/main.rs):
* create/open database (file)
* generate random key-value pairs
* insert all key-value pairs
* lookup all keys and check values match
* iterate all keys in ascending order
* iterate all keys in descending order
* remove all keys and check database is empty

The typical result looks like one below.

```shell
$ RUST_LOG=info cargo run --release
[snip]

# 1M
[...] file="target/main_1M.tmp" count=1000000 page=4096
[...] insert: 28742 ms (rate=34792 op/s)
[...] lookup: 5316 ms (rate=188111 op/s)
[...] iter: min=000003cf1bb4e04d max=ffffe6e240320123
[...] iter:  asc 553 ms (rate=1808318 op/s) n=1000000
[...] iter: desc 538 ms (rate=1858736 op/s) n=1000000
[...] remove: 27101 ms (rate=36899 op/s)

# 10M
[...] file="target/10M.db" count=10000000 page=4096
[...] insert: 371971 ms (rate=26883 op/s)
[...] lookup: 95038 ms (rate=105221 op/s)
[...] iter: min=00000244ad95c9eb max=ffffffbd837a505b
[...] iter:  asc 6793 ms (rate=1472103 op/s) n=10000000
[...] iter: desc 7008 ms (rate=1426940 op/s) n=10000000
[...] remove: 368056 ms (rate=27169 op/s)

# 100M
[...] file="target/100M.db" count=100000000 page=4096
[...] insert: 4387618 ms (rate=22791 op/s)
[...] lookup: 1003484 ms (rate=99652 op/s)
[...] iter: min=000000542c79d673 max=ffffffbd837a505b
[...] iter:  asc 74953 ms (rate=1334169 op/s) n=100000000
[...] iter: desc 73857 ms (rate=1353967 op/s) n=100000000
[...] remove: 4145790 ms (rate=24120 op/s)
```

### Code

```rust
use std::path::Path;
use std::sync::Arc;
use yakvdb::api::error::Result;
use yakvdb::api::{Store, KV};

// Create a new database with the given page size...
let db: KV = KV::make(Path::new("/tmp/db.yak"), /*page_size=*/ 4096).unwrap();
// ...or open an existing one:
// let db: KV = KV::open(Path::new("/tmp/db.yak")).unwrap();
// Both cache up to 256 MiB of pages, growing with the tree; `make_with_cache`/
// `open_with_cache` pick the limit.

let _: Result<()> = db.insert(b"key", b"val");
let _: Result<Option<Vec<u8>>> = db.lookup(b"key");
let _: Result<()> = db.remove(b"key");

// To iterate: db.min(), db.max(), db.above(b"key"), db.below(b"key")

// `insert` and `remove` flush on their own, `sync` forces it:
let _: Result<()> = db.sync();

// Every method takes `&self` and `KV` is `Send + Sync`, so one handle can
// be shared across threads:
let db = Arc::new(db);
```

### Other

- [Bitcask](https://riak.com/assets/bitcask-intro.pdf)
