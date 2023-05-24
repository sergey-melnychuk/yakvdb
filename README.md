Yet Another Kev-Value DataBase
==============================

PLAN:
- [x] make yakvdb thread-safe
  - distinct RW locks on pages in the pool?
  - cannot use it in async context now:
    - the trait `Sync` is not implemented for `RefCell<...>`
- [x] split `Tree` trait into pub KV-only and internal page-aware
  - to avoid leaking impl details leak into public API
- [ ] iterator impl (feature `iter`)
  - range lookup (returning an iterator)
- [ ] CLI
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
- [ ] add async impl based on `tokio::fs`
  - you can't go back to sync though
  - somehow make feature switch to async?

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

Each insert/remove gets flushed to disk for durability.

### API
* [Page](src/api/page.rs) defines BTree node (impl: [Block](src/disk/block.rs))
* [Tree](src/api/tree.rs) defines full BTree (impl: [File](src/disk/file.rs))

### Demo

Just `cargo run --release` to run example from [main.rs](src/main.rs):
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
use std::cell::Ref;
use crate::api::error::Result;
use crate::disk::block::Block;
use crate::disk::file::File;

// Create new database with given page_size
let mut db: File<Block> = File::make(path, /*page_size=*/4096).unwrap();
// Or open a database from an existing file
let mut db: File<Block> = File::open(path).unwrap();

let r: Result<Optional<Ref<u8>>> = db.lookup(&b"key");
let _: Result<()> = db.insert(&b"key", &b"val");
let _: Result<()> = db.remove(&b"key");

// To iterate: db.min(), db.max(), db.above(&[u8]), db.below(&[u8])
```

### Other

- [Bitcask](https://riak.com/assets/bitcask-intro.pdf)
