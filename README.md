Yet Another Kev-Value DataBase
==============================

Extremely simple (simplest possible?) single-file BTree-based key-value database. 

Build for fun and learning: goal is to "demystify" the "database".

Operations amortized runtime complexity:
* insert/remove: O(log(N) * log(K) + K)
* lookup/min/max/above/below: O(log(N) * log(K))

Where:
* N - number of entries in a tree
* K - number of entries in a page

Binary search is run for each page (log(K)) and touches at most log(N) pages.

On insert/remove each page performs O(K) cleanup to keep keys ordered, as well as extra housekeeping is performed if necessary (split or merge of pages).

Each insert/remove gets flushed to the disk for durability.

### API
* [Page](src/api/page.rs) defines BTree node (impl: [Block](src/disk/block.rs))
* [Tree](src/api/tree.rs) defines full BTree (impl: [File](src/disk/file.rs))

### Demo

Just `cargo run --release` to run example in from [main.rs](src/main.rs):
* create/open database (file)
* generate random key-value pairs
* insert all key-value pairs
* lookup all keys and check values match
* iterate all keys in ascending order
* iterate all keys in descending order
* remove all keys and check database is empty

```shell
$ cargo run --release
[...][INFO] file="target/main_1M.tmp" count=1000000 page=4096
[...][INFO] insert: 27520 ms (rate=36337 op/s)
[...][INFO] lookup: 921 ms (rate=1085776 op/s)
[...][INFO] iter: min=000003cf1bb4e04d max=ffffe6e240320123
[...][INFO] iter:  asc 489 ms (rate=2044989 op/s) n=1000000
[...][INFO] iter: desc 465 ms (rate=2150537 op/s) n=1000000
[...][INFO] remove: 25819 ms (rate=38731 op/s)
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

[bitcask]: https://riak.com/assets/bitcask-intro.pdf
