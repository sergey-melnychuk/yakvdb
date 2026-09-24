# Next

Follow-ups from the performance and WAL work on `perf/cache-dirty-pageops`.
Nothing on the branch is pushed yet.

## Where things stand

8-byte keys and values, `examples/main.rs`, release build, M4 Pro, `Durability::Os`.

| | inserts/s | lookups/s | removes/s | peak memory |
|---|---|---|---|---|
| 1M keys, write buffer 100 | 1.07M | 1.33M | 595k | - |
| 1M keys, write buffer 1 (default) | 315k | 1.39M | 248k | - |
| 10M keys, 256 MiB cache, buffer 100 | 76k | 407k | 56k | 1.5 GB |
| 10M keys, 1 GiB cache, buffer 100 (before the memory fix) | 93k | 622k | 58k | - |
| sled 1.0.0-alpha.124, 10M keys, defaults (512 MiB cache) | 75k | 353k | 79k | 3.2 GB |

yakvdb beats sled at 10M on lookups and memory, matches or beats it on
inserts, and loses on removes.

The 10M inserts at 256 MiB ran at 90k/s before the memory fix and 76k/s
after. That fix did not touch the insert path, so it is probably run-to-run
variance, but this is not confirmed: rerun both before relying on either.

To reproduce: `BUFFER=100 CACHE_MB=256 RUST_LOG=info cargo run --release --example main -- self 10000000`,
and `-- sled 10000000` for sled. Check the load average first: another
build running on the machine skewed several measurements by 2-3x.

## 1. Removes at 10M keys (56k/s vs sled's 79k/s)

- Profile the remove phase at 10M, not at 1M. At 1M, 65% of remove time
  was the in-page binary search, since sped up, and merges were only 4%.
  At 10M, page reads and checkpoint writes probably dominate instead.
- Check how often removes merge pages, how many pages each remove dirties,
  and whether the path still takes a write guard on pages it doesn't change.
- Inserts did not speed up with a larger cache (256 MiB to 1 GiB), which
  also suggests checkpoints limit writes: each writes up to 128 MiB of pages
  twice, once as images in the log and once in place.

## 2. Profile sled (1M, 3M, 10M)

Agreed but not done yet. Find out why sled's inserts drop from ~1.1M/s at
100k keys to 75k/s at 10M. My guesses, unverified:
- its 512 MiB leaf cache overflows, and each miss reloads and zstd-decompresses a whole leaf;
- every 200 ms flush reserializes and recompresses each dirty leaf in full;
- the 1.0 rewrite is still an alpha.

Profile with `sample <pid>`, as for yakvdb.

## 3. Ethereum-shaped workload (the actual goal: 100M-1B entries)

Keys are 64 bytes: `address || slot || block number`, or a hash. Values are
32 B to ~1 KB, such as trie-node RLP. That is 16-20 GB at 100M and 160-200 GB
at 1B, far beyond the page cache, so disk reads, write amplification and read
concurrency decide performance rather than CPU work.

1. **Benchmark first.** Two key families: `address || slot || block` (good
   locality) and `hash` (uniformly random), values of 32 B to 1 KB in a
   realistic mix, at 10M and then 100M, against sled. Check free disk
   space first: 20-30 GB per engine at 100M.
2. **Sorted `write_batch`.** Sort a batch by key (last write wins) before
   applying it: consecutive keys then land on the same leaf and dirty fewer pages.
3. **Bulk loader.** Build leaves bottom-up from sorted input, for initial
   import. This is how 1B entries load in reasonable time.
4. **Parallel reads.** Reads still peak at 2 threads: every lookup takes the
   `ops` and cache read locks about five times, on words all threads share.
   Shard the cache by page id, and look at per-page latches. At 1B entries,
   most lookups read one leaf from disk, and the SSD needs many reads in
   flight to reach its throughput.
5. **Fanout.** Use 16-32 KiB pages (up to 64 KiB is allowed), and store the
   shortest separator in internal pages instead of the child's full 64-byte
   max key. That gives 200+ children per page, so all internal nodes stay cached.
6. **Write amplification, depending on 1.** A random insert rewrites a whole
   page, twice. Options: copy-on-write pages (LMDB/MDBX style: no double
   write, no page images in the log), or an LSM-style layer for the
   hash-keyed data.

## 4. Crash tests with a real process (doc/wal.md, step 6)

Recovery is currently tested inside the process, by dropping a `File`
without its closing checkpoint (`mem::forget`). Add a test that runs a
writer in a child process, kills it at random points, reopens the database,
and compares it against the writes that were acknowledged.

## 5. Smaller items

- **Removes:** `remove_locked` still allocates for parent key updates
  (`page_max` makes a `to_vec`, `peers` and `candidates` are `Vec`s).
- **rand 0.9:** would remove the `rng.r#gen()` workaround left by the
  edition 2024 migration.
- **yalskv:** still on edition 2021 and untouched. Its `Store::open` never
  rebuilds the index from existing files (it is a TODO), so reopening loses
  everything.
- **Release 0.8.0:** breaking changes on this branch are `make_with`/`open_with`
  with `Options`, `Store::write_batch`, and the `<path>.wal` directory beside
  each database. Needs a version bump and a changelog; crates.io publish only
  with explicit approval.
- **Cleanup:** delete `backup/before-reset-author` (the branch from the
  author reset) once it is no longer needed.
