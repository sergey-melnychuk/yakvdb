# Write-ahead log with group commit: design

Status: implemented (steps 1-5 and 7 of the plan below). The design is kept
as written; [As built](#as-built) at the end lists where the code differs.

## Why

Today every `insert`/`remove` ends in `Tree::flush`, which `pwrite`s every dirty
page in place and returns. Nothing calls `fsync`, so the data sits in the OS
page cache: it survives a process crash, not a power loss or kernel panic.

Adding `fsync` to that flush is not the fix:

- **It is slow.** Measured on macOS (where `sync_data` is `F_FULLFSYNC`), 5k
  inserts went from 100k op/s to 249 op/s. One fsync per operation caps the
  store at a few hundred writes per second, whatever else is done.
- **It is still not crash-safe.** An insert that splits a page writes three or
  more pages; a crash between those writes leaves a parent pointing at a page
  that does not hold what it should. `fsync` orders nothing *within* a flush.
- **Pages are written at other times too.** A dirty page evicted from the
  cache is written back in place mid-operation (`Tree::cache`), and
  `free_overflow` overwrites an overflow region's header in place the moment
  the entry is removed.

A WAL solves the three together: commit becomes one sequential append plus an
fsync shared by every writer waiting at that moment, and the B-tree file is
only ever changed in place by a checkpoint that can be redone after a crash.

## Overview

- **Logical redo log.** Each operation appends one record (`INSERT k v`,
  `REMOVE k`, or an atomic `BATCH` of them). Records describe the operation,
  not the pages it touched, so they are about as small as the data.
- **No-steal page cache.** A dirty page is never written in place between
  checkpoints: the cache does not evict it.
- **Checkpoints protected by page images.** A checkpoint first appends the
  images of all dirty pages to the WAL and fsyncs it, then writes them in
  place. A crash half-way through the in-place writes is repaired by writing
  the images again. This is InnoDB's doublewrite buffer, kept in the WAL.
- **Group commit.** Writers apply their change and append their record under
  the operation lock, release it, and then wait for the WAL to be durable up to
  their record. Whoever gets there first while no fsync is running becomes the
  leader and fsyncs everything appended so far, for everyone.

With no-steal and whole-snapshot checkpoints, the `.yak` file is always exactly
the tree as of the last completed checkpoint. So recovery is simply: redo the
last checkpoint's page images if they are there, then replay the logical records
that came after it through the ordinary `insert`/`remove`. No per-page LSNs, no
undo.

## Write path

```
insert(k, v):
    ops.write()                          -- as today: one writer at a time
        apply to cached pages            -- pages become dirty, pinned in cache
        lsn = wal.append(INSERT k v)     -- memcpy into the WAL buffer
    release ops
    wal.wait_durable(lsn)                -- group commit, see below
```

Appending under `ops` makes WAL order the same as apply order, which replay
relies on. Waiting *outside* `ops` is what lets writers share an fsync: while
the leader is inside `fsync` (milliseconds), the next writers apply their
changes (microseconds) and queue their records behind it.

### Group commit

```rust
struct WalState {
    buf: Vec<u8>,        // records appended since the last write
    spare: Vec<u8>,      // swapped in when `buf` is taken, to reuse its capacity
    appended_lsn: u64,
    durable_lsn: u64,
    flushing: bool,
}
// Mutex<WalState> + Condvar

fn wait_durable(&self, lsn: u64) -> io::Result<()> {
    let mut st = self.state.lock();
    while st.durable_lsn < lsn {
        if st.flushing {
            self.cond.wait(&mut st);         // a leader is at it: ride along
            continue;
        }
        // become the leader for everything appended so far
        st.flushing = true;
        let upto = st.appended_lsn;
        let batch = std::mem::replace(&mut st.buf, std::mem::take(&mut st.spare));
        drop(st);
        let res = self.segment.write_all(&batch).and_then(|_| self.segment.sync_data());
        st = self.state.lock();
        st.spare = recycle(batch);           // clear() and keep the allocation
        st.flushing = false;
        if res.is_ok() { st.durable_lsn = upto; }
        self.cond.notify_all();
        res?;
    }
    Ok(())
}
```

A writer that arrives while an fsync is running waits one extra fsync at most,
and its record goes out with everyone else's that arrived in the meantime. An
optional `commit_delay` (as in Postgres) can hold the leader for a few tens of
microseconds before it takes `buf`, to widen batches when writers are many.

**Visible before durable.** Readers take `ops.read()` and see a change as soon
as it is applied, possibly before its fsync. The writer's own call does not
return until the record is durable, but another thread may read the value and
then lose it in a crash. This is the usual trade-off for group commit without
MVCC; it gets documented, not hidden. Making readers wait for `durable_lsn`
would close it, at the price of reads waiting on fsync.

## WAL format

yalskv's log is the starting point: an append-only sequence of
`[op][klen][vlen][key][val]` records, read back by a sequential scan. It lacks
what a WAL needs: a checksum (a torn tail that happens to parse is replayed as
garbage), a sequence number, and fsync. Frames:

```
[len: u32]      bytes that follow this field
[crc: u32]      CRC-32C of everything after this field
[lsn: u64]
[kind: u8]
payload:
  INSERT      [klen: u32][vlen: u32][key][val]
  REMOVE      [klen: u32][key]
  BATCH       [count: u32] then `count` INSERT/REMOVE payloads, each with its kind byte
  PAGE        [page_id: u32][page_bytes bytes]
  CHECKPOINT  [checkpoint_lsn: u64][pages: u32]
```

Big-endian, like the rest of the file format. Lengths are `u32`, as `Slot`
already caps key and value sizes there, instead of yalskv's `u64`. Replay stops
at the first frame whose length runs past the end of the segment or whose CRC
does not match: that is the torn tail of the last write before a crash, and
anything after it was never acknowledged. CRC-32C via the `crc32fast` crate (or a
small table-driven implementation, to keep dependencies at zero).

**Segments.** `<db>.yak.wal/{:020}.wal`, numbered the way yalskv names its data
files (`id_to_path`). A checkpoint starts a new segment, then deletes the older
ones. The WAL is only read during recovery: at runtime the page cache serves
reads, which avoids yalskv's hazards of reading the log through the writer
(flushing the `BufWriter` on every `lookup`, the `reset()`/`unset()` offset
juggling).

## Checkpoint

Triggers: dirty pages over half the cache (no-steal needs headroom), active
segment over `wal_segment_bytes` (64 MiB), idle for a second with dirty pages,
`sync()`, and `Drop`.

```
checkpoint():
    ops.write()                                -- freeze the tree
    ckpt_lsn = appended_lsn
    sync_data(.yak)                            -- (0) see "pages outside the cache"
    append PAGE(id, bytes) for every dirty page
    append CHECKPOINT(ckpt_lsn, n)
    write + sync_data the WAL                  -- (1) images durable
    pwrite every dirty page in place
    sync_data(.yak)                            -- (2) tree durable
    clear dirty set, apply deferred frees
    start a new segment, delete the old ones   -- (3)
```

A crash before (1) completes leaves no valid `CHECKPOINT` frame: the images are
ignored and the `.yak` file was not touched. A crash between (1) and (2) leaves
some pages written and some not: recovery writes all the images again. After (2)
the old segments are redundant, and deleting them is only cleanup.

Each dirty page is written twice per checkpoint instead of once per operation.
The upper levels of the tree are touched by almost every insert, so a
checkpoint every few thousand operations writes each of them twice instead of
thousands of times.

The first version holds `ops.write()` for the whole checkpoint. It can be
narrowed later: copy the dirty pages under `ops.read()`, release it, and write
the copies, as long as pages dirtied meanwhile stay pinned for the next checkpoint.

## Recovery

On `open`, if `<db>.yak.wal/` has segments:

1. Scan the segments in order, validating frames, and stop at the first invalid one.
2. Find the last `CHECKPOINT` whose `n` `PAGE` frames all precede it. Write
   those images in place, then `sync_data(.yak)`. Doing this twice is harmless.
3. Replay the `INSERT`/`REMOVE`/`BATCH` records with `lsn > checkpoint_lsn`
   through the normal `insert`/`remove`, with logging switched off. They are
   idempotent: inserting a key sets its value, and removing an absent key does nothing.
4. Checkpoint, which leaves an empty WAL.

This is correct because after step 2 the tree file is exactly the tree as of
`checkpoint_lsn`: that checkpoint wrote every dirty page, and no-steal means no
page reached the file after it. Replaying the later operations in order gives
the committed state.

## Changes to existing code

- **Cache eviction.** `ClockCache::victim` skips frames holding dirty pages (a
  predicate passed to `put`). If every frame is dirty, the writer runs a
  checkpoint first. With the checkpoint threshold at half the cache, that is rare.
- **`Tree::flush` at the end of `insert`/`remove`** goes away, replaced by
  `wal.append` and `wal.wait_durable`.
- **Pages outside the cache.** `next_id` and `alloc_overflow` extend the file
  directly, which is safe because nothing on disk refers to a new page until a
  checkpoint writes its parent. The data of a new overflow region has to be
  durable before that checkpoint's images are, hence step (0). `free_overflow`
  must stop overwriting the region header immediately: the pre-checkpoint tree
  on disk still points at it. Freed overflow pages go on a pending list, which
  the checkpoint writes out and hands to the free list. A regular page freed by
  `merge` can be reused straight away, because it only changes in the cache.
- **`Head.page_count`** is never updated today, so `open` scans every page for
  the free list. The checkpoint can maintain it (or persist the free list) at no
  extra cost.

## API

```rust
pub enum Durability {
    /// `insert`/`remove` return once their record is fsynced (group commit).
    Sync,
    /// Return at once; a background thread fsyncs the WAL every `interval`.
    /// A crash loses at most the last `interval` of acknowledged writes.
    Async { interval: Duration },
}

pub struct Options {
    pub cache_bytes: usize,          // DEFAULT_CACHE_BYTES
    pub durability: Durability,
    pub wal_segment_bytes: u64,      // 64 MiB
}

impl KV {
    pub fn open_with(path: &Path, options: Options) -> io::Result<Self>;
    /// Apply all ops atomically: one BATCH record, one commit wait.
    pub fn write_batch(&self, ops: &[Op<'_>]) -> Result<()>;
    /// Force a checkpoint.
    pub fn sync(&self) -> Result<()>;
}
```

## Expected performance

- **One thread, `Sync`:** still one fsync per operation, about 250 op/s on the
  macOS machine above. Group commit cannot help a writer that waits for each
  write before issuing the next. That is what `write_batch` and `Async` are for.
- **N threads, `Sync`:** up to N operations per fsync. Applying a change takes
  around 10 µs against milliseconds of fsync, so batches can grow large before
  the operation lock becomes the limit.
- **`Async`, or `write_batch` with large batches:** an operation costs one
  apply plus one memcpy into the WAL buffer, and page writes move to
  checkpoints. This should come close to today's figures with page writes
  switched off.

## Implementation plan

Each step lands on its own, with tests:

1. `wal` module: frame encoding and decoding, CRC, segment writer and reader.
   Tests include a WAL cut at every byte offset of its last frame.
2. No-steal cache and a checkpoint without the WAL: dirty pages stay pinned and
   are written by checkpoints instead of by every operation.
3. Deferred `free_overflow` writes.
4. Records appended in `insert`/`remove`, plus `wait_durable` with group commit.
5. Page-image checkpoints and recovery on `open`.
6. Crash tests: an integration test that runs a writer in a child process,
   kills it at random points (or injects I/O failures through a file wrapper),
   reopens the file and compares it against a model of the acknowledged writes.
7. `Options`, `Durability`, `write_batch`, README.

## As built

What the implementation does differently from the design above, or adds:

- **Durability modes.** `Durability::Os` (the default), `Periodic(interval)`
  and `Sync`, instead of `Sync`/`Async`. `Os` writes the record to the OS
  before the write returns and never fsyncs it; `Periodic` adds a background
  fsync. Checkpoints fsync in every mode, so no mode can leave the file
  half-updated: they differ only in how many recent writes a power loss takes.
  `Os` matches what the store promised before the WAL (a write survives a
  process crash), at about the same speed.
- **Fewer fsyncs per checkpoint.** A new segment is not fsynced on creation;
  one directory sync after rotation covers the new segment and the deleted old
  ones. Step (0), the fsync of the tree file before the images, only runs if
  pages were written straight to the file (`next_id`, `alloc_overflow`)
  since the last checkpoint. The rest is as designed: the log fsync after the
  images, and the tree file fsync after the in-place writes.
- **Poisoning.** If applying a write fails half-way (a corrupted page, an
  I/O error), a commit fails, a checkpoint fails, or a record fails to replay
  during recovery, the cache may hold part of a change that the log does not.
  The database then refuses further writes and reads, and skips the checkpoint
  on close, so the partial change is neither seen nor written to the file;
  reopening recovers the last committed state.
- **Checkpoint before the write that triggers it.** A write checks the
  thresholds before applying itself, not after: a failed checkpoint then fails
  a write that changed nothing, rather than one whose record it had already
  made durable, and that recovery would bring back.
- **One handle at a time.** `make` and `open` take an exclusive lock on the
  database file (`File::try_lock`): opening replays and rotates the log, which
  would delete it out from under a handle that is still using it.
- **Idle checkpoints are skipped.** With nothing logged since the last one,
  nothing dirty and nothing freed, closing or `sync` does no I/O.
- **Bounded buffers.** A checkpoint's page images go through the append
  buffer, which is given back after a flush if it grew past 1 MiB.
- **Write buffer.** Under `Os` and `Periodic`, `Options::write_buffer`
  lets committed records wait in memory before the `write()` that hands them
  to the OS: up to a number of records (1 by default: every commit is
  written), a number of bytes, or an interval enforced by the background
  flusher. That `write()` is most of what an insert costs, so a buffer of a
  hundred records roughly triples insert throughput, at the price of losing
  those records to a process crash. Checkpoints, `sync` and closing write out
  whatever waits, and so does closing a poisoned database: its buffered
  records are all of writes that succeeded.
- **No-op removes are not logged.** Removing a key that is not there changes
  nothing, dirties nothing, and does not wait for a commit.
- **Recovery cleans up.** After replaying, `open` checkpoints, which also
  deletes the old segments. With nothing to replay it deletes them directly.
- **`make` deletes a stale log.** A `<path>.wal` directory without its
  database belongs to one that was deleted, and would be replayed onto the new one.
- **Known leak.** A crash after an overflow region was written, but before a
  checkpoint made the tree refer to it, leaves the region unreferenced in the
  file. `open` recognises it as an overflow region, so it is skipped, not
  corrupted, but its pages are not reused. The same goes for freed overflow
  pages whose checkpoint never ran.
- **Tests.** Besides the unit tests of the frame format and recovery plan:
  crash recovery by dropping a `File` without its closing checkpoint
  (`mem::forget`), a crash at several points during a checkpoint's in-place
  writes (a test hook), the log cut at many byte offsets, a freed overflow
  region needed after a lost record, and a check that no page already in the
  file changes between checkpoints however hard the cache is churned. Removing
  the image redo, the pinning of dirty pages, the deferred frees or the replay
  makes the corresponding test fail. Killing a real child process (step 6) is
  not done yet.
