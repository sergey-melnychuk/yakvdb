use log::debug;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) trait Cache<K: Clone + Eq + PartialEq + Hash, V> {
    fn has(&self, key: &K) -> bool;
    fn get(&self, key: &K) -> Option<&V>;
    fn get_mut(&mut self, key: &K) -> Option<&mut V>;
    /// Insert an entry, returning the entry evicted to make room, if any.
    /// Entries for which `pinned` returns true are never evicted.
    fn put(&mut self, key: K, value: V, pinned: impl Fn(&K) -> bool) -> Option<(K, V)>;
}

struct Frame<K, V> {
    key: K,
    val: V,
    /// Set on every access, cleared by the clock hand as it sweeps past.
    referenced: AtomicBool,
}

/// A CLOCK (second-chance) cache: an approximation of LRU that costs O(1) per access.
///
/// A true LRU has to move the key to the most-recently-used end on every read,
/// which is a mutation, and so an exclusive lock, on the read path. Here a read
/// only sets the frame's `referenced` bit, which an atomic store does through
/// `&self`: readers holding the cache's shared lock do not exclude each other.
///
/// Eviction sweeps the hand over the frames, clearing `referenced` bits, and
/// takes the first frame whose bit was already clear: a page is evicted only if
/// nothing touched it since the hand last went by.
pub(crate) struct ClockCache<K, V> {
    map: HashMap<K, usize>,
    frames: Vec<Frame<K, V>>,
    hand: usize,
    cap: usize,
}

impl<K: Clone + Eq + Hash + Display, V> ClockCache<K, V> {
    /// A cache of up to `size` entries. Nothing is reserved for them up front:
    /// the cache grows as entries come in, so a large `size` costs nothing
    /// until it is used.
    pub(crate) fn new(size: usize) -> Self {
        let cap = size.max(1);
        Self {
            map: HashMap::new(),
            frames: Vec::new(),
            hand: 0,
            cap,
        }
    }

    /// Advance the hand to the first unpinned frame not referenced since the
    /// last sweep, or `None` if every frame is pinned.
    ///
    /// Two turns are enough: the first clears every bit it passes.
    fn victim(&mut self, pinned: impl Fn(&K) -> bool) -> Option<usize> {
        let len = self.frames.len();
        for _ in 0..2 * len {
            let idx = self.hand;
            self.hand = (self.hand + 1) % len;
            let frame = &self.frames[idx];
            if !pinned(&frame.key) && !frame.referenced.swap(false, Ordering::Relaxed) {
                return Some(idx);
            }
        }
        None
    }
}

impl<K: Clone + Hash + Eq + Display, V> Cache<K, V> for ClockCache<K, V> {
    fn has(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    fn get(&self, key: &K) -> Option<&V> {
        let frame = &self.frames[*self.map.get(key)?];
        // Load before storing: hot frames (the root, the upper levels) are read
        // by every operation, and an unconditional store would have each reader
        // take their cache line exclusively, however rarely the bit changes.
        if !frame.referenced.load(Ordering::Relaxed) {
            frame.referenced.store(true, Ordering::Relaxed);
        }
        Some(&frame.val)
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let frame = &mut self.frames[*self.map.get(key)?];
        *frame.referenced.get_mut() = true;
        Some(&mut frame.val)
    }

    fn put(&mut self, key: K, value: V, pinned: impl Fn(&K) -> bool) -> Option<(K, V)> {
        if let Some(&idx) = self.map.get(&key) {
            let frame = &mut self.frames[idx];
            frame.val = value;
            *frame.referenced.get_mut() = true;
            return None;
        }

        let frame = Frame {
            key: key.clone(),
            val: value,
            referenced: AtomicBool::new(false),
        };

        let victim = if self.frames.len() < self.cap {
            None
        } else {
            self.victim(pinned)
        };
        let Some(idx) = victim else {
            // Room to spare, or everything is pinned: then grow past `cap`
            // rather than fail. The extra frames are reused, not given back,
            // but once anything is unpinned the cache stops growing.
            if self.frames.len() >= self.cap {
                debug!(
                    "Cache over capacity: all {} frames pinned",
                    self.frames.len()
                );
            }
            self.map.insert(key, self.frames.len());
            self.frames.push(frame);
            return None;
        };
        let old = std::mem::replace(&mut self.frames[idx], frame);
        self.map.remove(&old.key);
        self.map.insert(key, idx);
        debug!("Evicted page {}", old.key);
        Some((old.key, old.val))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(cache: &ClockCache<u32, u32>) -> Vec<u32> {
        let mut keys = cache.map.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        keys
    }

    #[test]
    fn test_eviction() {
        let mut cache = ClockCache::new(3);
        cache.put(1, 0, |_| false);
        cache.put(2, 0, |_| false);
        cache.put(3, 0, |_| false);
        let evicted = cache.put(4, 0, |_| false);

        assert_eq!(evicted, Some((1, 0)));
        assert_eq!(keys(&cache), vec![2, 3, 4]);
    }

    #[test]
    fn test_eviction_keeps_map_bounded() {
        let cap = 3;
        let mut cache = ClockCache::new(cap);
        for i in 0..100 {
            cache.put(i, i, |_| false);
        }
        assert_eq!(cache.map.len(), cap, "map must not grow past capacity");
        assert_eq!(
            cache.frames.len(),
            cap,
            "frames must not grow past capacity"
        );
        // untouched entries leave in insertion order: the most recent `cap` survived
        assert_eq!(keys(&cache), vec![97, 98, 99]);
    }

    #[test]
    fn test_referenced_entry_gets_second_chance() {
        let mut cache = ClockCache::new(3);
        cache.put(1, 0, |_| false);
        cache.put(2, 0, |_| false);
        cache.put(3, 0, |_| false);
        assert!(cache.get(&1).is_some());

        // 1 was read since it went in, so the hand passes it over and takes 2
        assert_eq!(cache.put(4, 0, |_| false), Some((2, 0)));
        assert_eq!(keys(&cache), vec![1, 3, 4]);
    }

    #[test]
    fn test_put_existing_replaces_value() {
        let mut cache = ClockCache::new(2);
        cache.put(1, 10, |_| false);
        assert_eq!(cache.put(1, 11, |_| false), None);
        assert_eq!(cache.get(&1), Some(&11));
        assert_eq!(cache.frames.len(), 1);
    }

    #[test]
    fn test_get_miss() {
        let mut cache: ClockCache<u32, u32> = ClockCache::new(4);
        assert!(!cache.has(&1));
        assert!(cache.get(&1).is_none());
        assert!(cache.get_mut(&1).is_none());

        cache.put(1, 42, |_| false);
        assert!(cache.has(&1));
        assert_eq!(cache.get(&1), Some(&42));
        assert_eq!(cache.get_mut(&1), Some(&mut 42));

        // Key that was never inserted
        assert!(cache.get(&99).is_none());
        assert!(cache.get_mut(&99).is_none());
    }

    #[test]
    fn test_pinned_entries_are_not_evicted() {
        let mut cache = ClockCache::new(3);
        cache.put(1, 0, |_| false);
        cache.put(2, 0, |_| false);
        cache.put(3, 0, |_| false);

        // 1 would go first, but it is pinned
        assert_eq!(cache.put(4, 0, |k| *k == 1), Some((2, 0)));
        assert_eq!(keys(&cache), vec![1, 3, 4]);

        // everything pinned: grow past the capacity instead of evicting
        assert_eq!(cache.put(5, 0, |_| true), None);
        assert_eq!(keys(&cache), vec![1, 3, 4, 5]);

        // and stop growing once something can be evicted again
        assert!(cache.put(6, 0, |_| false).is_some());
        assert_eq!(cache.frames.len(), 4);
    }
}
