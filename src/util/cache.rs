use log::debug;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;

pub(crate) trait Cache<K: Clone + Eq + PartialEq + Hash, V> {
    fn has(&self, key: &K) -> bool;
    fn get(&self, key: &K) -> Option<&V>;
    fn get_mut(&mut self, key: &K) -> Option<&mut V>;
    /// Insert an entry, returning the entry evicted to make room, if any.
    fn put(&mut self, key: K, value: V) -> Option<(K, V)>;
}

pub(crate) struct LruCache<K, V> {
    map: HashMap<K, V>,
    lru: Arc<RwLock<Vec<K>>>,
    cap: usize,
}

impl<K: Clone + Eq + Hash + Display, V> LruCache<K, V> {
    pub(crate) fn new(size: usize) -> Self {
        Self {
            map: HashMap::with_capacity(size),
            lru: Arc::new(RwLock::new(Vec::with_capacity(size))),
            cap: size,
        }
    }

    /// Move `key` to the most-recently-used end, evicting the least-recently-used
    /// key first if the cache is at capacity. Returns the evicted key, if any.
    ///
    /// The whole update happens under a single write guard: `get(&self)` mutates
    /// the recency list through interior mutability, so two concurrent readers
    /// would otherwise race between locating an index and removing it.
    fn touch(&self, key: &K) -> Option<K> {
        let mut lru = self.lru.write();

        if let Some(idx) = lru.iter().position(|x| x == key) {
            lru.remove(idx);
            lru.push(key.clone());
            return None;
        }

        let evicted = if lru.len() >= self.cap {
            Some(lru.remove(0))
        } else {
            None
        };
        lru.push(key.clone());
        evicted
    }
}

impl<K: Clone + Hash + Eq + Display, V> Cache<K, V> for LruCache<K, V> {
    fn has(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    fn get(&self, key: &K) -> Option<&V> {
        if !self.map.contains_key(key) {
            None
        } else {
            self.touch(key);
            self.map.get(key)
        }
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if !self.map.contains_key(key) {
            None
        } else {
            self.touch(key);
            self.map.get_mut(key)
        }
    }

    fn put(&mut self, key: K, value: V) -> Option<(K, V)> {
        let evicted = self.touch(&key).and_then(|evicted| {
            debug!("Evicted page {evicted}");
            let value = self.map.remove(&evicted)?;
            Some((evicted, value))
        });
        self.map.insert(key, value);
        evicted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eviction() {
        let mut cache = LruCache::new(3);
        cache.put(1, 0);
        cache.put(2, 0);
        cache.put(3, 0);
        cache.put(4, 0);

        let mut keys = cache.map.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, vec![2, 3, 4]);
    }

    #[test]
    fn test_eviction_keeps_map_bounded() {
        let cap = 3;
        let mut cache = LruCache::new(cap);
        for i in 0..100 {
            cache.put(i, i);
        }
        assert_eq!(cache.map.len(), cap, "map must not grow past capacity");
        assert_eq!(cache.lru.read().len(), cap, "lru must not grow past capacity");
        // the most recent `cap` keys survived
        let mut keys = cache.map.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, vec![97, 98, 99]);
    }

    #[test]
    fn test_get_miss() {
        let mut cache: LruCache<u32, u32> = LruCache::new(4);
        assert!(!cache.has(&1));
        assert!(cache.get(&1).is_none());
        assert!(cache.get_mut(&1).is_none());

        cache.put(1, 42);
        assert!(cache.has(&1));
        assert_eq!(cache.get(&1), Some(&42));
        assert_eq!(cache.get_mut(&1), Some(&mut 42));

        // Key that was never inserted
        assert!(cache.get(&99).is_none());
        assert!(cache.get_mut(&99).is_none());
    }
}
