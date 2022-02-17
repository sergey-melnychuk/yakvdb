use log::debug;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;

pub(crate) trait Cache<K: Clone + Eq + PartialEq + Hash, V> {
    fn has(&self, key: &K) -> bool;
    fn get(&self, key: &K) -> Option<&V>;
    fn get_mut(&mut self, key: &K) -> Option<&mut V>;
    fn put(&mut self, key: K, value: V);
    fn len(&self) -> usize;
    fn keys(&self) -> Vec<K>;
}

pub(crate) struct LruCache<K, V> {
    map: HashMap<K, V>,
    lru: RefCell<Vec<K>>,
    cap: usize,
}

impl<K: Clone + Eq + Hash + Display, V> LruCache<K, V> {
    pub(crate) fn new(size: usize) -> Self {
        Self {
            map: HashMap::with_capacity(size),
            lru: RefCell::new(Vec::with_capacity(size)),
            cap: size,
        }
    }

    fn touch(&self, key: &K) -> Option<K> {
        let existing = if !self.map.contains_key(key) {
            None
        } else {
            self.lru
                .borrow()
                .iter()
                .enumerate()
                .find(|(_, x)| x == &key)
                .map(|(i, _)| i)
        };

        if let Some(idx) = existing {
            let mut lru = self.lru.borrow_mut();
            lru.remove(idx);
            lru.push(key.clone());
        } else {
            let mut lru = self.lru.borrow_mut();
            if lru.len() == self.cap {
                let evicted = lru.remove(0);
                return Some(evicted);
            }
            lru.push(key.clone());
        }
        None
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

    fn put(&mut self, key: K, value: V) {
        if let Some(evicted) = self.touch(&key) {
            self.map.remove(&evicted);
            debug!("Evicted page {}", evicted);
        }
        self.map.insert(key, value);
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn keys(&self) -> Vec<K> {
        self.map.keys().into_iter().cloned().collect()
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

        let mut keys = cache.keys();
        keys.sort();
        assert_eq!(keys, vec![2, 3, 4]);
    }
}
