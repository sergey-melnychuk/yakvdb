use log::debug;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::Hash;

pub(crate) trait Cache<K: Clone + Eq + PartialEq + Hash, V> {
    fn has(&self, key: &K) -> bool;
    fn get(&self, key: &K) -> Option<&V>;
    fn get_mut(&mut self, key: &K) -> Option<&mut V>;
    fn put(&mut self, key: K, value: V);
    fn del(&mut self, key: &K);
    fn len(&self) -> usize;
    fn keys(&self) -> Vec<K>;
    fn lock(&self, key: &K);
    fn free(&self, key: &K);
}

pub(crate) struct LruCache<K, V> {
    map: HashMap<K, V>,
    lru: RefCell<HashMap<K, u64>>,
    locks: RefCell<HashSet<K>>,
    cap: usize,
    op: RefCell<u64>,
}

impl<K: Clone + Eq + Hash + Display, V> LruCache<K, V> {
    pub(crate) fn new(size: usize) -> Self {
        Self {
            map: HashMap::with_capacity(size),
            lru: RefCell::new(HashMap::with_capacity(size)),
            locks: RefCell::new(HashSet::with_capacity(size)),
            cap: size,
            op: RefCell::new(0),
        }
    }

    fn op(&self) -> u64 {
        *self.op.borrow_mut() += 1;
        *self.op.borrow()
    }

    fn lru(&self) -> Option<K> {
        if self.map.len() < self.cap {
            None
        } else {
            let locked = self.locks.borrow();
            self.lru
                .borrow()
                .iter()
                .filter(|(key, _)| !locked.contains(*key))
                .min_by_key(|(_, lru)| *lru)
                .map(|(key, _)| key)
                .cloned()
        }
    }

    fn evict(&mut self) {
        if let Some(key) = self.lru() {
            debug!("Evict page {}", key);
            self.map.remove(&key);
            self.lru.borrow_mut().remove(&key);
        }
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
            let op = self.op();
            self.lru.borrow_mut().insert(key.clone(), op);
            self.map.get(key)
        }
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if !self.map.contains_key(key) {
            None
        } else {
            let op = self.op();
            self.lru.borrow_mut().insert(key.clone(), op);
            self.map.get_mut(key)
        }
    }

    fn put(&mut self, key: K, value: V) {
        self.evict();
        let op = self.op();
        self.lru.borrow_mut().insert(key.clone(), op);
        self.map.insert(key, value);
    }

    fn del(&mut self, key: &K) {
        self.lru.borrow_mut().remove(key);
        self.map.remove(key);
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn keys(&self) -> Vec<K> {
        self.map.keys().into_iter().cloned().collect()
    }

    fn lock(&self, key: &K) {
        self.locks.borrow_mut().insert(key.clone());
    }

    fn free(&self, key: &K) {
        self.locks.borrow_mut().remove(key);
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

        assert_eq!(cache.lru(), Some(1));
        cache.put(4, 0);

        let mut keys = cache.keys();
        keys.sort();
        assert_eq!(keys, vec![2, 3, 4]);
    }
}
