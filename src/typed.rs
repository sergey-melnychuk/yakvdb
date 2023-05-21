use std::path::Path;

use crate::api::{self, Store as KVStore};

pub struct Store(api::KV);

pub trait DB<K, V>
where
    K: AsRef<[u8]> + for<'a> From<&'a [u8]>,
    V: AsRef<[u8]> + for<'a> From<&'a [u8]>,
{
    fn new(path: &Path) -> Self;

    fn contains(&self, key: &K) -> anyhow::Result<bool>;
    fn lookup(&self, key: &K) -> anyhow::Result<Option<V>>;
    fn remove(&mut self, key: &K) -> anyhow::Result<Option<V>>;
    fn insert(&mut self, key: &K, val: V) -> anyhow::Result<()>;

    fn min(&self) -> anyhow::Result<Option<K>>;
    fn max(&self) -> anyhow::Result<Option<K>>;
    fn above(&self, key: &K) -> anyhow::Result<Option<K>>;
    fn below(&self, key: &K) -> anyhow::Result<Option<K>>;
}

impl<K, V> DB<K, V> for Store
where
    K: AsRef<[u8]> + for<'a> From<&'a [u8]>,
    V: AsRef<[u8]> + for<'a> From<&'a [u8]>,
{
    fn new(path: &Path) -> Self {
        let kv = if !path.exists() {
            api::KV::make(path, 4096).unwrap()
        } else {
            api::KV::open(path).unwrap()
        };
        Self(kv)
    }

    fn contains(&self, key: &K) -> anyhow::Result<bool> {
        Ok(self.0.lookup(key.as_ref())?.is_some())
    }

    fn lookup(&self, key: &K) -> anyhow::Result<Option<V>> {
        Ok(self.0.lookup(key.as_ref())?.map(|bytes| V::from(&bytes)))
    }

    fn remove(&mut self, key: &K) -> anyhow::Result<Option<V>> {
        let val = self.lookup(key)?;
        self.0.remove(key.as_ref())?;
        Ok(val)
    }

    fn insert(&mut self, key: &K, val: V) -> anyhow::Result<()> {
        Ok(self.0.insert(key.as_ref(), val.as_ref())?)
    }

    fn min(&self) -> anyhow::Result<Option<K>> {
        Ok(self.0.min()?.map(|bytes| K::from(&bytes)))
    }

    fn max(&self) -> anyhow::Result<Option<K>> {
        Ok(self.0.max()?.map(|bytes| K::from(&bytes)))
    }

    fn above(&self, key: &K) -> anyhow::Result<Option<K>> {
        Ok(self.0.above(key.as_ref())?.map(|bytes| K::from(&bytes)))
    }

    fn below(&self, key: &K) -> anyhow::Result<Option<K>> {
        Ok(self.0.below(key.as_ref())?.map(|bytes| K::from(&bytes)))
    }
}
