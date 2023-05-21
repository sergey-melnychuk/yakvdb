pub mod error;
pub mod page;
pub mod tree;

use crate::{
    api::error::Result,
    disk::{block::Block, file::File},
};

pub type KV = File<Block>;

pub trait Store {
    fn lookup(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn insert(&self, key: &[u8], val: &[u8]) -> Result<()>;
    fn remove(&self, key: &[u8]) -> Result<()>;

    fn is_empty(&self) -> bool;

    /// Get lowest/smallest key stored in the tree, or none if tree is empty.
    fn min(&self) -> Result<Option<Vec<u8>>>;

    /// Get highest/biggest key stored in the tree, or none if tree is empty.
    fn max(&self) -> Result<Option<Vec<u8>>>;

    /// Get smallest key that is strictly greater than given one, if any.
    fn above(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Get biggest key that is strictly lesser than given one, if any.
    fn below(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
}
