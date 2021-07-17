use crate::api::tree::Tree;
use crate::disk::block::Block;
use crate::disk::file::File;
use rand::{thread_rng, RngCore};
use std::ops::Deref;
use std::path::Path;

pub(crate) mod api;
pub(crate) mod disk;
pub(crate) mod util;

fn main() {
    let path = Path::new("target/main_10k.tmp");
    let size: u32 = 4096;

    // let mut file: File<Block> = if path.exists() {
    //     File::open(path).unwrap() // TODO FIXME
    // } else {
    //     File::make(path, size).unwrap()
    // };

    if path.exists() {
        std::fs::remove_file(path).unwrap();
    }
    let mut file: File<Block> = File::make(path, size).unwrap();

    let mut rng = thread_rng();
    let count = 10000;
    let data = (0..count)
        .into_iter()
        .map(|_| {
            (
                rng.next_u64().to_be_bytes().to_vec(),
                rng.next_u64().to_be_bytes().to_vec(),
            )
        })
        .collect::<Vec<_>>();

    for (k, v) in data.iter() {
        file.insert(k, v).unwrap();
    }

    for (k, v) in data.iter() {
        assert_eq!(file.lookup(k).unwrap().unwrap().deref(), v);
    }

    for (key, _) in data.iter() {
        file.remove(key).unwrap();
        assert!(file.lookup(key).unwrap().is_none());
    }
}
