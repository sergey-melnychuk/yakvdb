use rand::prelude::SliceRandom;
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};

pub mod bsearch;
pub mod cache;
pub mod hex;

pub fn data(count: usize, seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .into_iter()
        .map(|_| {
            (
                rng.next_u64().to_be_bytes().to_vec(),
                rng.next_u64().to_be_bytes().to_vec(),
            )
        })
        .collect()
}

pub fn shuffle<T>(mut data: Vec<T>, seed: u64) -> Vec<T> {
    let mut rng = StdRng::seed_from_u64(seed);
    data.shuffle(&mut rng);
    data
}
