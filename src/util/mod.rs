use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};

pub(crate) mod bsearch;
pub(crate) mod cache;
pub(crate) mod hex;

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
