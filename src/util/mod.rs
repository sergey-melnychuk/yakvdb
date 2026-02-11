use rand::prelude::SliceRandom;
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};

pub mod bsearch;
pub mod cache;
pub mod hex;

pub fn data(count: usize, seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data() {
        let d = data(5, 42);
        assert_eq!(d.len(), 5);
        for (k, v) in &d {
            assert_eq!(k.len(), 8);
            assert_eq!(v.len(), 8);
        }
        // Deterministic with same seed
        assert_eq!(d, data(5, 42));
    }

    #[test]
    fn test_shuffle() {
        let items = vec![1, 2, 3, 4, 5];
        let shuffled = shuffle(items.clone(), 42);
        assert_eq!(shuffled.len(), 5);
        // Deterministic
        assert_eq!(shuffled, shuffle(vec![1, 2, 3, 4, 5], 42));
        // Contains same elements
        let mut sorted = shuffled;
        sorted.sort();
        assert_eq!(sorted, items);
    }
}
