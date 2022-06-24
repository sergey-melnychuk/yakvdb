use crate::api::tree::Tree;
use crate::disk::block::Block;
use crate::disk::file::File;
use crate::util::hex::hex;
use log::{debug, error, info};
use std::path::Path;
use std::time::SystemTime;

pub(crate) mod api;
pub(crate) mod disk;
pub(crate) mod util;

fn main() {
    env_logger::init();

    let path = Path::new("target/main_1M.tmp");
    let size: u32 = 4096;

    let mut file: File<Block> = if path.exists() {
        File::open(path).unwrap()
    } else {
        File::make(path, size).unwrap()
    };

    let count = 1000 * 1000;
    let data = util::data(count, 42);
    info!("file={:?} count={} page={}", path, count, size);

    let mut now = SystemTime::now();
    for (k, v) in data.iter() {
        debug!("insert: key='{}' val='{}'", hex(k), hex(v));
        file.insert(k, v).unwrap();
    }
    let mut millis = now.elapsed().unwrap_or_default().as_millis();
    info!(
        "insert: {} ms (rate={} op/s)",
        millis,
        count as u128 * 1000 / millis
    );

    now = SystemTime::now();
    let mut found = Vec::with_capacity(data.len());
    for (k, _) in data.iter() {
        if let Some(r) = file.lookup(k).unwrap() {
            let val = r.to_vec();
            found.push(val);
        } else {
            error!("key='{}' not found", hex(k));
        }
    }
    millis = now.elapsed().unwrap_or_default().as_millis();
    info!(
        "lookup: {} ms (rate={} op/s)",
        millis,
        count as u128 * 1000 / millis
    );

    for ((k, v), r) in data.iter().zip(found.iter()) {
        if v != r {
            error!(
                "key='{}': expected '{}' but got '{}'",
                hex(k),
                hex(v),
                hex(r)
            );
        }
    }

    now = SystemTime::now();
    let min = file.min().unwrap().unwrap().to_vec();
    let max = file.max().unwrap().unwrap().to_vec();
    info!("iter: min={} max={}", hex(&min), hex(&max));
    let mut this = min.clone();
    let mut n = 1usize;
    loop {
        if let Ok(Some(r)) = file.above(&this) {
            n += 1;
            let next = r.to_vec();
            if next <= this {
                error!(
                    "iter:  asc order violated: {} comes before {} (n={})",
                    hex(&this),
                    hex(&next),
                    n
                );
                break;
            }
            this = next;
        } else if this < max || n < data.len() {
            error!("iter: failed to call above={} (n={})", hex(&this), n);
            break;
        } else {
            break;
        }
    }
    if let Some(x) = file.below(&min).unwrap() {
        error!("below min returned {}", hex(x.as_ref()));
    }
    millis = now.elapsed().unwrap_or_default().as_millis();
    info!(
        "iter:  asc {} ms (rate={} op/s) n={}",
        millis,
        count as u128 * 1000 / millis,
        n
    );

    now = SystemTime::now();
    let mut this = max.clone();
    let mut n = 1usize;
    loop {
        if let Ok(Some(r)) = file.below(&this) {
            n += 1;
            let next = r.to_vec();
            if next >= this {
                error!(
                    "iter: desc order violated: {} comes before {} (n={})",
                    hex(&this),
                    hex(&next),
                    n
                );
                break;
            }
            this = next;
        } else if this > min || n < data.len() {
            error!("iter: failed to call below={} (n={})", hex(&this), n);
            break;
        } else {
            break;
        }
    }
    if let Some(x) = file.above(&max).unwrap() {
        error!("above max returned {}", hex(x.as_ref()));
    }
    millis = now.elapsed().unwrap_or_default().as_millis();
    info!(
        "iter: desc {} ms (rate={} op/s) n={}",
        millis,
        count as u128 * 1000 / millis,
        n
    );

    now = SystemTime::now();
    for (key, _) in util::shuffle(data, 42).iter() {
        file.remove(key).unwrap();
        let opt = file.lookup(key).unwrap();
        if let Some(r) = opt {
            error!("key='{}' not removed", hex(r.as_ref()));
        }
    }
    millis = now.elapsed().unwrap_or_default().as_millis();
    info!(
        "remove: {} ms (rate={} op/s)",
        millis,
        count as u128 * 1000 / millis
    );

    if !file.is_empty() {
        error!("non-empty file");
    }
}
