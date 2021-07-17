use std::path::Path;
use log::{error, info};
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};
use crate::api::tree::Tree;
use crate::disk::block::Block;
use crate::disk::file::File;
use crate::util::hex::hex;

pub(crate) mod api;
pub(crate) mod disk;
pub(crate) mod util;

fn setup_logger() -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .chain(fern::log_file("yakvdb.log")?)
        .apply()?;
    Ok(())
}

fn main() {
    setup_logger().expect("logger");

    let path = Path::new("target/main_10k.tmp");
    let size: u32 = 4096;

    let mut file: File<Block> = if path.exists() {
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
        //File::open(path).unwrap() // TODO perform cleanup/compaction when opening existing file
        File::make(path, size).unwrap()
    } else {
        File::make(path, size).unwrap()
    };

    let mut rng = StdRng::seed_from_u64(42);
    let count = 11918 + 1;  // TODO Still works on 11918, but fails on 11919 (seed: 42)
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
        let opt = file.lookup(k).unwrap();
        if let Some(r) = opt {
            let val = r.to_vec();
            if val != v.to_vec() {
                error!("key='{}' expected val='{}' but got '{}'", hex(k), hex(v), hex(&val));
            }
        } else {
            error!("key='{}' not found", hex(k));
        }
    }

    for (key, _) in data.iter() {
        file.remove(key).unwrap();
        let opt = file.lookup(key).unwrap();
        if let Some(r) = opt {
            error!("key='{}' not removed", hex(r.as_ref()));
        }
    }

    info!("file={:?} count={} page={}", path, count, size);
}
