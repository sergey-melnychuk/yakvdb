use crate::api::page::Page;
use crate::api::tree::Tree;
use crate::disk::block::Block;
use crate::disk::file::File;
use crate::util::hex::hex;
use log::{debug, error, info};
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};
use std::path::Path;
use std::time::SystemTime;

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
        //.chain(fern::log_file("yakvdb.log")?) // TODO set up log rotation
        .apply()?;
    Ok(())
}

fn main() {
    setup_logger().expect("logger");

    let path = Path::new("target/main_1M.tmp");
    let size: u32 = 4096; // TODO handle keys/values larger than (half-) page size

    let mut file: File<Block> = if path.exists() {
        File::open(path).unwrap()
    } else {
        File::make(path, size).unwrap()
    };

    let mut rng = StdRng::seed_from_u64(42);
    let count = 1000 * 1000;
    let data = (0..count)
        .into_iter()
        .map(|_| {
            (
                rng.next_u64().to_be_bytes().to_vec(),
                rng.next_u64().to_be_bytes().to_vec(),
            )
        })
        .collect::<Vec<_>>();

    let mut now = SystemTime::now();

    for (k, v) in data.iter() {
        debug!("insert: key='{}' val='{}'", hex(k), hex(v));
        file.insert(k, v).unwrap();
    }

    let mut millis = now.elapsed().unwrap_or_default().as_millis();
    info!("insert: {} ms (rate={} op/s)", millis, count as u128 * 1000 / millis);

    let full = {
        let root = file.root();
        root.full()
    };
    debug!("root.full={}", full);

    now = SystemTime::now();

    for (k, v) in data.iter() {
        let opt = file.lookup(k).unwrap();
        if let Some(r) = opt {
            let val = r.to_vec();
            if val != v.to_vec() {
                error!(
                    "key='{}' expected val='{}' but got '{}'",
                    hex(k),
                    hex(v),
                    hex(&val)
                );
            }
        } else {
            error!("key='{}' not found", hex(k));
        }
    }

    millis = now.elapsed().unwrap_or_default().as_millis();
    info!("lookup: {} ms (rate={} op/s)", millis, count as u128 * 1000 / millis);

    now = SystemTime::now();

    for (key, _) in data.iter() {
        file.remove(key).unwrap();
        let opt = file.lookup(key).unwrap();
        if let Some(r) = opt {
            error!("key='{}' not removed", hex(r.as_ref()));
        }
    }

    millis = now.elapsed().unwrap_or_default().as_millis();
    info!("remove: {} ms (rate={} op/s)", millis, count as u128 * 1000 / millis);

    info!("file={:?} count={} page={}", path, count, size);
}
