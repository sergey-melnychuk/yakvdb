use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader},
    path::{Path, PathBuf},
};

use yakvdb::{
    api::Store,
    disk::{block::Block, file::File as YakFile},
};

fn path<P: AsRef<Path>>(path: P) -> PathBuf {
    path.as_ref().to_owned()
}

// https://doc.rust-lang.org/rust-by-example/std_misc/file/read_lines.html
fn lines<P: AsRef<Path>>(path: P) -> io::Result<io::Lines<BufReader<File>>> {
    let file = File::open(path.as_ref())?;
    Ok(BufReader::new(file).lines())
}

fn data<P: AsRef<Path>>(path: P) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    Ok(lines(path)?
        .into_iter()
        .flatten()
        .skip_while(|line| line.starts_with('#'))
        .filter_map(|line| {
            let mut it = line.split(' ');
            let key = hex::decode(it.next()?).ok()?;
            let val = hex::decode(it.next()?).ok()?;
            Some((key, val))
        })
        .collect())
}

// RUST_LOG=trace cargo run --example issue-004
fn main() -> io::Result<()> {
    env_logger::init();

    let mut path = path("target/examples/issue-004");
    if path.exists() {
        fs::remove_dir_all(&path)?;
    }
    fs::create_dir_all(&path)?;
    path.push("db.yak");
    let file = YakFile::<Block>::make(&path, 4096)?;

    for (key, val) in data("./etc/issue-004.txt")? {
        file.insert(&key, &val).unwrap();
    }

    Ok(())
}
