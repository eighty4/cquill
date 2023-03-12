use std::fs;
use std::path::PathBuf;

pub(crate) fn make_file(path: PathBuf) {
    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .expect("could not write file");
}
