use std::fmt::Display;
use std::fs;
use std::path::PathBuf;

use rand::Rng;
use scylla::Session;

pub(crate) fn make_file(path: PathBuf) {
    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .expect("could not write file");
}

pub(crate) fn error_panic(err: &dyn Display) -> ! {
    println!("{err}");
    panic!();
}

pub(crate) async fn cql_session() -> Session {
    let node_address = "127.0.0.1:9042";
    scylla::SessionBuilder::new()
        .known_node(node_address)
        .build()
        .await
        .expect("cql session")
}

fn alphanumeric_str(len: u8) -> String {
    let mut rng = rand::thread_rng();
    (0..len)
        .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

pub(crate) fn keyspace_name() -> String {
    format!("cquill_test_{}", alphanumeric_str(6))
}
