extern crate rand;
extern crate scylla;

use rand::Rng;
use scylla::Session;

pub(crate) async fn cql_session() -> Session {
    let node_address = "127.0.0.1:9042";
    scylla::SessionBuilder::new()
        .known_node(node_address)
        .build()
        .await
        .unwrap()
}

fn alphanumeric_str(len: u8) -> String {
    let mut rng = rand::thread_rng();
    (0..len)
        .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
        .collect()
}

pub(crate) fn keyspace_name() -> String {
    format!("cquill_test_{}", alphanumeric_str(6))
}
