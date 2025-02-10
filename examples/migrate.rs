use std::env;
use std::path::PathBuf;
use std::process::exit;

use cquill::*;
use scylla::SessionBuilder;

#[tokio::main]
async fn main() {
    let host = env::var("CASSANDRA_HOST").unwrap();
    let session = SessionBuilder::new().known_node(host).build().await.unwrap();
    let migrator = Migrator::new(session.into(), PathBuf::from("examples/cql"));
    match migrator.run_pending().await {
        Err(err) => {
            println!("EXAMPLE ERRORED: {}", err);
            exit(1);
        }
        Ok(migrated_cql_files) => {
            println!(
                "✔ {} cql file(s) migrated: {}",
                migrated_cql_files.len(),
                migrated_cql_files
                    .iter()
                    .map(|f| f.filename.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    };
}
