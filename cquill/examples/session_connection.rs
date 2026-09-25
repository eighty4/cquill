use std::sync::Arc;

use cquill::*;
use scylla::client::session_builder::SessionBuilder;

#[tokio::main]
async fn main() {
    let cql_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("examples")
        .join("cql");
    let session = SessionBuilder::new()
        .known_node("127.0.0.1")
        .build()
        .await
        .unwrap();
    let opts = MigrateOpts {
        connection_opts: Some(ConnectionOpts::Session(Arc::new(session))),
        cql_dir,
        history_keyspace: None,
        history_table: None,
    };
    match migrate_cql(opts).await {
        Err(err) => {
            println!("EXAMPLE ERRORED: {}", err);
            std::process::exit(1);
        }
        Ok(migrated_cql_files) => {
            if migrated_cql_files.is_empty() {
                println!("✔ already up-to-date!");
            } else {
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
        }
    };
}
