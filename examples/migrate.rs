use std::path::PathBuf;
use std::process::exit;

use cquill::*;

#[tokio::main]
async fn main() {
    let opts = MigrateOpts {
        cassandra_opts: None,
        cql_dir: PathBuf::from("examples/cql"),
        history_keyspace: None,
        history_table: None,
    };
    match migrate_cql(opts).await {
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
