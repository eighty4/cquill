use std::process::ExitCode;

use cquill::{CqlFile, MigrateError};

pub fn example_cql_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("examples")
        .join("cql")
}

pub fn on_migrate_result(result: Result<Vec<CqlFile>, MigrateError>) -> ExitCode {
    match result {
        Err(err) => {
            println!("EXAMPLE ERRORED: {}", err);
            ExitCode::FAILURE
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
            ExitCode::SUCCESS
        }
    }
}
