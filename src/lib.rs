mod cql;
pub mod keyspace;
mod queries;

use crate::keyspace::*;
use crate::queries::*;
use anyhow::{anyhow, Result};
use scylla::Session;
use std::{fs, path::PathBuf, str};

pub const KEYSPACE: &str = "cquill";

pub const TABLE: &str = "migrated_cql";

pub struct MigrateOpts {
    pub cql_dir: PathBuf,
    pub history_keyspace: Option<KeyspaceOpts>,
    pub history_table: Option<String>,
}

/// `migrate_cql` performs a migration of all newly added cql scripts in [MigrateOpts::cql_dir]
/// since its last invocation. Migrated scripts are tracked in a cquill keyspace and history table
/// specified with [MigrateOpts::history_keyspace] and [MigrateOpts::history_table]. A successful
/// method result contains a vec of the cql script paths executed during this invocation.
pub async fn migrate_cql(opts: MigrateOpts) -> Result<Vec<PathBuf>> {
    // if cql_files_from_dir(&opts.cql_dir)?.is_empty() {
    //     return Ok(Vec::new());
    // }
    let session = cql_session().await?;

    let history_keyspace = opts
        .history_keyspace
        .unwrap_or_else(|| KeyspaceOpts::simple(String::from(KEYSPACE), 1));
    let history_table = opts.history_table.unwrap_or_else(|| String::from(TABLE));
    prepare_cquill_keyspace(&session, history_keyspace, &history_table).await?;

    Ok(Vec::new())
}

// todo drop and recreate dev mode
async fn prepare_cquill_keyspace(
    session: &Session,
    keyspace: KeyspaceOpts,
    table_name: &String,
) -> Result<()> {
    let create_table: bool =
        match queries::keyspace::select_table_names(session, &keyspace.name).await {
            Ok(table_names) => !table_names.contains(table_name),
            Err(_) => {
                queries::keyspace::create(session, &keyspace).await?;
                true
            }
        };
    if create_table {
        migrated::table::create(session, &keyspace.name, table_name).await?;
    }
    Ok(())
}

#[allow(dead_code)]
fn cql_files_from_dir(cql_dir: &PathBuf) -> Result<Vec<PathBuf>> {
    return match fs::read_dir(cql_dir) {
        Ok(read_dir) => {
            let mut result = Vec::new();
            for dir_entry in read_dir {
                let path = dir_entry?.path();
                if path.is_file() {
                    if let Some(extension) = path.extension() {
                        if extension == "cql" {
                            result.push(path);
                        }
                    }
                }
            }
            if result.is_empty() {
                return Err(anyhow!(
                    "no cql files found in directory '{}'",
                    cql_dir.to_string_lossy()
                ));
            }
            result.sort();
            Ok(result)
        }
        Err(_) => Err(anyhow!(
            "could not find directory '{}'",
            cql_dir.to_string_lossy()
        )),
    };
}

async fn cql_session() -> Result<Session> {
    let node_address = "127.0.0.1:9042";
    let connecting = scylla::SessionBuilder::new()
        .known_node(node_address)
        .build()
        .await;
    match connecting {
        Ok(session) => Ok(session),
        Err(_) => Err(anyhow!("could not connect to {}", node_address)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temp_dir::TempDir;

    #[test]
    fn test_cql_files_from_dir() {
        let temp_dir = TempDir::new().unwrap();
        ["foo.cql", "foo.sh", "foo.sql"].iter().for_each(|f| {
            fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(temp_dir.path().join(f))
                .expect("could not write file");
        });
        let temp_dir_path = temp_dir.path().canonicalize().unwrap();
        println!("{}", temp_dir_path.to_string_lossy());
        let result = cql_files_from_dir(&temp_dir_path);
        assert!(result.is_ok());
        let cql_files = result.unwrap();
        assert_eq!(cql_files.len(), 1);
        assert!(cql_files
            .iter()
            .any(|p| { p.file_name().unwrap() == "foo.cql" }));
    }
}
