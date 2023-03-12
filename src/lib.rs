use std::{fs, path::PathBuf, str};

use anyhow::{anyhow, Result};
use scylla::Session;

use crate::keyspace::*;
use crate::queries::*;

mod cql;
pub mod keyspace;
mod migrate;
mod queries;
#[cfg(test)]
pub(crate) mod test_utils;

const NODE_ADDRESS: &str = "127.0.0.1:9042";

pub const KEYSPACE: &str = "cquill";

pub const TABLE: &str = "migrated_cql";

pub struct MigrateOpts {
    pub cassandra_opts: Option<CassandraOpts>,
    pub cql_dir: PathBuf,
    pub history_keyspace: Option<KeyspaceOpts>,
    pub history_table: Option<String>,
}

#[derive(Default)]
pub struct CassandraOpts {
    pub cassandra_host: Option<String>,
}

impl CassandraOpts {
    pub fn node_address(&self) -> String {
        let node_address = match &self.cassandra_host {
            None => match std::env::var("CASSANDRA_NODE") {
                Ok(host) => host,
                Err(_) => NODE_ADDRESS.to_string(),
            },
            Some(cassandra_host) => cassandra_host.clone(),
        };
        if node_address.contains(':') {
            node_address
        } else {
            format!("{node_address}:9042")
        }
    }
}

/// `migrate_cql` performs a migration of all newly added cql scripts in [MigrateOpts::cql_dir]
/// since its last invocation. Migrated scripts are tracked in a cquill keyspace and history table
/// specified with [MigrateOpts::history_keyspace] and [MigrateOpts::history_table]. A successful
/// method result contains a vec of the cql script paths executed during this invocation.
pub async fn migrate_cql(opts: MigrateOpts) -> Result<Vec<PathBuf>> {
    let cql_files = cql_files_from_dir(&opts.cql_dir)?;
    let node_address = opts.cassandra_opts.unwrap_or_default().node_address();
    let session = cql_session(node_address).await?;

    let cquill_keyspace = opts
        .history_keyspace
        .unwrap_or_else(|| KeyspaceOpts::simple(String::from(KEYSPACE), 1));
    let history_table = opts.history_table.unwrap_or_else(|| String::from(TABLE));
    prepare_cquill_keyspace(&session, &cquill_keyspace, &history_table).await?;

    migrate::perform(
        &session,
        cql_files,
        migrate::MigrateArgs {
            history_keyspace: cquill_keyspace.name,
            history_table,
        },
    )
    .await
}

// todo drop and recreate dev mode
async fn prepare_cquill_keyspace(
    session: &Session,
    keyspace: &KeyspaceOpts,
    table_name: &String,
) -> Result<()> {
    let create_table: bool = match table_names_from_session_metadata(session, &keyspace.name) {
        Ok(table_names) => !table_names.contains(table_name),
        Err(_) => {
            queries::keyspace::create(session, keyspace).await?;
            true
        }
    };
    if create_table {
        migrated::table::create(session, &keyspace.name, table_name).await?;
    }
    Ok(())
}

fn cql_files_from_dir(cql_dir: &PathBuf) -> Result<Vec<PathBuf>> {
    match fs::read_dir(cql_dir) {
        Ok(read_dir) => {
            let mut result = Vec::new();
            for dir_entry in read_dir {
                let path = dir_entry?.path();
                if path.is_file() && path.file_name().is_some() {
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
    }
}

async fn cql_session(node_address: String) -> Result<Session> {
    let connecting = scylla::SessionBuilder::new()
        .known_node(&node_address)
        .build()
        .await;
    match connecting {
        Ok(session) => Ok(session),
        Err(_) => Err(anyhow!("could not connect to {}", &node_address)),
    }
}

#[cfg(test)]
mod tests {
    use temp_dir::TempDir;

    use crate::test_utils::make_file;

    use super::*;

    #[test]
    fn test_cql_files_from_dir() {
        let temp_dir = TempDir::new().unwrap();
        ["foo.cql", "foo.sh", "foo.sql"]
            .iter()
            .for_each(|f| make_file(temp_dir.path().join(f)));
        let temp_dir_path = temp_dir.path().canonicalize().unwrap();

        match cql_files_from_dir(&temp_dir_path) {
            Err(err) => {
                println!("{err}");
                panic!();
            }
            Ok(cql_files) => {
                assert_eq!(cql_files.len(), 1);
                assert!(cql_files
                    .iter()
                    .any(|p| { p.file_name().unwrap() == "foo.cql" }));
            }
        }
    }

    #[test]
    fn test_cassandra_opts_provides_node_address() {
        let without_host = CassandraOpts {
            cassandra_host: None,
        };
        let with_host = CassandraOpts {
            cassandra_host: Some("localhost".to_string()),
        };
        let with_port = CassandraOpts {
            cassandra_host: Some("localhost:9043".to_string()),
        };
        assert_eq!(without_host.node_address(), "127.0.0.1:9042");
        assert_eq!(with_host.node_address(), "localhost:9042");
        assert_eq!(with_port.node_address(), "localhost:9043");
    }

    #[tokio::test]
    async fn test_prepare_cquill_keyspace_when_keyspace_does_not_exist() {
        let session = cql_session(NODE_ADDRESS.to_string()).await.unwrap();
        let keyspace_opts = KeyspaceOpts::simple(queries::test_utils::keyspace_name(), 1);
        let table_name = String::from("table_name");

        if let Err(err) = prepare_cquill_keyspace(&session, &keyspace_opts, &table_name).await {
            println!("{err}");
            panic!();
        }
        match table_names_from_session_metadata(&session, &keyspace_opts.name) {
            Ok(table_names) => assert!(table_names.contains(&table_name)),
            Err(_) => panic!(),
        }
    }

    #[tokio::test]
    async fn test_prepare_cquill_keyspace_when_table_does_not_exist() {
        let session = cql_session(NODE_ADDRESS.to_string()).await.unwrap();
        let keyspace_opts = KeyspaceOpts::simple(queries::test_utils::keyspace_name(), 1);
        queries::keyspace::create(&session, &keyspace_opts)
            .await
            .expect("create keyspace");
        let table_name = String::from("table_name");

        if let Err(err) = prepare_cquill_keyspace(&session, &keyspace_opts, &table_name).await {
            println!("{err}");
            panic!();
        }
        match table_names_from_session_metadata(&session, &keyspace_opts.name) {
            Ok(table_names) => assert!(table_names.contains(&table_name)),
            Err(_) => panic!(),
        }
    }

    #[tokio::test]
    async fn test_prepare_cquill_keyspace_when_keyspace_and_table_exist() {
        let session = cql_session(NODE_ADDRESS.to_string()).await.unwrap();
        let keyspace_opts = KeyspaceOpts::simple(queries::test_utils::keyspace_name(), 1);
        queries::keyspace::create(&session, &keyspace_opts)
            .await
            .expect("create keyspace");
        let table_name = String::from("table_name");
        migrated::table::create(&session, &keyspace_opts.name, &table_name)
            .await
            .expect("create table");

        if let Err(err) = prepare_cquill_keyspace(&session, &keyspace_opts, &table_name).await {
            println!("{err}");
            panic!();
        }
        match table_names_from_session_metadata(&session, &keyspace_opts.name) {
            Ok(table_names) => assert!(table_names.contains(&table_name)),
            Err(_) => panic!(),
        }
    }
}
