use std::{path::PathBuf, str, sync::Arc};

use anyhow::Result;
use scylla::Session;

pub use crate::cql_file::CqlFile;
use crate::keyspace::*;
pub use crate::migrate::{MigrateError, MigrateErrorState};
use crate::queries::*;

#[allow(unused)]
mod cql;
mod cql_file;
pub mod keyspace;
mod migrate;
mod queries;
#[cfg(test)]
pub(crate) mod test_utils;

pub const KEYSPACE: &str = "cquill";

pub const TABLE: &str = "migrated_cql";

pub struct Migrator {
    session: Arc<Session>,
    pub migrations_dir: PathBuf,
    pub history_keyspace: Option<KeyspaceOpts>,
    pub history_table: Option<String>,
}

impl Migrator {
    pub fn new(session: Arc<Session>, migrations_dir: PathBuf) -> Self {
        Self {
            session,
            migrations_dir,
            history_keyspace: None,
            history_table: None,
        }
    }

    /// Specify a keyspace for the migration tracking table. If not
    /// specified, the default - `cquill` - is used.
    pub fn with_keyspace(self, keyspace: KeyspaceOpts) -> Self {
        Self {
            history_keyspace: Some(keyspace),
            ..self
        }
    }

    /// Specify a name for the migration tracking table. If not
    /// specified, the default - `migrated_cql` - is used.
    pub fn with_table_name(self, name: String) -> Self {
        Self {
            history_table: Some(name),
            ..self
        }
    }

    async fn prepare_db(
        session: &Session,
        keyspace: &KeyspaceOpts,
        table: &String,
    ) -> Result<(), MigrateError> {
        // look for the table, creating the keyspace as needed
        let need_table = match table_names_from_session_metadata(session, &keyspace.name) {
            Ok(tables) => !tables.contains(&table),
            Err(_) => {
                queries::keyspace::create(session, keyspace).await?;
                true
            }
        };

        if need_table {
            migrated::table::create(session, &keyspace.name, table).await?;
        }

        Ok(())
    }

    /// performs a migration of all newly added cql scripts in [MigrateOpts::cql_dir]
    /// since its last invocation. Migrated scripts are tracked in a cquill keyspace and history table
    /// specified with [MigrateOpts::history_keyspace] and [MigrateOpts::history_table]. A successful
    /// method result contains a vec of the cql script paths executed during this invocation.
    pub async fn run_pending(self) -> Result<Vec<CqlFile>, MigrateError> {
        let discovered_migrations = cql_file::files_from_dir(&self.migrations_dir)?;
        let keyspace = self
            .history_keyspace
            .unwrap_or_else(|| KeyspaceOpts::simple(KEYSPACE.into(), 1));
        let table = self.history_table.unwrap_or_else(|| TABLE.into());

        Self::prepare_db(&self.session, &keyspace, &table).await?;

        migrate::perform(
            &self.session,
            &discovered_migrations,
            migrate::MigrateArgs {
                cql_dir: self.migrations_dir,
                history_keyspace: keyspace.name,
                history_table: table,
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_prepare_cquill_keyspace_when_keyspace_does_not_exist() {
        let session = test_utils::cql_session().await;
        let keyspace_opts = KeyspaceOpts::simple(test_utils::keyspace_name(), 1);
        let table_name = String::from("table_name");

        if let Err(err) = Migrator::prepare_db(&session, &keyspace_opts, &table_name).await {
            println!("{err}");
            panic!();
        }

        match table_names_from_session_metadata(&session, &keyspace_opts.name) {
            Ok(table_names) => assert!(table_names.contains(&table_name)),
            Err(_) => panic!(),
        }

        queries::keyspace::drop(&session, &keyspace_opts.name)
            .await
            .expect("drop keyspace");
    }

    #[tokio::test]
    async fn test_prepare_cquill_keyspace_when_table_does_not_exist() {
        let session = test_utils::cql_session().await;
        let keyspace_opts = test_utils::create_keyspace(&session).await;
        let table_name = String::from("table_name");

        Migrator::prepare_db(&session, &keyspace_opts, &table_name)
            .await
            .expect("prepare keyspace");
        match table_names_from_session_metadata(&session, &keyspace_opts.name) {
            Ok(table_names) => assert!(table_names.contains(&table_name)),
            Err(_) => panic!(),
        }

        queries::keyspace::drop(&session, &keyspace_opts.name)
            .await
            .expect("drop keyspace");
    }

    #[tokio::test]
    async fn test_prepare_cquill_keyspace_when_keyspace_and_table_exist() {
        let harness = test_utils::TestHarness::builder().initialize().await;

        Migrator::prepare_db(
            &harness.session,
            &KeyspaceOpts::simple(harness.cquill_keyspace.clone(), 1),
            &harness.cquill_table,
        )
        .await
        .expect("prepare keyspace");
        match table_names_from_session_metadata(&harness.session, &harness.cquill_keyspace) {
            Ok(table_names) => assert!(table_names.contains(&harness.cquill_table)),
            Err(_) => panic!(),
        }

        harness.drop_keyspace().await;
    }
}
