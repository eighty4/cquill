use std::sync::Arc;
use std::{path::PathBuf, str};

use anyhow::{Result, anyhow};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

pub use crate::cql_file::CqlFile;
use crate::cqlshrc::session_from_cqlshrc;
pub use crate::migrate::{MigrateError, MigrateErrorState};
use crate::queries::*;
use crate::{keyspace::*, queries::keyspace::CreateKeyspaceError};

mod cql_file;
mod cqlshrc;
pub mod keyspace;
mod migrate;
mod queries;
#[cfg(test)]
pub(crate) mod test_utils;

pub const KEYSPACE: &str = "cquill";

pub const TABLE: &str = "migrated_cql";

pub struct MigrateOpts {
    pub connection_init: Option<ConnectionInit>,
    pub cql_dir: PathBuf,
    pub history_keyspace: Option<KeyspaceOpts>,
    pub history_table: Option<String>,
}

#[derive(Default)]
pub struct ConnectionOpts {
    pub hostname: Option<String>,
    pub port: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Default)]
pub struct CqlshrcOpts {
    pub path: Option<PathBuf>,
    pub overrides: ConnectionOpts,
}

pub enum ConnectionInit {
    /// Use a cqlshrc ini file to configure a connection.
    ///
    /// [`CqlshrcOpts`] allows overriding `cqlshrc` values
    /// and specifying the path to the `cqlshrc` ini file.
    ///
    /// [`CqlshrcOpts::default`] will use `~/.cassandra/cqlshrc`
    /// without any connection config overrides.
    Cqlshrc(CqlshrcOpts),

    /// Use a `scylla` crate `SessionBuilder` to specify complex
    /// auth schemes and mLTS to provide robust and secure connections
    /// to Amazon Keyframes, Astra DB, Cassandra & ScyllaDB.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use cquill::ConnectionInit;
    /// use scylla::client::session_builder::SessionBuilder;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     ConnectionInit::Session(Arc::new(SessionBuilder::new()
    ///         .known_node("127.0.0.1")
    ///         .build()
    ///         .await
    ///         .unwrap()));
    /// }
    /// ```
    Session(Arc<Session>),

    /// Specify a hostname or hostname & port for a simple TCP connection.
    /// [`ConnectionOpts`] supports PasswordAuthenticator connections with
    /// [`ConnectionOpts::username`] and [`ConnectionOpts::password`].
    ///
    /// [`ConnectionInit`] will default to `127.0.0.1` and `:9042`.
    ///
    /// ```
    /// use cquill::{ConnectionInit, ConnectionOpts};
    ///
    /// ConnectionInit::SimpleTcp(Some(ConnectionOpts{
    ///     hostname: Some("us-east-1.scylla.swissfjord.com".into()),
    ///     port: None,
    ///     username: Some("bjarne".into()),
    ///     password: Some("definedBehavior".into()),
    /// }));
    /// ```
    SimpleTcp(Option<ConnectionOpts>),
}

impl Default for ConnectionInit {
    fn default() -> Self {
        ConnectionInit::SimpleTcp(None)
    }
}

impl ConnectionInit {
    async fn session(&self) -> Result<Arc<Session>> {
        match self {
            ConnectionInit::Cqlshrc(cqlshrc_opts) => session_from_cqlshrc(cqlshrc_opts).await,
            ConnectionInit::Session(session) => Ok(session.clone()),
            ConnectionInit::SimpleTcp(opts) => session_from_opts(opts).await,
        }
    }
}

async fn session_from_opts(opts: &Option<ConnectionOpts>) -> Result<Arc<Session>> {
    let (address, username, password): (String, Option<String>, Option<String>) = match opts {
        None => ("127.0.0.1:9042".to_string(), None, None),
        Some(opts) => {
            let address = format!(
                "{}:{}",
                opts.hostname.as_deref().unwrap_or("127.0.0.1"),
                opts.port.unwrap_or(9042)
            );
            (address, None, None)
        }
    };
    let mut building = SessionBuilder::new().known_node(&address);
    if let Some(username) = username
        && let Some(password) = password
    {
        building = building.user(username, password);
    }
    let connecting = building.build().await;
    match connecting {
        Ok(session) => Ok(Arc::new(session)),
        Err(err) => Err(anyhow!("could not connect to {address}: {err}")),
    }
}

/// `migrate_cql` performs a migration of all newly added cql scripts in [MigrateOpts::cql_dir]
/// since its last invocation. Migrated scripts are tracked in a cquill keyspace and history table
/// specified with [MigrateOpts::history_keyspace] and [MigrateOpts::history_table]. A successful
/// method result contains a vec of the cql script paths executed during this invocation.
pub async fn migrate_cql(opts: MigrateOpts) -> Result<Vec<CqlFile>, MigrateError> {
    let cql_files = cql_file::files_from_dir(&opts.cql_dir)?;
    let session = opts.connection_init.unwrap_or_default().session().await?;

    let cquill_keyspace = opts
        .history_keyspace
        .unwrap_or_else(|| KeyspaceOpts::simple(String::from(KEYSPACE), 1));
    let history_table = opts.history_table.unwrap_or_else(|| String::from(TABLE));
    prepare_cquill_keyspace(&session, &cquill_keyspace, &history_table).await?;

    migrate::perform(
        &session,
        &cql_files,
        migrate::MigrateArgs {
            cql_dir: opts.cql_dir,
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
) -> Result<(), CreateKeyspaceError> {
    let create_table: bool = match get_keyspace_table_names(session, &keyspace.name) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_init_defaults_simple_tcp_no_opts() {
        match ConnectionInit::default() {
            ConnectionInit::SimpleTcp(opts) => assert!(opts.is_none()),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn test_prepare_cquill_keyspace_when_keyspace_does_not_exist() {
        let session = test_utils::cql_session().await;
        let keyspace_opts = KeyspaceOpts::simple(test_utils::keyspace_name(), 1);
        let table_name = String::from("table_name");

        if let Err(err) = prepare_cquill_keyspace(&session, &keyspace_opts, &table_name).await {
            println!("{err}");
            panic!();
        }
        match get_keyspace_table_names(&session, &keyspace_opts.name) {
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

        prepare_cquill_keyspace(&session, &keyspace_opts, &table_name)
            .await
            .expect("prepare keyspace");
        match get_keyspace_table_names(&session, &keyspace_opts.name) {
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

        prepare_cquill_keyspace(
            &harness.session,
            &KeyspaceOpts::simple(harness.cquill_keyspace.clone(), 1),
            &harness.cquill_table,
        )
        .await
        .expect("prepare keyspace");
        match get_keyspace_table_names(&harness.session, &harness.cquill_keyspace) {
            Ok(table_names) => assert!(table_names.contains(&harness.cquill_table)),
            Err(_) => panic!(),
        }

        harness.drop_keyspace().await;
    }
}
