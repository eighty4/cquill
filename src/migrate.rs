use std::collections::VecDeque;
use std::path::PathBuf;

use anyhow::Result;
use scylla::Session;

use crate::cql_file::{CqlFile, CqlStatement};
use crate::queries;
use crate::queries::QueryError;

#[derive(thiserror::Error, Debug)]
pub enum MigrateError {
    #[error("error reading {filename} from disk: {error}")]
    CqlFileReadError { filename: String, error: String },
    #[error("cql query error: {source}")]
    CqlQueryError {
        #[from]
        source: QueryError,
    },
    #[error("previously migrated file '{filename}' has been modified (its current contents do not match the migrated cql file's content hash recorded in {cquill_keyspace}.{cquill_table})")]
    HashConflict {
        filename: String,
        cquill_keyspace: String,
        cquill_table: String,
    },
    #[error("errored saving migrate status of '{0}' to {cquill_keyspace}.{cquill_table}: {1}", error_state.failed_file.filename, error_state.error)]
    HistoryUpdateFailed {
        cquill_keyspace: String,
        cquill_table: String,
        error_state: Box<MigrateErrorState>,
    },
    #[error("errored during migrate of '{0}': {1}", error_state.failed_file.filename, error_state.error)]
    PartialMigration { error_state: Box<MigrateErrorState> },
    #[error("{source}")]
    Other {
        #[from]
        source: anyhow::Error,
    },
}

#[derive(Debug)]
pub struct MigrateErrorState {
    pub error: String,
    pub failed_cql: Option<CqlStatement>,
    pub failed_file: CqlFile,
    pub migrated: Vec<CqlFile>,
}

pub(crate) struct MigrateArgs {
    pub cql_dir: PathBuf,
    pub history_keyspace: String,
    pub history_table: String,
}

pub(crate) async fn perform(
    session: &Session,
    cql_files: &[CqlFile],
    args: MigrateArgs,
) -> Result<Vec<CqlFile>, MigrateError> {
    let mut previously_migrated = VecDeque::from(
        queries::migrated::files::select_all(
            session,
            &args.history_keyspace,
            &args.history_table,
            &args.cql_dir,
        )
        .await?,
    );
    let mut not_migrated: Vec<(CqlFile, Vec<CqlStatement>)> = Vec::new();
    for cql_file in cql_files {
        if let Some(migrated_cql_file) = previously_migrated.pop_front() {
            if cql_file.hash == migrated_cql_file.hash {
                continue;
            } else {
                return Err(MigrateError::HashConflict {
                    filename: cql_file.filename.clone(),
                    cquill_keyspace: args.history_keyspace.clone(),
                    cquill_table: args.history_table.clone(),
                });
            }
        }
        let cql = cql_file.read_statements()?;
        not_migrated.push((cql_file.clone(), cql));
    }
    let mut migrated: Vec<CqlFile> = Vec::new();
    for cql in not_migrated {
        for cql_statement in cql.1 {
            if let Err(err) = queries::exec(session, cql_statement.cql.clone()).await {
                return Err(MigrateError::PartialMigration {
                    error_state: Box::from(MigrateErrorState {
                        error: err.to_string(),
                        failed_cql: Some(cql_statement),
                        failed_file: cql.0,
                        migrated,
                    }),
                });
            }
        }
        migrated.push(cql.0.clone());
        if let Err(err) = queries::migrated::files::insert(
            session,
            &args.history_keyspace,
            &args.history_table,
            &cql.0,
        )
        .await
        {
            return Err(MigrateError::HistoryUpdateFailed {
                error_state: Box::from(MigrateErrorState {
                    error: err.to_string(),
                    failed_file: cql.0,
                    failed_cql: None,
                    migrated,
                }),
                cquill_keyspace: args.history_keyspace,
                cquill_table: args.history_table,
            });
        };
    }
    Ok(migrated)
}

#[cfg(test)]
mod tests {
    use crate::test_utils;

    use super::*;

    #[tokio::test]
    async fn test_migrate_fresh_state() {
        let harness = test_utils::TestHarness::builder()
            .cql_file("v001.cql", "")
            .cql_file("v002.cql", "")
            .cql_file("v003.cql", "")
            .initialize()
            .await;

        let migrate_result =
            perform(&harness.session, &harness.cql_files, harness.migrate_args()).await;
        match migrate_result {
            Err(err) => panic!("{err}"),
            Ok(migrated_files) => {
                assert_eq!(migrated_files.len(), 3);
            }
        }

        harness.drop_keyspace().await;
    }

    #[tokio::test]
    async fn test_migrate_skip_migrated() {
        let harness = test_utils::TestHarness::builder()
            .cql_file("v001.cql", "")
            .cql_file("v002.cql", "")
            .cql_file("v003.cql", "")
            .initialize()
            .await;
        queries::migrated::files::insert(
            &harness.session,
            &harness.cquill_keyspace,
            &harness.cquill_table.clone(),
            &CqlFile::from_path(harness.cql_file_path("v001.cql")).unwrap(),
        )
        .await
        .expect("save migrated file");

        let migrate_result =
            perform(&harness.session, &harness.cql_files, harness.migrate_args()).await;
        match migrate_result {
            Err(err) => panic!("{err}"),
            Ok(migrated_files) => {
                assert_eq!(migrated_files.len(), 2);
                let migrated_file_names: Vec<&str> =
                    migrated_files.iter().map(|f| f.filename.as_str()).collect();
                assert!(!migrated_file_names.contains(&"v001.cql"));
            }
        }

        harness.drop_keyspace().await;
    }

    #[tokio::test]
    async fn test_migrate_errors_when_executed_cql_content_changed() {
        let harness = test_utils::TestHarness::builder()
            .cql_file("v001.cql", "")
            .cql_file("v002.cql", "")
            .cql_file("v003.cql", "")
            .initialize()
            .await;
        queries::migrated::files::insert(
            &harness.session,
            &harness.cquill_keyspace,
            &harness.cquill_table.clone(),
            &CqlFile {
                version: 1,
                hash: "abc".to_string(),
                path: harness.cql_file_path("v001.cql"),
                filename: "v001.cql".to_string(),
            },
        )
        .await
        .expect("save migrated file");

        let migrate_result =
            perform(&harness.session, &harness.cql_files, harness.migrate_args()).await;
        match migrate_result {
            Ok(_) => panic!(),
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    format!("previously migrated file 'v001.cql' has been modified (its current contents do not match the migrated cql file's content hash recorded in {}.{})",
                            harness.cquill_keyspace, harness.cquill_table)
                );
            }
        }

        harness.drop_keyspace().await;
    }

    #[tokio::test]
    async fn test_partial_migration_error_state() {
        let keyspace = test_utils::keyspace_name();
        let harness = test_utils::TestHarness::builder()
            .cql_file("v001.cql", "")
            .cql_file(
                "v002.cql",
                format!(
                    "CREATE TABLE {}.asdf (id UUID PRIMARY KEY, data TEXT); CREATE TABLE;",
                    keyspace
                )
                .as_str(),
            )
            .cquill_history(keyspace.as_str(), "cquill")
            .initialize()
            .await;

        let migrate_result =
            perform(&harness.session, &harness.cql_files, harness.migrate_args()).await;
        match migrate_result {
            Ok(_) => panic!(),
            Err(err) => match err {
                MigrateError::PartialMigration { error_state } => {
                    assert_eq!(error_state.migrated.len(), 1);
                    assert_eq!(error_state.migrated.first().unwrap().filename, "v001.cql");
                    assert!(error_state.failed_cql.is_some());
                    assert_eq!(error_state.failed_cql.unwrap().cql, "CREATE TABLE");
                    assert_eq!(error_state.failed_file.filename, "v002.cql");
                    assert!(error_state.error.starts_with("cql query error: Database returned an error: The submitted query has a syntax error, Error message:"));
                }
                _ => panic!("error was not a MigrateError::PartialMigration"),
            },
        }

        harness.drop_keyspace().await;
    }
}
