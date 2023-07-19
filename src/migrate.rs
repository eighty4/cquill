use std::collections::VecDeque;
use std::path::PathBuf;

use anyhow::Result;
use scylla::Session;

use crate::cql::CqlFile;
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
    #[error("{source}")]
    Other {
        #[from]
        source: anyhow::Error,
    },
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
    let mut not_migrated: Vec<(CqlFile, Vec<String>)> = Vec::new();
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
            queries::exec(session, cql_statement).await?
        }
        queries::migrated::files::insert(
            session,
            &args.history_keyspace,
            &args.history_table,
            &cql.0,
        )
        .await?;
        migrated.push(cql.0);
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
            Err(err) => test_utils::error_panic(&err),
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
            Err(err) => test_utils::error_panic(&err),
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
}
