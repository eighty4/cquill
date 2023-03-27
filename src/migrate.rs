use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use scylla::Session;

use crate::cql::CqlFile;
use crate::queries;

pub(crate) struct MigrateArgs {
    pub cql_dir: PathBuf,
    pub history_keyspace: String,
    pub history_table: String,
}

pub(crate) async fn perform(
    session: &Session,
    cql_files: Vec<CqlFile>,
    args: MigrateArgs,
) -> Result<Vec<CqlFile>> {
    let previously_migrated = select_migrated_cql_content_hashes(session, &args).await?;
    let mut migrated: Vec<CqlFile> = Vec::new();
    for cql_file in cql_files {
        if let Some(prev_cql_hash) = previously_migrated.get(&cql_file.filename) {
            if cql_file.hash == *prev_cql_hash {
                continue;
            } else {
                return Err(anyhow!(
                    "previously migrated file '{}' has been modified (its current contents do not match the migrated cql file's content hash recorded in {}.{})",
                    cql_file.filename,
                    args.history_keyspace,
                    args.history_table
                ));
            }
        }
        for cql_statement in read_statements(&cql_file.path)? {
            // println!("\n---\n{cql_statement}\n---");
            session.query(cql_statement, ()).await?;
        }
        queries::migrated::files::insert(
            session,
            &args.history_keyspace,
            &args.history_table,
            &cql_file,
        )
        .await?;
        migrated.push(cql_file);
    }
    Ok(migrated)
}

async fn select_migrated_cql_content_hashes(
    session: &Session,
    args: &MigrateArgs,
) -> Result<HashMap<String, String>> {
    let migrated_cql_files = queries::migrated::files::select_all(
        session,
        &args.history_keyspace,
        &args.history_table,
        &args.cql_dir,
    )
    .await?;
    let mut result = HashMap::with_capacity(migrated_cql_files.len());
    for migrated in migrated_cql_files {
        result.insert(migrated.filename, migrated.hash);
    }
    Ok(result)
}

fn read_statements(cql_file: &PathBuf) -> Result<Vec<String>> {
    let statements = fs::read_to_string(cql_file)?
        .split(';')
        .map(|s| s.replace('\n', "").trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use crate::cql::CqlFile;
    use crate::migrate::{perform, MigrateArgs};
    use crate::{queries, test_utils};

    #[tokio::test]
    async fn test_migrate_fresh_state() {
        let harness = test_utils::TestHarness::builder()
            .cql_file("v001.cql", "")
            .cql_file("v002.cql", "")
            .cql_file("v003.cql", "")
            .build();
        let session = harness.setup().await.expect("cql session");

        let args = MigrateArgs {
            cql_dir: harness.directory.path().to_path_buf(),
            history_keyspace: harness.cquill_keyspace.name.clone(),
            history_table: harness.cquill_table.clone(),
        };
        let migrate_result = perform(&session, harness.cql_files, args).await;
        match migrate_result {
            Err(err) => test_utils::error_panic(&err),
            Ok(migrated_files) => {
                assert_eq!(migrated_files.len(), 3);
            }
        }
    }

    #[tokio::test]
    async fn test_migrate_skip_migrated() {
        let harness = test_utils::TestHarness::builder()
            .cql_file("v001.cql", "")
            .cql_file("v002.cql", "")
            .cql_file("v003.cql", "")
            .build();
        let session = harness.setup().await.expect("cql session");
        queries::migrated::files::insert(
            &session,
            &harness.cquill_keyspace.name,
            &harness.cquill_table.clone(),
            &CqlFile::from_path(harness.directory.path().join("v001.cql")).unwrap(),
        )
        .await
        .expect("save migrated file");

        let args = MigrateArgs {
            cql_dir: harness.directory.path().to_path_buf(),
            history_keyspace: harness.cquill_keyspace.name.clone(),
            history_table: harness.cquill_table.clone(),
        };
        let migrate_result = perform(&session, harness.cql_files, args).await;
        match migrate_result {
            Err(err) => test_utils::error_panic(&err),
            Ok(migrated_files) => {
                assert_eq!(migrated_files.len(), 2);
                let migrated_file_names: Vec<&str> =
                    migrated_files.iter().map(|f| f.filename.as_str()).collect();
                assert!(!migrated_file_names.contains(&"v001.cql"));
            }
        }
    }

    #[tokio::test]
    async fn test_migrate_errors_when_executed_cql_content_changed() {
        let harness = test_utils::TestHarness::builder()
            .cql_file("v001.cql", "")
            .cql_file("v002.cql", "")
            .cql_file("v003.cql", "")
            .build();
        let session = harness.setup().await.expect("cql session");
        queries::migrated::files::insert(
            &session,
            &harness.cquill_keyspace.name,
            &harness.cquill_table.clone(),
            &CqlFile {
                version: 1,
                hash: "abc".to_string(),
                path: harness.directory.path().join("v001.cql"),
                filename: "v001.cql".to_string(),
            },
        )
        .await
        .expect("save migrated file");

        let args = MigrateArgs {
            cql_dir: harness.directory.path().to_path_buf(),
            history_keyspace: harness.cquill_keyspace.name.clone(),
            history_table: harness.cquill_table.clone(),
        };
        let migrate_result = perform(&session, harness.cql_files, args).await;
        match migrate_result {
            Ok(_) => panic!(),
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    format!("previously migrated file 'v001.cql' has been modified (its current contents do not match the migrated cql file's content hash recorded in {}.{})",
                            harness.cquill_keyspace.name, harness.cquill_table)
                );
            }
        }
    }
}
