use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use scylla::Session;

use crate::cql::CqlFile;
use crate::queries;

pub(crate) struct MigrateArgs {
    pub history_keyspace: String,
    pub history_table: String,
}

pub(crate) async fn perform(
    session: &Session,
    cql_files: Vec<CqlFile>,
    args: MigrateArgs,
) -> Result<Vec<CqlFile>> {
    let previously_migrated = select_migrated_cql_filenames(session, &args).await?;
    let mut migrated: Vec<CqlFile> = Vec::new();
    for cql_file in cql_files {
        if previously_migrated.contains(&cql_file.filename) {
            continue;
        }
        for cql_statement in read_statements(&PathBuf::from(&cql_file.filename))? {
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

async fn select_migrated_cql_filenames(
    session: &Session,
    args: &MigrateArgs,
) -> Result<Vec<String>> {
    let migrated_cql_files =
        queries::migrated::files::select_all(session, &args.history_keyspace, &args.history_table)
            .await?;
    let mut result = Vec::new();
    for migrated_cql_file in migrated_cql_files {
        result.push(migrated_cql_file.filename);
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
