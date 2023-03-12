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
    for cql_file in &cql_files {
        for cql_statement in read_statements(&PathBuf::from(&cql_file.filename))? {
            // println!("\n---\n{cql_statement}\n---");
            session.query(cql_statement, ()).await?;
        }

        queries::migrated::files::insert(
            session,
            &args.history_keyspace,
            &args.history_table,
            cql_file,
        )
        .await?;
    }

    Ok(cql_files)
}

fn read_statements(cql_file: &PathBuf) -> Result<Vec<String>> {
    let statements = fs::read_to_string(cql_file)?
        .split(';')
        .map(|s| s.replace('\n', "").trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    Ok(statements)
}
