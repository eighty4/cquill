extern crate anyhow;
extern crate scylla;

use crate::cql::*;
use crate::keyspace::KeyspaceOpts;
use anyhow::{anyhow, Result};
use scylla::{transport::session::IntoTypedRows, Session};

pub(crate) async fn create_keyspace(session: &Session, keyspace_opts: &KeyspaceOpts) -> Result<()> {
    session
        .query(create_keyspace_cql(keyspace_opts)?, ())
        .await?;
    Ok(())
}

pub(crate) async fn create_history_table(
    session: &Session,
    keyspace_opts: &KeyspaceOpts,
    table_name: String,
) -> Result<()> {
    let cql = format!(
        "create table {}.{table_name} (id timeuuid primary key, ver int, name varchar, hash varchar)",
        keyspace_opts.name);
    session.query(cql, ()).await?;
    Ok(())
}

pub(crate) async fn select_keyspace_table_names(
    session: &Session,
    keyspace_opts: &KeyspaceOpts,
) -> Result<Vec<String>> {
    let cql = format!(
        "select table_name from system_schema.tables where keyspace_name='{}'",
        keyspace_opts.name
    );
    let query_result = session.query(cql, &[]).await;
    match query_result {
        Err(err) => Err(anyhow!(
            "error selecting table names from keyspace {}: {}",
            keyspace_opts.name,
            err.to_string(),
        )),
        Ok(query_result) => {
            let mut table_names: Vec<String> = Vec::new();
            if let Some(rows) = query_result.rows {
                for row_result in rows.into_typed::<(String,)>() {
                    match row_result {
                        Ok(row) => table_names.push(row.0),
                        Err(err) => {
                            return Err(anyhow!(
                            "error reading table name rows from query result for keyspace {}: {}",
                            keyspace_opts.name,
                            err.to_string()))
                        }
                    }
                }
            }
            Ok(table_names)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use scylla::Session;

    async fn cql_session() -> Session {
        let node_address = "127.0.0.1:9042";
        scylla::SessionBuilder::new()
            .known_node(node_address)
            .build()
            .await
            .unwrap()
    }

    fn alphanumeric_str(len: u8) -> String {
        let mut rng = rand::thread_rng();
        (0..len)
            .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
            .collect()
    }

    #[tokio::test]
    async fn test_create_keyspace() {
        let session = cql_session().await;
        let keyspace_name = format!("cquill_test_{}", alphanumeric_str(6));
        let keyspace_opts = KeyspaceOpts::simple(keyspace_name.clone(), 1);
        if create_keyspace(&session, &keyspace_opts).await.is_err() {
            panic!();
        }
        session
            .query(format!("drop keyspace {keyspace_name}"), ())
            .await
            .unwrap_or_else(|_| panic!("failed dropping keyspace {keyspace_name}"));
    }

    #[tokio::test]
    async fn test_create_keyspace_errors_keyspace_already_exists() {
        let session = cql_session().await;
        let keyspace_name = format!("cquill_test_{}", alphanumeric_str(6));
        let keyspace_opts = KeyspaceOpts::simple(keyspace_name.clone(), 1);
        if create_keyspace(&session, &keyspace_opts).await.is_err() {
            panic!();
        }
        if create_keyspace(&session, &keyspace_opts).await.is_ok() {
            panic!();
        }
        session
            .query(format!("drop keyspace {keyspace_name}"), ())
            .await
            .unwrap_or_else(|_| panic!("failed dropping keyspace {keyspace_name}"));
    }

    #[tokio::test]
    async fn test_create_history_table() {
        let session = cql_session().await;
        let keyspace_name = format!("cquill_test_{}", alphanumeric_str(6));
        let keyspace_opts = KeyspaceOpts::simple(keyspace_name.clone(), 1);
        if create_keyspace(&session, &keyspace_opts).await.is_err() {
            panic!();
        }
        if create_history_table(&session, &keyspace_opts, String::from("migrated_cql"))
            .await
            .is_err()
        {
            panic!();
        }
        session
            .query(format!("drop keyspace {keyspace_name}"), ())
            .await
            .unwrap_or_else(|_| panic!("failed dropping keyspace {keyspace_name}"));
    }

    #[tokio::test]
    async fn test_create_history_table_errors_table_already_exists() {
        let session = cql_session().await;
        let keyspace_name = format!("cquill_test_{}", alphanumeric_str(6));
        let keyspace_opts = KeyspaceOpts::simple(keyspace_name.clone(), 1);
        if create_keyspace(&session, &keyspace_opts).await.is_err() {
            panic!();
        }
        if create_history_table(&session, &keyspace_opts, String::from("migrated_cql"))
            .await
            .is_err()
        {
            panic!();
        }
        if create_history_table(&session, &keyspace_opts, String::from("migrated_cql"))
            .await
            .is_ok()
        {
            panic!();
        }
        session
            .query(format!("drop keyspace {keyspace_name}"), ())
            .await
            .unwrap_or_else(|_| panic!("failed dropping keyspace {keyspace_name}"));
    }

    #[tokio::test]
    async fn test_select_keyspace_table_names() {
        let session = cql_session().await;
        let keyspace_name = format!("cquill_test_{}", alphanumeric_str(6));
        let keyspace_opts = KeyspaceOpts::simple(keyspace_name.clone(), 1);
        create_keyspace(&session, &keyspace_opts).await.unwrap();
        let table_1 = String::from("project_1_cql");
        let table_2 = String::from("project_2_cql");
        create_history_table(&session, &keyspace_opts, table_1.clone())
            .await
            .unwrap();
        create_history_table(&session, &keyspace_opts, table_2.clone())
            .await
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1000));
        match select_keyspace_table_names(&session, &keyspace_opts).await {
            Ok(table_names) => {
                assert_eq!(table_names.len(), 2);
                assert!(table_names.contains(&table_1));
                assert!(table_names.contains(&table_2));
            }
            Err(err) => {
                println!("{err}",);
                panic!();
            }
        }
    }
}
