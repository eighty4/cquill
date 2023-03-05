use super::*;
use scylla::transport::session::IntoTypedRows;

pub(crate) async fn create(session: &Session, keyspace_opts: &KeyspaceOpts) -> Result<()> {
    session
        .query(create_keyspace_cql(keyspace_opts)?, ())
        .await?;
    Ok(())
}

#[allow(dead_code)]
pub(crate) async fn drop(session: &Session, keyspace_opts: &KeyspaceOpts) -> Result<()> {
    session
        .query(format!("drop keyspace {}", keyspace_opts.name), ())
        .await?;
    Ok(())
}

pub(crate) async fn select_table_names(
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

    #[tokio::test]
    async fn test_create_keyspace() {
        let session = test_utils::cql_session().await;
        let keyspace_name = test_utils::keyspace_name();
        let keyspace_opts = KeyspaceOpts::simple(keyspace_name.clone(), 1);
        if create(&session, &keyspace_opts).await.is_err() {
            panic!();
        }
        session
            .query(format!("drop keyspace {keyspace_name}"), ())
            .await
            .unwrap_or_else(|_| panic!("failed dropping keyspace {keyspace_name}"));
    }

    #[tokio::test]
    async fn test_create_keyspace_errors_keyspace_already_exists() {
        let session = test_utils::cql_session().await;
        let keyspace_name = test_utils::keyspace_name();
        let keyspace_opts = KeyspaceOpts::simple(keyspace_name.clone(), 1);
        if create(&session, &keyspace_opts).await.is_err() {
            panic!();
        }
        if create(&session, &keyspace_opts).await.is_ok() {
            panic!();
        }
        session
            .query(format!("drop keyspace {keyspace_name}"), ())
            .await
            .unwrap_or_else(|_| panic!("failed dropping keyspace {keyspace_name}"));
    }

    #[tokio::test]
    async fn test_drop_keyspace() {
        let session = test_utils::cql_session().await;
        let keyspace_name = test_utils::keyspace_name();
        let keyspace_opts = KeyspaceOpts::simple(keyspace_name.clone(), 1);
        if create(&session, &keyspace_opts).await.is_err() {
            panic!();
        }
        if drop(&session, &keyspace_opts).await.is_err() {
            panic!();
        }
        if drop(&session, &keyspace_opts).await.is_ok() {
            panic!();
        }
    }

    #[tokio::test]
    async fn test_select_keyspace_table_names() {
        let session = test_utils::cql_session().await;
        let keyspace_name = test_utils::keyspace_name();
        let keyspace_opts = KeyspaceOpts::simple(keyspace_name.clone(), 1);
        create(&session, &keyspace_opts).await.unwrap();
        let table_1 = String::from("project_1_cql");
        let table_2 = String::from("project_2_cql");
        migrated::table::create(&session, &keyspace_opts, table_1.clone())
            .await
            .unwrap();
        migrated::table::create(&session, &keyspace_opts, table_2.clone())
            .await
            .unwrap();
        match select_table_names(&session, &keyspace_opts).await {
            Ok(table_names) => {
                assert_eq!(table_names.len(), 2);
                assert!(table_names.contains(&table_1));
                assert!(table_names.contains(&table_2));
            }
            Err(err) => {
                println!("{err}");
                panic!();
            }
        }
        session
            .query(format!("drop keyspace {keyspace_name}"), ())
            .await
            .unwrap_or_else(|_| panic!("failed dropping keyspace {keyspace_name}"));
    }
}
