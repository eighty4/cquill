use super::*;

pub(crate) async fn create(
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

#[allow(dead_code)]
pub(crate) async fn drop(
    session: &Session,
    keyspace_opts: &KeyspaceOpts,
    table_name: String,
) -> Result<()> {
    session
        .query(
            format!("drop table {}.{}", keyspace_opts.name, table_name),
            (),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_table() {
        let session = test_utils::cql_session().await;
        let keyspace_name = test_utils::keyspace_name();
        let keyspace_opts = KeyspaceOpts::simple(keyspace_name.clone(), 1);
        if keyspace::create(&session, &keyspace_opts).await.is_err() {
            panic!();
        }
        if create(&session, &keyspace_opts, String::from("migrated_cql"))
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
    async fn test_drop_table() {
        let session = test_utils::cql_session().await;
        let keyspace_name = test_utils::keyspace_name();
        let keyspace_opts = KeyspaceOpts::simple(keyspace_name.clone(), 1);
        if keyspace::create(&session, &keyspace_opts).await.is_err() {
            panic!();
        }
        let table_name = String::from("migrated_cql");
        if create(&session, &keyspace_opts, table_name.clone())
            .await
            .is_err()
        {
            panic!();
        }
        if drop(&session, &keyspace_opts, table_name.clone())
            .await
            .is_err()
        {
            panic!();
        }
        if drop(&session, &keyspace_opts, table_name).await.is_ok() {
            panic!();
        }
        session
            .query(format!("drop keyspace {keyspace_name}"), ())
            .await
            .unwrap_or_else(|_| panic!("failed dropping keyspace {keyspace_name}"));
    }

    #[tokio::test]
    async fn test_create_table_errors_table_already_exists() {
        let session = test_utils::cql_session().await;
        let keyspace_name = test_utils::keyspace_name();
        let keyspace_opts = KeyspaceOpts::simple(keyspace_name.clone(), 1);
        if keyspace::create(&session, &keyspace_opts).await.is_err() {
            panic!();
        }
        if create(&session, &keyspace_opts, String::from("migrated_cql"))
            .await
            .is_err()
        {
            panic!();
        }
        if create(&session, &keyspace_opts, String::from("migrated_cql"))
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
}
