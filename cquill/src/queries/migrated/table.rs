use super::*;

pub(crate) async fn create(
    session: &Session,
    keyspace_name: &String,
    table_name: &String,
) -> Result<(), QueryError> {
    let cql = format!(
        "create table {keyspace_name}.{table_name} (id timeuuid primary key, ver smallint, name varchar, hash varchar)"
    );
    session.query(cql, ()).await?;
    Ok(())
}

#[allow(dead_code)]
pub(crate) async fn drop(
    session: &Session,
    keyspace_name: &String,
    table_name: &String,
) -> Result<(), QueryError> {
    session
        .query(format!("drop table {keyspace_name}.{table_name}"), ())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::test_utils;

    use super::*;

    #[tokio::test]
    async fn test_create_table() {
        let session = test_utils::cql_session().await;
        let keyspace_opts = test_utils::create_keyspace(&session).await;

        create(&session, &keyspace_opts.name, &String::from("migrated_cql"))
            .await
            .expect("creating table");

        keyspace::drop(&session, &keyspace_opts.name)
            .await
            .expect("drop keyspace");
    }

    #[tokio::test]
    async fn test_drop_table() {
        let harness = test_utils::TestHarness::builder().initialize().await;

        drop(
            &harness.session,
            &harness.cquill_keyspace,
            &harness.cquill_table,
        )
        .await
        .expect("drop table");
        drop(
            &harness.session,
            &harness.cquill_keyspace,
            &harness.cquill_table,
        )
        .await
        .expect_err("drop table");

        harness.drop_keyspace().await;
    }

    #[tokio::test]
    async fn test_create_table_errors_table_already_exists() {
        let session = test_utils::cql_session().await;
        let keyspace_opts = test_utils::create_keyspace(&session).await;
        let table_name = String::from("migrated_cql");
        create(&session, &keyspace_opts.name, &table_name)
            .await
            .expect("creating table");

        create(&session, &keyspace_opts.name, &table_name)
            .await
            .expect_err("creating table");

        keyspace::drop(&session, &keyspace_opts.name)
            .await
            .expect("drop keyspace");
    }
}
