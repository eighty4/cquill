use anyhow::Result;
use scylla::Session;

use crate::cql::CqlFile;

#[allow(dead_code)]
pub(crate) async fn insert(
    session: &Session,
    keyspace: &String,
    table: &String,
    cql_file: &CqlFile,
) -> Result<()> {
    let cql =
        format!("insert into {keyspace}.{table} (id, ver, name, hash) values (now(), ?, ?, ?)");
    let values = (&cql_file.version, &cql_file.filename, &cql_file.hash);
    session.query(cql, values).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use scylla::transport::session::IntoTypedRows;
    use uuid::Uuid;

    use crate::keyspace::KeyspaceOpts;
    use crate::queries::{migrated::table, *};

    use super::*;

    #[tokio::test]
    async fn test_insert() {
        let session = test_utils::cql_session().await;
        let keyspace_opts = KeyspaceOpts::simple(test_utils::keyspace_name(), 1);
        keyspace::create(&session, &keyspace_opts)
            .await
            .expect("create keyspace");
        let table_name = String::from("table_name");
        table::create(&session, &keyspace_opts.name, &table_name)
            .await
            .expect("create table");
        let cql_file = CqlFile {
            version: 73,
            hash: "7f5b4bdccd3863f31be5c257ff497704".to_string(),
            filename: "v073-more_tables.cql".to_string(),
        };

        insert(&session, &keyspace_opts.name, &table_name, &cql_file)
            .await
            .unwrap();
        let select_cql = format!(
            "select id, ver, name, hash from {}.{table_name}",
            keyspace_opts.name
        );
        match session.query(select_cql, ()).await {
            Err(err) => {
                println!("{err}");
                panic!();
            }
            Ok(query_result) => {
                let rows = query_result.rows.unwrap();
                assert_eq!(rows.len(), 1);
                for row_result in rows.into_typed::<(Uuid, i16, String, String)>() {
                    match row_result {
                        Err(err) => {
                            println!("{err}");
                            panic!();
                        }
                        Ok(row) => {
                            assert_eq!(row.0.get_version_num(), 1);
                            assert_eq!(row.1, 73);
                            assert_eq!(row.2, "v073-more_tables.cql");
                            assert_eq!(row.3, "7f5b4bdccd3863f31be5c257ff497704");
                        }
                    }
                }
            }
        }
    }
}
