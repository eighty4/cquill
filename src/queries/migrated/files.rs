use anyhow::Result;
use scylla::{IntoTypedRows, Session};
use uuid::Uuid;

use crate::cql::CqlFile;

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

#[allow(dead_code)]
pub(crate) async fn select_all(
    session: &Session,
    keyspace: &String,
    table: &String,
) -> Result<Vec<CqlFile>> {
    let cql = format!("select id, name, hash, ver from {keyspace}.{table}");
    let query_result = session.query(cql, ()).await?;
    let mut result = Vec::new();
    if let Some(rows) = query_result.rows {
        for row_result in rows.into_typed::<(Uuid, String, String, i16)>() {
            let row_values = row_result.unwrap();
            let filename = row_values.1;
            let hash = row_values.2;
            let version = row_values.3;
            result.push(CqlFile {
                filename,
                version,
                hash,
            })
        }
    }
    result.sort_by(|a, b| a.version.cmp(&b.version));
    Ok(result)
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

    #[tokio::test]
    async fn test_select_all_when_empty() {
        let session = test_utils::cql_session().await;
        let keyspace_opts = KeyspaceOpts::simple(test_utils::keyspace_name(), 1);
        keyspace::create(&session, &keyspace_opts)
            .await
            .expect("create keyspace");
        let table_name = String::from("table_name");
        table::create(&session, &keyspace_opts.name, &table_name)
            .await
            .expect("create table");

        let migrated_cql_files = select_all(&session, &keyspace_opts.name, &table_name)
            .await
            .expect("select all migrated cql files");
        assert!(migrated_cql_files.is_empty());
    }

    #[tokio::test]
    async fn test_select_all_returns_ordered() {
        let session = test_utils::cql_session().await;
        let keyspace_opts = KeyspaceOpts::simple(test_utils::keyspace_name(), 1);
        keyspace::create(&session, &keyspace_opts)
            .await
            .expect("create keyspace");
        let table_name = String::from("table_name");
        table::create(&session, &keyspace_opts.name, &table_name)
            .await
            .expect("create table");
        insert(
            &session,
            &keyspace_opts.name,
            &table_name,
            &CqlFile {
                version: 1,
                hash: "7f5b4bdccd3863f31be5c257ff497704".to_string(),
                filename: "v001-more_cql.cql".to_string(),
            },
        )
        .await
        .unwrap();
        insert(
            &session,
            &keyspace_opts.name,
            &table_name,
            &CqlFile {
                version: 2,
                hash: "8f5b4bdccd3863f31be5c257ff497704".to_string(),
                filename: "v002-more_cql.cql".to_string(),
            },
        )
        .await
        .unwrap();
        insert(
            &session,
            &keyspace_opts.name,
            &table_name,
            &CqlFile {
                version: 3,
                hash: "9f5b4bdccd3863f31be5c257ff497704".to_string(),
                filename: "v003-more_cql.cql".to_string(),
            },
        )
        .await
        .unwrap();

        let migrated_cql_files = select_all(&session, &keyspace_opts.name, &table_name)
            .await
            .expect("select all migrated cql files");
        assert_eq!(migrated_cql_files.len(), 3);
        let first = migrated_cql_files.get(0).unwrap();
        assert_eq!(first.filename, "v001-more_cql.cql");
        assert_eq!(first.version, 1);
        assert_eq!(first.hash, "7f5b4bdccd3863f31be5c257ff497704");
        let second = migrated_cql_files.get(1).unwrap();
        assert_eq!(second.filename, "v002-more_cql.cql");
        assert_eq!(second.version, 2);
        assert_eq!(second.hash, "8f5b4bdccd3863f31be5c257ff497704");
        let third = migrated_cql_files.get(2).unwrap();
        assert_eq!(third.filename, "v003-more_cql.cql");
        assert_eq!(third.version, 3);
        assert_eq!(third.hash, "9f5b4bdccd3863f31be5c257ff497704");
    }
}
