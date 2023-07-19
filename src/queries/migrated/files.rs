use std::path::Path;

use scylla::{IntoTypedRows, Session};
use uuid::Uuid;

use crate::cql::CqlFile;
use crate::queries::QueryError;

pub(crate) async fn insert(
    session: &Session,
    keyspace: &String,
    table: &String,
    cql_file: &CqlFile,
) -> Result<(), QueryError> {
    let cql =
        format!("insert into {keyspace}.{table} (id, ver, name, hash) values (now(), ?, ?, ?)");
    let values = (&cql_file.version, &cql_file.filename, &cql_file.hash);
    session.query(cql, values).await?;
    Ok(())
}

pub(crate) async fn select_all(
    session: &Session,
    keyspace: &String,
    table: &String,
    cql_dir: &Path,
) -> Result<Vec<CqlFile>, QueryError> {
    let cql = format!("select id, name, hash, ver from {keyspace}.{table}");
    let query_result = session.query(cql, ()).await?;
    let mut result = Vec::new();
    if let Some(rows) = query_result.rows {
        for row_result in rows.into_typed::<(Uuid, String, String, i16)>() {
            let row_values = row_result.unwrap();
            let filename = row_values.1;
            let hash = row_values.2;
            let path = cql_dir.join(&filename);
            let version = row_values.3;
            result.push(CqlFile {
                filename,
                hash,
                path,
                version,
            })
        }
    }
    result.sort_by(|a, b| a.version.cmp(&b.version));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use uuid::Uuid;

    use crate::test_utils;

    use super::*;

    #[tokio::test]
    async fn test_insert() {
        let harness = test_utils::TestHarness::builder().initialize().await;
        let cql_file = CqlFile {
            filename: "v073-more_tables.cql".to_string(),
            hash: "7f5b4bdccd3863f31be5c257ff497704".to_string(),
            path: PathBuf::from("v073-more_tables.cql"),
            version: 73,
        };

        insert(
            &harness.session,
            &harness.cquill_keyspace,
            &harness.cquill_table,
            &cql_file,
        )
        .await
        .unwrap();
        let select_cql = format!(
            "select id, ver, name, hash from {}.{}",
            harness.cquill_keyspace, harness.cquill_table
        );
        match harness.session.query(select_cql, ()).await {
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

        harness.drop_keyspace().await;
    }

    #[tokio::test]
    async fn test_select_all_when_empty() {
        let harness = test_utils::TestHarness::builder().initialize().await;

        let migrated_cql_files = select_all(
            &harness.session,
            &harness.cquill_keyspace,
            &harness.cquill_table,
            harness.cql_dir.as_path(),
        )
        .await
        .expect("select all migrated cql files");
        assert!(migrated_cql_files.is_empty());

        harness.drop_keyspace().await;
    }

    #[tokio::test]
    async fn test_select_all_returns_ordered() {
        let harness = test_utils::TestHarness::builder()
            .cql_file("v001-more_cql.cql", "abc")
            .cql_file("v002-more_cql.cql", "def")
            .cql_file("v003-more_cql.cql", "ghi")
            .initialize()
            .await;
        for i in [0, 2, 1] {
            let cql_file = harness.cql_files.get(i).unwrap();
            insert(
                &harness.session,
                &harness.cquill_keyspace,
                &harness.cquill_table,
                cql_file,
            )
            .await
            .expect("save migrated cql file history");
        }

        let migrated_cql_files = select_all(
            &harness.session,
            &harness.cquill_keyspace,
            &harness.cquill_table,
            harness.cql_dir.as_path(),
        )
        .await
        .expect("select all migrated cql files");
        assert_eq!(migrated_cql_files.len(), 3);
        let first = migrated_cql_files.get(0).unwrap();
        assert_eq!(first.filename, "v001-more_cql.cql");
        assert_eq!(first.version, 1);
        assert_eq!(first.hash, "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(first.path, harness.cql_file_path("v001-more_cql.cql"));
        let second = migrated_cql_files.get(1).unwrap();
        assert_eq!(second.filename, "v002-more_cql.cql");
        assert_eq!(second.version, 2);
        assert_eq!(second.hash, "4ed9407630eb1000c0f6b63842defa7d");
        assert_eq!(second.path, harness.cql_file_path("v002-more_cql.cql"));
        let third = migrated_cql_files.get(2).unwrap();
        assert_eq!(third.filename, "v003-more_cql.cql");
        assert_eq!(third.version, 3);
        assert_eq!(third.hash, "826bbc5d0522f5f20a1da4b60fa8c871");
        assert_eq!(third.path, harness.cql_file_path("v003-more_cql.cql"));

        harness.drop_keyspace().await;
    }
}
