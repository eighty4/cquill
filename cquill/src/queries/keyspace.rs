use std::collections::HashMap;

use anyhow::{Result, anyhow};

use crate::keyspace::ReplicationFactor::*;

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum CreateKeyspaceError {
    #[error(transparent)]
    CqlQueryError(#[from] QueryError),
    #[error(transparent)]
    InvalidKeyspace(#[from] anyhow::Error),
}

pub(crate) async fn create(
    session: &Session,
    keyspace_opts: &KeyspaceOpts,
) -> Result<(), CreateKeyspaceError> {
    let cql = create_keyspace_cql(keyspace_opts)?;
    session
        .query_unpaged(cql, ())
        .await
        .map_err(|err| QueryError::Execution(err.to_string()))?;
    Ok(())
}

#[allow(dead_code)]
pub(crate) async fn drop(session: &Session, keyspace_name: &String) -> Result<(), QueryError> {
    let cql = format!("drop keyspace {keyspace_name}");
    session
        .query_unpaged(cql, ())
        .await
        .map_err(|err| QueryError::Execution(err.to_string()))?;
    Ok(())
}

fn create_keyspace_cql(keyspace_opts: &KeyspaceOpts) -> Result<String, anyhow::Error> {
    if keyspace_opts.name.is_empty() {
        return Err(anyhow!("keyspace has empty name"));
    }
    let replication = match &keyspace_opts.replication {
        Some(r) => match r {
            NetworkTopologyStrategy { datacenter_factors } => {
                create_network_topology_strategy_keyspace_replication_map_str(datacenter_factors)
            }
            SimpleStrategy { factor } => {
                Ok(create_simple_strategy_keyspace_replication_map_str(factor))
            }
        },
        None => Ok(create_simple_strategy_keyspace_replication_map_str(&1)),
    };
    match replication {
        Ok(r) => Ok(format!(
            "create keyspace {} with replication = {}",
            keyspace_opts.name, r
        )),
        Err(e) => Err(anyhow!("keyspace {} {}", keyspace_opts.name, e,)),
    }
}

fn create_simple_strategy_keyspace_replication_map_str(replication_factor: &u8) -> String {
    format!("{{ 'class': 'SimpleStrategy', 'replication_factor': {replication_factor} }}")
}

fn create_network_topology_strategy_keyspace_replication_map_str(
    datacenter_factors: &HashMap<String, u8>,
) -> Result<String> {
    if datacenter_factors.is_empty() {
        return Err(anyhow!(
            "network topology replication has no datacenter replication factors"
        ));
    }
    let mut v = Vec::with_capacity(datacenter_factors.len());
    for (dc, rf) in datacenter_factors {
        if dc.is_empty() {
            return Err(anyhow!(
                "network topology replication factor has empty datacenter name"
            ));
        } else if *rf == 0 {
            return Err(anyhow!(
                "network topology replication factor for datacenter {} is not a positive number",
                dc
            ));
        }
        v.push(format!("'{dc}': {rf}"));
    }
    Ok(format!(
        "{{ 'class': 'NetworkTopologyStrategy', {} }}",
        v.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use crate::test_utils;

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
            .query_unpaged(format!("drop keyspace {keyspace_name}"), ())
            .await
            .unwrap_or_else(|_| panic!("failed dropping keyspace {keyspace_name}"));
    }

    #[tokio::test]
    async fn test_create_keyspace_errors_keyspace_already_exists() {
        let session = test_utils::cql_session().await;
        let keyspace_opts = test_utils::create_keyspace(&session).await;

        if create(&session, &keyspace_opts).await.is_ok() {
            panic!();
        }
        session
            .query_unpaged(format!("drop keyspace {}", keyspace_opts.name), ())
            .await
            .unwrap_or_else(|_| panic!("failed dropping keyspace {}", keyspace_opts.name));
    }

    #[tokio::test]
    async fn test_drop_keyspace() {
        let session = test_utils::cql_session().await;
        let keyspace_opts = test_utils::create_keyspace(&session).await;

        if drop(&session, &keyspace_opts.name).await.is_err() {
            panic!();
        }
        if drop(&session, &keyspace_opts.name).await.is_ok() {
            panic!();
        }
    }

    #[test]
    fn test_create_keyspace_cql_errors_with_empty_keyspace_name() {
        let opts = KeyspaceOpts {
            name: "".to_string(),
            replication: None,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "keyspace has empty name".to_string()
        );
    }

    #[test]
    fn test_create_keyspace_cql_with_default_replication() {
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication: None,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "create keyspace cquill_migration with replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }".to_string());
    }

    #[test]
    fn test_create_keyspace_cql_with_simple_strategy_replication() {
        let replication = Some(SimpleStrategy { factor: 3 });
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "create keyspace cquill_migration with replication = { 'class': 'SimpleStrategy', 'replication_factor': 3 }".to_string());
    }

    #[test]
    fn test_create_keyspace_cql_with_single_network_topology_replication_factor() {
        let datacenter_factors = HashMap::from([(String::from("dc1"), 7)]);
        let replication = Some(NetworkTopologyStrategy { datacenter_factors });
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "create keyspace cquill_migration with replication = { 'class': 'NetworkTopologyStrategy', 'dc1': 7 }".to_string());
    }

    #[test]
    fn test_create_keyspace_cql_with_multiple_network_topology_replication_factors() {
        let datacenter_factors =
            HashMap::from([(String::from("dc1"), 7), (String::from("dc2"), 2)]);
        let replication = Some(NetworkTopologyStrategy { datacenter_factors });
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_ok());
        let cql = result.unwrap();
        let expect_begin = "create keyspace cquill_migration with replication = {";
        assert!(cql.starts_with(expect_begin));
        assert!(cql.ends_with('}'));
        let mut split: Vec<&str> = cql[expect_begin.len()..cql.len() - 1].split(',').collect();
        split.sort();
        assert_eq!(split.len(), 3);
        assert_eq!(split[0].trim(), "'class': 'NetworkTopologyStrategy'");
        assert_eq!(split[1].trim(), "'dc1': 7");
        assert_eq!(split[2].trim(), "'dc2': 2");
    }

    #[test]
    fn test_create_keyspace_cql_errors_without_datacenter_factors() {
        let datacenter_factors = HashMap::new();
        let replication = Some(NetworkTopologyStrategy { datacenter_factors });
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "keyspace cquill_migration network topology replication has no datacenter replication factors"
        );
    }

    #[test]
    fn test_create_keyspace_cql_errors_without_datacenter_name() {
        let datacenter_factors = HashMap::from([(String::from(""), 7)]);
        let replication = Some(NetworkTopologyStrategy { datacenter_factors });
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "keyspace cquill_migration network topology replication factor has empty datacenter name"
        );
    }

    #[test]
    fn test_create_keyspace_cql_errors_with_zero_replication_factor() {
        let datacenter_factors = HashMap::from([(String::from("dc1"), 0)]);
        let replication = Some(NetworkTopologyStrategy { datacenter_factors });
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "keyspace cquill_migration network topology replication factor for datacenter dc1 is not a positive number"
        );
    }
}
