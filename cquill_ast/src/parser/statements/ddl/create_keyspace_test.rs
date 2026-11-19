use std::collections::HashMap;

use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_create_keyspace_with_simple_replication() {
    let cql = CREATE_KEYSPACE_WITH_SIMPLE_REPLICATION;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Keyspace(
            CreateKeyspaceStatement {
                if_not_exists: false,
                keyspace_name: find_token(cql, "big_data_keyspace"),
                replication: KeyspaceReplication::Simple(1),
                durable_writes: None,
            }
        )))
    );
}

#[test]
fn test_create_keyspace_with_network_topology_replication() {
    let cql = CREATE_KEYSPACE_WITH_NETWORK_REPLICATION;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Keyspace(
            CreateKeyspaceStatement {
                if_not_exists: false,
                keyspace_name: find_token(cql, "big_data_keyspace"),
                replication: KeyspaceReplication::NetworkTopology(HashMap::from([
                    ("dc1".to_string(), 2),
                    ("dc2".to_string(), 2),
                ])),
                durable_writes: None,
            }
        )))
    );
}

#[test]
fn test_create_keyspace_if_not_exists() {
    let cql = CREATE_KEYSPACE_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Keyspace(
            CreateKeyspaceStatement {
                if_not_exists: true,
                keyspace_name: find_token(cql, "big_data_keyspace"),
                replication: KeyspaceReplication::Simple(1),
                durable_writes: None,
            }
        )))
    );
}

#[test]
fn test_create_keyspace_with_durable_writes_false() {
    let cql = CREATE_KEYSPACE_WITH_DURABLE_WRITES_FALSE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Keyspace(
            CreateKeyspaceStatement {
                if_not_exists: false,
                keyspace_name: find_token(cql, "big_data_keyspace"),
                replication: KeyspaceReplication::Simple(1),
                durable_writes: Some(false),
            }
        )))
    );
}

#[test]
fn test_create_keyspace_with_durable_writes_true() {
    let cql = CREATE_KEYSPACE_WITH_DURABLE_WRITES_TRUE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Keyspace(
            CreateKeyspaceStatement {
                if_not_exists: false,
                keyspace_name: find_token(cql, "big_data_keyspace"),
                replication: KeyspaceReplication::Simple(1),
                durable_writes: Some(true),
            }
        )))
    );
}
