use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_parsing_create_trigger_in_default_keyspace() {
    let cql = CREATE_TRIGGER_DEFAULT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Trigger(
            CreateTriggerStatement {
                trigger_name: find_token(cql, "big_data_trigger"),
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: None,
                if_not_exists: false,
                index_classpath: find_string_literal(cql, "'trigger name'"),
            }
        )))
    );
}

#[test]
fn test_parsing_create_trigger_in_default_keyspace_if_not_exists() {
    let cql = CREATE_TRIGGER_IF_NOT_EXISTS_DEFAULT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Trigger(
            CreateTriggerStatement {
                trigger_name: find_token(cql, "big_data_trigger"),
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: None,
                if_not_exists: true,
                index_classpath: find_string_literal(cql, "'trigger name'"),
            }
        )))
    );
}

#[test]
fn test_parsing_create_trigger_in_explicit_keyspace() {
    let cql = CREATE_TRIGGER_EXPLICIT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Trigger(
            CreateTriggerStatement {
                trigger_name: find_token(cql, "big_data_trigger"),
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: Some(find_token(cql, "big_data_keyspace")),
                if_not_exists: false,
                index_classpath: find_string_literal(cql, "'trigger name'"),
            }
        )))
    );
}

#[test]
fn test_parsing_create_trigger_in_explicit_keyspace_if_not_exists() {
    let cql = CREATE_TRIGGER_IF_NOT_EXISTS_EXPLICIT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Trigger(
            CreateTriggerStatement {
                trigger_name: find_token(cql, "big_data_trigger"),
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: Some(find_token(cql, "big_data_keyspace")),
                if_not_exists: true,
                index_classpath: find_string_literal(cql, "'trigger name'"),
            }
        )))
    );
}

#[test]
fn test_parsing_drop_trigger_default_keyspace() {
    let cql = DROP_TRIGGER_DEFAULT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Trigger(
            DropTriggerStatement {
                table_name: find_token(cql, "big_data_table"),
                trigger_name: find_token(cql, "big_data_trigger"),
                if_exists: false,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_trigger_default_keyspace_if_exists() {
    let cql = DROP_TRIGGER_DEFAULT_KEYSPACE_IF_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Trigger(
            DropTriggerStatement {
                table_name: find_token(cql, "big_data_table"),
                trigger_name: find_token(cql, "big_data_trigger"),
                if_exists: true,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_trigger_explicit_keyspace() {
    let cql = DROP_TRIGGER_EXPLICIT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Trigger(
            DropTriggerStatement {
                table_name: find_token(cql, "big_data_table"),
                trigger_name: find_token(cql, "big_data_trigger"),
                if_exists: false,
                keyspace_name: Some(find_token(cql, "big_data_keyspace"))
            }
        )))
    );
}

#[test]
fn test_parsing_drop_trigger_explicit_keyspace_if_exists() {
    let cql = DROP_TRIGGER_EXPLICIT_KEYSPACE_IF_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Trigger(
            DropTriggerStatement {
                table_name: find_token(cql, "big_data_table"),
                trigger_name: find_token(cql, "big_data_trigger"),
                if_exists: true,
                keyspace_name: Some(find_token(cql, "big_data_keyspace"))
            }
        )))
    );
}
