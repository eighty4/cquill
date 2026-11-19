use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_parsing_drop_table_with_default_keyspace() {
    assert_eq!(
        parse_cql(DROP_TABLE_DEFAULT_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Table(
            DropTableStatement {
                alias: None,
                table_name: find_token(DROP_TABLE_DEFAULT_KEYSPACE, "big_data_table"),
                if_exists: false,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_table_with_default_keyspace_if_exists() {
    assert_eq!(
        parse_cql(DROP_TABLE_DEFAULT_KEYSPACE_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Table(
            DropTableStatement {
                alias: None,
                table_name: find_token(DROP_TABLE_DEFAULT_KEYSPACE_IF_EXISTS, "big_data_table"),
                if_exists: true,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_table_with_explicit_keyspace() {
    assert_eq!(
        parse_cql(DROP_TABLE_EXPLICIT_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Table(
            DropTableStatement {
                alias: None,
                table_name: find_token(DROP_TABLE_EXPLICIT_KEYSPACE, "big_data_table"),
                if_exists: false,
                keyspace_name: Some(find_token(
                    DROP_TABLE_EXPLICIT_KEYSPACE,
                    "big_data_keyspace"
                ))
            }
        )))
    );
}

#[test]
fn test_parsing_drop_table_with_explicit_keyspace_if_exists() {
    assert_eq!(
        parse_cql(DROP_TABLE_EXPLICIT_KEYSPACE_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Table(
            DropTableStatement {
                alias: None,
                table_name: find_token(DROP_TABLE_EXPLICIT_KEYSPACE_IF_EXISTS, "big_data_table"),
                if_exists: true,
                keyspace_name: Some(find_token(
                    DROP_TABLE_EXPLICIT_KEYSPACE_IF_EXISTS,
                    "big_data_keyspace"
                ))
            }
        )))
    );
}
