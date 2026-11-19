use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_create_index() {
    let cql = CREATE_INDEX;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Index(
            CreateIndexStatement {
                index_name: Some(find_token(cql, "big_data_index")),
                if_not_exists: false,
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: None,
                on_column: CreateIndexColumn::Column(find_token(cql, "text_column"))
            }
        )))
    );
}

#[test]
fn test_create_index_without_name() {
    let cql = CREATE_INDEX_WITHOUT_NAME;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Index(
            CreateIndexStatement {
                index_name: None,
                if_not_exists: false,
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: None,
                on_column: CreateIndexColumn::Column(find_token(cql, "text_column"))
            }
        )))
    );
}

#[test]
fn test_create_index_if_not_exists() {
    let cql = CREATE_INDEX_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Index(
            CreateIndexStatement {
                index_name: Some(find_token(cql, "big_data_index")),
                if_not_exists: true,
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: None,
                on_column: CreateIndexColumn::Column(find_token(cql, "text_column"))
            }
        )))
    );
}

#[test]
fn test_create_index_on_keys() {
    let cql = CREATE_INDEX_ON_KEYS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Index(
            CreateIndexStatement {
                index_name: Some(find_token(cql, "big_data_index")),
                if_not_exists: false,
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: None,
                on_column: CreateIndexColumn::MapKeys(find_token(cql, "map_column"))
            }
        )))
    );
}

#[test]
fn test_create_index_on_full() {
    let cql = CREATE_INDEX_ON_FULL;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Index(
            CreateIndexStatement {
                index_name: Some(find_token(cql, "big_data_index")),
                if_not_exists: false,
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: None,
                on_column: CreateIndexColumn::FullCollection(find_token(cql, "map_column"))
            }
        )))
    );
}

#[test]
fn test_create_index_on_entries() {
    let cql = CREATE_INDEX_ON_ENTRIES;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Index(
            CreateIndexStatement {
                index_name: Some(find_token(cql, "big_data_index")),
                if_not_exists: false,
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: None,
                on_column: CreateIndexColumn::MapEntries(find_token(cql, "map_column"))
            }
        )))
    );
}

#[test]
fn test_create_index_on_values() {
    let cql = CREATE_INDEX_ON_VALUES;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Index(
            CreateIndexStatement {
                index_name: Some(find_token(cql, "big_data_index")),
                if_not_exists: false,
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: None,
                on_column: CreateIndexColumn::MapValues(find_token(cql, "map_column"))
            }
        )))
    );
}

#[test]
fn test_parsing_drop_index_with_default_keyspace() {
    assert_eq!(
        parse_cql(DROP_INDEX_DEFAULT_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Index(
            DropIndexStatement {
                index_name: find_token(DROP_INDEX_DEFAULT_KEYSPACE, "big_data_index"),
                if_exists: false,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_index_with_default_keyspace_if_exists() {
    assert_eq!(
        parse_cql(DROP_INDEX_DEFAULT_KEYSPACE_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Index(
            DropIndexStatement {
                index_name: find_token(DROP_INDEX_DEFAULT_KEYSPACE_IF_EXISTS, "big_data_index"),
                if_exists: true,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_index_with_explicit_keyspace() {
    assert_eq!(
        parse_cql(DROP_INDEX_EXPLICIT_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Index(
            DropIndexStatement {
                index_name: find_token(DROP_INDEX_EXPLICIT_KEYSPACE, "big_data_index"),
                if_exists: false,
                keyspace_name: Some(find_token(
                    DROP_INDEX_EXPLICIT_KEYSPACE,
                    "big_data_keyspace"
                ))
            }
        )))
    );
}

#[test]
fn test_parsing_drop_index_with_explicit_keyspace_if_exists() {
    assert_eq!(
        parse_cql(DROP_INDEX_EXPLICIT_KEYSPACE_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Index(
            DropIndexStatement {
                index_name: find_token(DROP_INDEX_EXPLICIT_KEYSPACE_IF_EXISTS, "big_data_index"),
                if_exists: true,
                keyspace_name: Some(find_token(
                    DROP_INDEX_EXPLICIT_KEYSPACE_IF_EXISTS,
                    "big_data_keyspace"
                ))
            }
        )))
    );
}
