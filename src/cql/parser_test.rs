use crate::cql::ast::*;
use crate::cql::lex::TokenRange;
use crate::cql::parser::parse_cql;
use crate::cql::test_cql::*;
use std::sync::Arc;

fn find_token(cql: &str, s: &str) -> TokenView {
    let b = cql.find(s).unwrap();
    let e = b + s.len() - 1;
    let range = TokenRange::new(b, e);
    TokenView {
        cql: Arc::new(String::from(cql)),
        range,
    }
}

#[test]
fn test_token_view() {
    let ast = parse_cql(DROP_KEYSPACE.to_string()).unwrap();
    assert_eq!(
        match ast.first() {
            Some(CqlStatement::Drop(DropStatement::Keyspace(dks))) => dks.keyspace_name.value(),
            _ => panic!(),
        },
        "big_data_keyspace".to_string()
    );
}

#[test]
fn test_parsing_drop_aggregate_without_keyspace() {
    assert_eq!(
        parse_cql(DROP_AGGREGATE_WITHOUT_ARGS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Aggregate(
            DropAggregateStatement {
                aggregate_name: find_token(DROP_AGGREGATE_WITHOUT_ARGS, "big_data_agg"),
                if_exists: false,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_aggregate_with_keyspace() {
    assert_eq!(
        parse_cql(DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Aggregate(
            DropAggregateStatement {
                aggregate_name: find_token(DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE, "big_data_agg"),
                if_exists: false,
                keyspace_name: Some(find_token(
                    DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE,
                    "big_data_keyspace"
                ))
            }
        )))
    );
}

#[test]
fn test_parsing_drop_function_without_keyspace() {
    assert_eq!(
        parse_cql(DROP_FUNCTION_WITHOUT_ARGS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Function(
            DropFunctionStatement {
                function_name: find_token(DROP_FUNCTION_WITHOUT_ARGS, "big_data_fn"),
                if_exists: false,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_function_with_keyspace() {
    assert_eq!(
        parse_cql(DROP_FUNCTION_WITH_EXPLICIT_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Function(
            DropFunctionStatement {
                function_name: find_token(DROP_FUNCTION_WITH_EXPLICIT_KEYSPACE, "big_data_fn"),
                if_exists: false,
                keyspace_name: Some(find_token(
                    DROP_FUNCTION_WITH_EXPLICIT_KEYSPACE,
                    "big_data_keyspace"
                ))
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

#[test]
fn test_parsing_drop_keyspace() {
    assert_eq!(
        parse_cql(DROP_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Keyspace(
            DropKeyspaceStatement {
                keyspace_name: find_token(DROP_KEYSPACE, "big_data_keyspace"),
                if_exists: false,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_if_exists_keyspace() {
    assert_eq!(
        parse_cql(DROP_KEYSPACE_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Keyspace(
            DropKeyspaceStatement {
                keyspace_name: find_token(DROP_KEYSPACE_IF_EXISTS, "big_data_keyspace"),
                if_exists: true,
            }
        )))
    );
}
