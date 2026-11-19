use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_parsing_drop_aggregate_default_keyspace() {
    assert_eq!(
        parse_cql(DROP_AGGREGATE_WITHOUT_ARGS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Aggregate(
            DropAggregateStatement {
                aggregate_name: find_token(DROP_AGGREGATE_WITHOUT_ARGS, "big_data_agg"),
                if_exists: false,
                keyspace_name: None,
                signature: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_aggregate_default_keyspace_single_arg() {
    let cql = DROP_AGGREGATE_WITH_SINGLE_ARG;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Aggregate(
            DropAggregateStatement {
                aggregate_name: find_token(cql, "big_data_agg"),
                if_exists: false,
                keyspace_name: None,
                signature: Some(vec!(CqlDataType::ValueType(CqlValueType::NativeType(
                    CqlNativeType::Int
                )))),
            }
        )))
    );
}

#[test]
fn test_parsing_drop_aggregate_explicit_keyspace() {
    assert_eq!(
        parse_cql(DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Aggregate(
            DropAggregateStatement {
                aggregate_name: find_token(DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE, "big_data_agg"),
                if_exists: false,
                keyspace_name: Some(find_token(
                    DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE,
                    "big_data_keyspace"
                )),
                signature: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_aggregate_explicit_keyspace_multiple_args() {
    let cql = DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE_AND_MULTIPLE_ARGS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Aggregate(
            DropAggregateStatement {
                aggregate_name: find_token(cql, "big_data_agg"),
                if_exists: false,
                keyspace_name: Some(find_token(cql, "big_data_keyspace")),
                signature: Some(vec!(
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                )),
            }
        )))
    );
}
