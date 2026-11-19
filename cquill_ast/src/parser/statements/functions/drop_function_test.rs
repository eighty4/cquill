use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_parsing_drop_function_without_keyspace() {
    assert_eq!(
        parse_cql(DROP_FUNCTION_WITHOUT_ARGS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Function(
            DropFunctionStatement {
                function_name: find_token(DROP_FUNCTION_WITHOUT_ARGS, "big_data_fn"),
                if_exists: false,
                keyspace_name: None,
                signature: None,
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
                )),
                signature: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_function_explicit_keyspace_multiple_args() {
    let cql = DROP_FUNCTION_WITH_EXPLICIT_KEYSPACE_AND_MULTIPLE_ARGS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Function(
            DropFunctionStatement {
                function_name: find_token(cql, "big_data_fn"),
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
