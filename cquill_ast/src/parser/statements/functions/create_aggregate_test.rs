use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_create_aggregate_with_collection_stype() {
    let cql = CREATE_AGGREGATE_WITH_COLLECTION_STYPE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Aggregate(
            CreateAggregateStatement {
                if_exists_behavior: CreateIfExistsBehavior::Error,
                function_name: find_token(cql, "big_data_agg"),
                function_arg: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                state_function: find_token(cql, "fn_name"),
                state_type: CqlDataType::CollectionType(CqlCollectionType::List(
                    CqlValueType::NativeType(CqlNativeType::Text)
                )),
                final_function: None,
                init_condition: false,
            }
        )))
    );
}

#[test]
fn test_create_aggregate_with_udt_stype() {
    let cql = CREATE_AGGREGATE_WITH_UDT_STYPE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Aggregate(
            CreateAggregateStatement {
                if_exists_behavior: CreateIfExistsBehavior::Error,
                function_name: find_token(cql, "big_data_agg"),
                function_arg: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                state_function: find_token(cql, "fn_name"),
                state_type: CqlDataType::ValueType(CqlValueType::UserDefinedType(find_token(
                    cql, "some_udt"
                ))),
                final_function: None,
                init_condition: false,
            }
        )))
    );
}

#[test]
fn test_create_aggregate_or_replace() {
    let cql = CREATE_OR_REPLACE_AGGREGATE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Aggregate(
            CreateAggregateStatement {
                if_exists_behavior: CreateIfExistsBehavior::Replace,
                function_name: find_token(cql, "big_data_agg"),
                function_arg: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                state_function: find_token(cql, "fn_name"),
                state_type: CqlDataType::CollectionType(CqlCollectionType::List(
                    CqlValueType::NativeType(CqlNativeType::Text)
                )),
                final_function: None,
                init_condition: false,
            }
        )))
    );
}

#[test]
fn test_create_aggregate_if_not_exists() {
    let cql = CREATE_AGGREGATE_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Aggregate(
            CreateAggregateStatement {
                if_exists_behavior: CreateIfExistsBehavior::DoNotError,
                function_name: find_token(cql, "big_data_agg"),
                function_arg: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                state_function: find_token(cql, "fn_name"),
                state_type: CqlDataType::CollectionType(CqlCollectionType::List(
                    CqlValueType::NativeType(CqlNativeType::Text)
                )),
                final_function: None,
                init_condition: false,
            }
        )))
    );
}

#[test]
fn test_create_aggregate_with_final_function() {
    let cql = CREATE_AGGREGATE_WITH_FINALFUNC;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Aggregate(
            CreateAggregateStatement {
                if_exists_behavior: CreateIfExistsBehavior::Error,
                function_name: find_token(cql, "big_data_agg"),
                function_arg: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                state_function: find_token(cql, "fn_name"),
                state_type: CqlDataType::CollectionType(CqlCollectionType::List(
                    CqlValueType::NativeType(CqlNativeType::Text)
                )),
                final_function: Some(find_token(cql, "ffn_name")),
                init_condition: false,
            }
        )))
    );
}

#[test]
fn test_create_aggregate_with_init_condition() {
    let cql = CREATE_AGGREGATE_WITH_INITCOND;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Aggregate(
            CreateAggregateStatement {
                if_exists_behavior: CreateIfExistsBehavior::Error,
                function_name: find_token(cql, "big_data_agg"),
                function_arg: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                state_function: find_token(cql, "fn_name"),
                state_type: CqlDataType::CollectionType(CqlCollectionType::List(
                    CqlValueType::NativeType(CqlNativeType::Text)
                )),
                final_function: None,
                init_condition: true,
            }
        )))
    );
}

#[test]
fn test_create_aggregate_with_final_function_and_init_condition() {
    let cql = CREATE_AGGREGATE_WITH_FINALFUNC_AND_INITCOND;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Aggregate(
            CreateAggregateStatement {
                if_exists_behavior: CreateIfExistsBehavior::Error,
                function_name: find_token(cql, "big_data_agg"),
                function_arg: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                state_function: find_token(cql, "fn_name"),
                state_type: CqlDataType::CollectionType(CqlCollectionType::List(
                    CqlValueType::NativeType(CqlNativeType::Text)
                )),
                final_function: Some(find_token(cql, "ffn_name")),
                init_condition: true,
            }
        )))
    );
}
