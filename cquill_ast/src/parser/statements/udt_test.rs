use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_parsing_create_type_with_default_keyspace_and_single_field() {
    let cql = CREATE_DEFAULT_KEYSPACE_UDT_WITH_SINGLE_ATTRIBUTE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Type(
            CreateTypeStatement {
                type_name: find_token(cql, "big_data_udt"),
                if_not_exists: false,
                keyspace_name: None,
                fields: vec![(
                    find_token(cql, "int_attribute"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )],
            }
        )))
    );
}

#[test]
fn test_parsing_create_type_with_default_keyspace_and_single_field_if_not_exists() {
    let cql = CREATE_DEFAULT_KEYSPACE_UDT_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Type(
            CreateTypeStatement {
                type_name: find_token(cql, "big_data_udt"),
                if_not_exists: true,
                keyspace_name: None,
                fields: vec![(
                    find_token(cql, "int_attr"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )],
            }
        )))
    );
}

#[test]
fn test_parsing_create_type_with_default_keyspace_and_multiple_fields() {
    let cql = CREATE_DEFAULT_KEYSPACE_UDT_WITH_MULTIPLE_ATTRIBUTES;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Type(
            CreateTypeStatement {
                type_name: find_token(cql, "big_data_udt"),
                if_not_exists: false,
                keyspace_name: None,
                fields: vec![
                    (
                        find_token(cql, "int_attr"),
                        CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                    ),
                    (
                        find_token(cql, "text_attr"),
                        CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                    )
                ],
            }
        )))
    );
}

#[test]
fn test_parsing_create_type_with_explicit_keyspace_and_single_field() {
    let cql = CREATE_EXPLICIT_KEYSPACE_UDT_WITH_SINGLE_ATTRIBUTE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Type(
            CreateTypeStatement {
                type_name: find_token(cql, "big_data_udt"),
                if_not_exists: false,
                keyspace_name: Some(find_token(cql, "big_data_keyspace")),
                fields: vec![(
                    find_token(cql, "int_attribute"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )],
            }
        )))
    );
}

#[test]
fn test_parsing_create_type_with_explicit_keyspace_and_single_field_if_not_exists() {
    let cql = CREATE_EXPLICIT_KEYSPACE_UDT_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Type(
            CreateTypeStatement {
                type_name: find_token(cql, "big_data_udt"),
                if_not_exists: true,
                keyspace_name: Some(find_token(cql, "big_data_keyspace")),
                fields: vec![(
                    find_token(cql, "int_attr"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )],
            }
        )))
    );
}

#[test]
fn test_parsing_create_type_with_explicit_keyspace_and_multiple_fields() {
    let cql = CREATE_EXPLICIT_KEYSPACE_UDT_WITH_MULTIPLE_ATTRIBUTES;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Type(
            CreateTypeStatement {
                type_name: find_token(cql, "big_data_udt"),
                if_not_exists: false,
                keyspace_name: Some(find_token(cql, "big_data_keyspace")),
                fields: vec![
                    (
                        find_token(cql, "int_attr"),
                        CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                    ),
                    (
                        find_token(cql, "text_attr"),
                        CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                    )
                ],
            }
        )))
    );
}

#[test]
fn test_parsing_drop_type_default_keyspace() {
    let cql = DROP_UDT_DEFAULT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Type(DropTypeStatement {
            type_name: find_token(cql, "big_data_udt"),
            if_exists: false,
            keyspace_name: None,
        })))
    );
}

#[test]
fn test_parsing_drop_type_default_keyspace_if_exists() {
    let cql = DROP_UDT_DEFAULT_KEYSPACE_IF_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Type(DropTypeStatement {
            type_name: find_token(cql, "big_data_udt"),
            if_exists: true,
            keyspace_name: None,
        })))
    );
}

#[test]
fn test_parsing_drop_type_explicit_keyspace() {
    let cql = DROP_UDT_EXPLICIT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Type(DropTypeStatement {
            type_name: find_token(cql, "big_data_udt"),
            if_exists: false,
            keyspace_name: Some(find_token(cql, "big_data_keyspace"))
        })))
    );
}

#[test]
fn test_parsing_drop_type_explicit_keyspace_if_exists() {
    let cql = DROP_UDT_EXPLICIT_KEYSPACE_IF_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Type(DropTypeStatement {
            type_name: find_token(cql, "big_data_udt"),
            if_exists: true,
            keyspace_name: Some(find_token(cql, "big_data_keyspace"))
        })))
    );
}
