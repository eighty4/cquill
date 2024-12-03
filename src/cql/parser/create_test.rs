use crate::cql::ast::{
    CqlStatement, CreateStatement, CreateTypeStatement, CreateUserPassword, CreateUserStatement,
    CreateUserStatus,
};
use crate::cql::parse_cql;
use crate::cql::parser::token::testing::{find_string_literal, find_token, rfind_token};
use crate::cql::test_cql::*;
use std::collections::HashMap;

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
                fields: HashMap::from([(
                    find_token(cql, "int_attribute"),
                    rfind_token(cql, "int"),
                )]),
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
                fields: HashMap::from([(find_token(cql, "int_attr"), rfind_token(cql, "int"),)]),
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
                fields: HashMap::from([
                    (find_token(cql, "int_attr"), rfind_token(cql, "int"),),
                    (find_token(cql, "text_attr"), rfind_token(cql, "text"),)
                ]),
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
                fields: HashMap::from([(
                    find_token(cql, "int_attribute"),
                    rfind_token(cql, "int"),
                )]),
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
                fields: HashMap::from([(find_token(cql, "int_attr"), rfind_token(cql, "int"),)]),
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
                fields: HashMap::from([
                    (find_token(cql, "int_attr"), rfind_token(cql, "int"),),
                    (find_token(cql, "text_attr"), rfind_token(cql, "text"),)
                ]),
            }
        )))
    );
}

#[test]
fn test_parsing_create_user_without_password() {
    let cql = CREATE_USER_WITHOUT_PASSWORD;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::User(
            CreateUserStatement {
                user_name: find_token(cql, "big_data_user"),
                if_not_exists: false,
                password: None,
                user_status: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_user_if_not_exists() {
    let cql = CREATE_USER_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::User(
            CreateUserStatement {
                user_name: find_token(cql, "big_data_user"),
                if_not_exists: true,
                password: None,
                user_status: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_user_with_superuser_status() {
    let cql = CREATE_USER_SUPERUSER;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::User(
            CreateUserStatement {
                user_name: find_token(cql, "big_data_user"),
                if_not_exists: false,
                password: None,
                user_status: Some(CreateUserStatus::Superuser),
            }
        )))
    );
}

#[test]
fn test_parsing_create_user_with_not_superuser_status() {
    let cql = CREATE_USER_NOT_SUPERUSER;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::User(
            CreateUserStatement {
                user_name: find_token(cql, "big_data_user"),
                if_not_exists: false,
                password: None,
                user_status: Some(CreateUserStatus::NoSuperuser),
            }
        )))
    );
}

#[test]
fn test_parsing_create_user_with_password() {
    let cql = CREATE_USER_WITH_PASSWORD;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::User(
            CreateUserStatement {
                user_name: find_token(cql, "big_data_user"),
                if_not_exists: false,
                password: Some(CreateUserPassword::PlainText(find_string_literal(
                    cql, "'asdf'"
                ))),
                user_status: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_user_with_triple_quote_password() {
    let cql = CREATE_USER_WITH_TRIPLE_QUOTE_PASSWORD;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::User(
            CreateUserStatement {
                user_name: find_token(cql, "big_data_user"),
                if_not_exists: false,
                password: Some(CreateUserPassword::PlainText(find_string_literal(
                    cql, "'''asdf'''"
                ))),
                user_status: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_user_with_dollar_quote_password() {
    let cql = CREATE_USER_WITH_DOLLAR_QUOTE_PASSWORD;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::User(
            CreateUserStatement {
                user_name: find_token(cql, "big_data_user"),
                if_not_exists: false,
                password: Some(CreateUserPassword::PlainText(find_string_literal(
                    cql, "$$asdf$$"
                ))),
                user_status: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_user_with_superuser_status_and_password() {
    let cql = CREATE_USER_WITH_PASSWORD_SUPERUSER;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::User(
            CreateUserStatement {
                user_name: find_token(cql, "big_data_user"),
                if_not_exists: false,
                password: Some(CreateUserPassword::PlainText(find_string_literal(
                    cql, "'asdf'"
                ))),
                user_status: Some(CreateUserStatus::Superuser),
            }
        )))
    );
}

#[test]
fn test_parsing_create_user_with_not_superuser_status_and_password() {
    let cql = CREATE_USER_WITH_PASSWORD_NOT_SUPERUSER;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::User(
            CreateUserStatement {
                user_name: find_token(cql, "big_data_user"),
                if_not_exists: false,
                password: Some(CreateUserPassword::PlainText(find_string_literal(
                    cql, "'asdf'"
                ))),
                user_status: Some(CreateUserStatus::NoSuperuser),
            }
        )))
    );
}

#[test]
fn test_parsing_create_user_with_hashed_password() {
    let cql = CREATE_USER_WITH_HASHED_PASSWORD;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::User(
            CreateUserStatement {
                user_name: find_token(cql, "big_data_user"),
                if_not_exists: false,
                password: Some(CreateUserPassword::Hashed(find_string_literal(
                    cql,
                    "'aassddff'"
                ))),
                user_status: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_user_with_hashed_password_and_superuser_status() {
    let cql = CREATE_USER_WITH_HASHED_PASSWORD_SUPERUSER;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::User(
            CreateUserStatement {
                user_name: find_token(cql, "big_data_user"),
                if_not_exists: false,
                password: Some(CreateUserPassword::Hashed(find_string_literal(
                    cql,
                    "'aassddff'"
                ))),
                user_status: Some(CreateUserStatus::Superuser),
            }
        )))
    );
}

#[test]
fn test_parsing_create_user_with_hashed_password_and_not_superuser_status() {
    let cql = CREATE_USER_IF_NOT_EXISTS_WITH_HASHED_PASSWORD_NOT_SUPERUSER;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::User(
            CreateUserStatement {
                user_name: find_token(cql, "big_data_user"),
                if_not_exists: true,
                password: Some(CreateUserPassword::Hashed(find_string_literal(
                    cql,
                    "'aassddff'"
                ))),
                user_status: Some(CreateUserStatus::NoSuperuser),
            }
        )))
    );
}
