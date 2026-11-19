use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

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
                password: Some(AuthPassword::PlainText(find_string_literal(cql, "'asdf'"))),
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
                password: Some(AuthPassword::PlainText(find_string_literal(
                    cql,
                    "'''asdf'''"
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
                password: Some(AuthPassword::PlainText(find_string_literal(
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
                password: Some(AuthPassword::PlainText(find_string_literal(cql, "'asdf'"))),
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
                password: Some(AuthPassword::PlainText(find_string_literal(cql, "'asdf'"))),
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
                password: Some(AuthPassword::Hashed(find_string_literal(cql, "'aassddff'"))),
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
                password: Some(AuthPassword::Hashed(find_string_literal(cql, "'aassddff'"))),
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
                password: Some(AuthPassword::Hashed(find_string_literal(cql, "'aassddff'"))),
                user_status: Some(CreateUserStatus::NoSuperuser),
            }
        )))
    );
}
