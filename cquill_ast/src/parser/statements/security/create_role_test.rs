use std::collections::HashMap;

use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_parsing_create_role() {
    let cql = CREATE_ROLE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: false,
                attributes: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_role_if_not_exists() {
    let cql = CREATE_ROLE_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: true,
                attributes: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_role_with_plaintext_password() {
    let cql = CREATE_ROLE_WITH_PASSWORD;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: false,
                attributes: Some(vec!(RoleConfigAttribute::Password(
                    AuthPassword::PlainText(find_string_literal(cql, "'asdf'"))
                ))),
            }
        )))
    );
}

#[test]
fn test_parsing_create_role_plaintext_password_if_not_exists() {
    let cql = CREATE_ROLE_WITH_PASSWORD_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: true,
                attributes: Some(vec!(RoleConfigAttribute::Password(
                    AuthPassword::PlainText(find_string_literal(cql, "'asdf'"))
                ))),
            }
        )))
    );
}

#[test]
fn test_parsing_create_role_with_hashed_password() {
    let cql = CREATE_ROLE_WITH_HASHED_PASSWORD;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: false,
                attributes: Some(vec!(RoleConfigAttribute::Password(AuthPassword::Hashed(
                    find_string_literal(cql, "'aassddff'")
                )))),
            }
        )))
    );
}

#[test]
fn test_parsing_create_role_with_login_true() {
    let cql = CREATE_ROLE_WITH_LOGIN_TRUE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: false,
                attributes: Some(vec!(RoleConfigAttribute::Login(true))),
            }
        )))
    );
}

#[test]
fn test_parsing_create_role_with_login_false() {
    let cql = CREATE_ROLE_WITH_LOGIN_FALSE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: false,
                attributes: Some(vec!(RoleConfigAttribute::Login(false))),
            }
        )))
    );
}

#[test]
fn test_parsing_create_role_with_superuser_true() {
    let cql = CREATE_ROLE_WITH_SUPERUSER_TRUE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: false,
                attributes: Some(vec!(RoleConfigAttribute::Superuser(true))),
            }
        )))
    );
}

#[test]
fn test_parsing_create_role_with_superuser_false() {
    let cql = CREATE_ROLE_WITH_SUPERUSER_FALSE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: false,
                attributes: Some(vec!(RoleConfigAttribute::Superuser(false))),
            }
        )))
    );
}

#[test]
fn test_parsing_create_role_with_options() {
    let cql = CREATE_ROLE_WITH_OPTIONS_MAP;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: false,
                attributes: Some(vec!(RoleConfigAttribute::Options(HashMap::from([
                    (find_string_literal(cql, "'opt1'"), find_token(cql, "'val'")),
                    (find_string_literal(cql, "'opt2'"), find_token(cql, "99")),
                ])))),
            }
        )))
    );
}

#[test]
fn test_parsing_create_role_with_access_to_datacenters() {
    let cql = CREATE_ROLE_WITH_ACCESS_TO_DATACENTERS_SET;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: false,
                attributes: Some(vec!(RoleConfigAttribute::Access(Datacenters::Explicit(
                    vec!(
                        find_string_literal(cql, "'dc1'"),
                        find_string_literal(cql, "'dc2'"),
                    )
                )))),
            }
        )))
    );
}

#[test]
fn test_parsing_create_role_with_access_to_all_datacenters() {
    let cql = CREATE_ROLE_WITH_ACCESS_TO_ALL_DATACENTERS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: false,
                attributes: Some(vec!(RoleConfigAttribute::Access(Datacenters::All))),
            }
        )))
    );
}

#[test]
fn test_parsing_create_role_with_multiple_attributes() {
    let cql = CREATE_ROLE_WITH_MULTIPLE_ROLE_OPTIONS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Role(
            CreateRoleStatement {
                role_name: find_token(cql, "big_data_role"),
                if_not_exists: false,
                attributes: Some(vec!(
                    RoleConfigAttribute::Password(AuthPassword::PlainText(find_string_literal(
                        cql, "'asdf'"
                    ))),
                    RoleConfigAttribute::Login(true)
                )),
            }
        )))
    );
}
