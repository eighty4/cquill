use crate::cql::ast::{
    AuthPassword, CqlDataType, CqlNativeType, CqlStatement, CqlUserDefinedType, CqlValueType,
    CreateFunctionStatement, CreateIfExistsBehavior, CreateIndexColumn, CreateIndexStatement,
    CreateRoleStatement, CreateStatement, CreateTriggerStatement, CreateTypeStatement,
    CreateUserStatement, CreateUserStatus, Datacenters, OnNullInput, RoleConfigAttribute,
};
use crate::cql::parse_cql;
use crate::cql::parser::testing::{find_string_literal, find_token};
use crate::cql::test_cql::*;
use std::collections::HashMap;

#[test]
fn test_create_function_called_on_null_as_single_quote_string() {
    let cql = CREATE_FUNCTION_CALLED_ON_NULL_AS_SINGLE_QUOTE_STRING;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                function_body: find_string_literal(cql, "'return fn_arg.toString();'"),
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                function_name: find_token(cql, "big_data_fn"),
                if_exists: CreateIfExistsBehavior::Error,
                language: find_token(cql, "java"),
                on_null_input: OnNullInput::Called,
                function_args: vec!((
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )),
            }
        )))
    );
}

#[test]
fn test_create_function_called_on_null_as_dollar_dollar() {
    let cql = CREATE_FUNCTION_CALLED_ON_NULL_AS_DOLLAR_DOLLAR;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg.toString();\n    $$"
                ),
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                function_name: find_token(cql, "big_data_fn"),
                if_exists: CreateIfExistsBehavior::Error,
                language: find_token(cql, "java"),
                on_null_input: OnNullInput::Called,
                function_args: vec![(
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )],
            }
        )))
    );
}

#[test]
fn test_create_function_returns_null_on_null() {
    let cql = CREATE_FUNCTION_RETURNS_NULL_ON_NULL_INPUT;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg.toString();\n    $$"
                ),
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                function_name: find_token(cql, "big_data_fn"),
                if_exists: CreateIfExistsBehavior::Error,
                language: find_token(cql, "java"),
                on_null_input: OnNullInput::ReturnsNull,
                function_args: vec![(
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )],
            }
        )))
    );
}

#[test]
fn test_create_function_or_replace() {
    let cql = CREATE_OR_REPLACE_FUNCTION;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg.toString();\n    $$"
                ),
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                function_name: find_token(cql, "big_data_fn"),
                if_exists: CreateIfExistsBehavior::Replace,
                language: find_token(cql, "java"),
                on_null_input: OnNullInput::Called,
                function_args: vec![(
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )],
            }
        )))
    );
}

#[test]
fn test_create_function_if_not_exists() {
    let cql = CREATE_FUNCTION_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg.toString();\n    $$"
                ),
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                function_name: find_token(cql, "big_data_fn"),
                if_exists: CreateIfExistsBehavior::DoNotError,
                language: find_token(cql, "java"),
                on_null_input: OnNullInput::Called,
                function_args: vec![(
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )],
            }
        )))
    );
}

#[test]
fn test_create_function_with_multiple_args() {
    let cql = CREATE_FUNCTION_WITH_MULTIPLE_ARGS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                if_exists: CreateIfExistsBehavior::Error,
                function_name: find_token(cql, "big_data_fn"),
                function_args: vec!(
                    (
                        find_token(cql, "fn_arg1"),
                        CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                    ),
                    (
                        find_token(cql, "fn_arg2"),
                        CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                    )
                ),
                on_null_input: OnNullInput::Called,
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                language: find_token(cql, "java"),
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg1.toString();\n    $$"
                ),
            }
        )))
    );
}

#[test]
fn test_create_function_returns_udt() {
    let cql = CREATE_FUNCTION_RETURNS_USER_DEFINED_TYPE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                if_exists: CreateIfExistsBehavior::Error,
                function_name: find_token(cql, "big_data_fn"),
                function_args: vec!((
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )),
                on_null_input: OnNullInput::Called,
                returns: CqlDataType::ValueType(CqlValueType::UserDefinedType(
                    CqlUserDefinedType::Unfrozen(find_token(cql, "some_udt"))
                )),
                language: find_token(cql, "java"),
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg.toString();\n    $$"
                ),
            }
        )))
    );
}

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

#[test]
fn test_parsing_create_trigger_in_default_keyspace() {
    let cql = CREATE_TRIGGER_DEFAULT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Trigger(
            CreateTriggerStatement {
                trigger_name: find_token(cql, "big_data_trigger"),
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: None,
                if_not_exists: false,
                index_classpath: find_string_literal(cql, "'trigger name'"),
            }
        )))
    );
}

#[test]
fn test_parsing_create_trigger_in_default_keyspace_if_not_exists() {
    let cql = CREATE_TRIGGER_IF_NOT_EXISTS_DEFAULT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Trigger(
            CreateTriggerStatement {
                trigger_name: find_token(cql, "big_data_trigger"),
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: None,
                if_not_exists: true,
                index_classpath: find_string_literal(cql, "'trigger name'"),
            }
        )))
    );
}

#[test]
fn test_parsing_create_trigger_in_explicit_keyspace() {
    let cql = CREATE_TRIGGER_EXPLICIT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Trigger(
            CreateTriggerStatement {
                trigger_name: find_token(cql, "big_data_trigger"),
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: Some(find_token(cql, "big_data_keyspace")),
                if_not_exists: false,
                index_classpath: find_string_literal(cql, "'trigger name'"),
            }
        )))
    );
}

#[test]
fn test_parsing_create_trigger_in_explicit_keyspace_if_not_exists() {
    let cql = CREATE_TRIGGER_IF_NOT_EXISTS_EXPLICIT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Trigger(
            CreateTriggerStatement {
                trigger_name: find_token(cql, "big_data_trigger"),
                table_name: find_token(cql, "big_data_table"),
                keyspace_name: Some(find_token(cql, "big_data_keyspace")),
                if_not_exists: true,
                index_classpath: find_string_literal(cql, "'trigger name'"),
            }
        )))
    );
}

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
