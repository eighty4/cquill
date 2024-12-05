use crate::cql::ast::*;
use crate::cql::parse_cql;
use crate::cql::parser::testing::{find_nth_token, find_string_literal, find_token, rfind_token};
use crate::cql::test_cql::*;
use std::collections::HashMap;

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
                if_exists_behavior: CreateIfExistsBehavior::Error,
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
                if_exists_behavior: CreateIfExistsBehavior::Error,
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
                if_exists_behavior: CreateIfExistsBehavior::Error,
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
                if_exists_behavior: CreateIfExistsBehavior::Replace,
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
                if_exists_behavior: CreateIfExistsBehavior::DoNotError,
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
                if_exists_behavior: CreateIfExistsBehavior::Error,
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
fn test_create_function_with_frozen_udt_arg() {
    let cql = CREATE_FUNCTION_WITH_FROZEN_UDT_ARG;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                if_exists_behavior: CreateIfExistsBehavior::Error,
                function_name: find_token(cql, "big_data_fn"),
                function_args: vec!((
                    find_token(cql, "fn_arg"),
                    CqlDataType::Frozen(Box::new(CqlDataType::ValueType(
                        CqlValueType::UserDefinedType(find_token(cql, "some_udt"))
                    ))),
                )),
                on_null_input: OnNullInput::Called,
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
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
fn test_create_function_returns_udt() {
    let cql = CREATE_FUNCTION_RETURNS_USER_DEFINED_TYPE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                if_exists_behavior: CreateIfExistsBehavior::Error,
                function_name: find_token(cql, "big_data_fn"),
                function_args: vec!((
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )),
                on_null_input: OnNullInput::Called,
                returns: CqlDataType::ValueType(CqlValueType::UserDefinedType(find_token(
                    cql, "some_udt"
                ))),
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
fn test_create_keyspace_with_simple_replication() {
    let cql = CREATE_KEYSPACE_WITH_SIMPLE_REPLICATION;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Keyspace(
            CreateKeyspaceStatement {
                if_not_exists: false,
                keyspace_name: find_token(cql, "big_data_keyspace"),
                replication: KeyspaceReplication::Simple(1),
                durable_writes: None,
            }
        )))
    );
}

#[test]
fn test_create_keyspace_with_network_topology_replication() {
    let cql = CREATE_KEYSPACE_WITH_NETWORK_REPLICATION;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Keyspace(
            CreateKeyspaceStatement {
                if_not_exists: false,
                keyspace_name: find_token(cql, "big_data_keyspace"),
                replication: KeyspaceReplication::NetworkTopology(HashMap::from([
                    ("dc1".to_string(), 2),
                    ("dc2".to_string(), 2),
                ])),
                durable_writes: None,
            }
        )))
    );
}

#[test]
fn test_create_keyspace_if_not_exists() {
    let cql = CREATE_KEYSPACE_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Keyspace(
            CreateKeyspaceStatement {
                if_not_exists: true,
                keyspace_name: find_token(cql, "big_data_keyspace"),
                replication: KeyspaceReplication::Simple(1),
                durable_writes: None,
            }
        )))
    );
}

#[test]
fn test_create_keyspace_with_durable_writes_false() {
    let cql = CREATE_KEYSPACE_WITH_DURABLE_WRITES_FALSE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Keyspace(
            CreateKeyspaceStatement {
                if_not_exists: false,
                keyspace_name: find_token(cql, "big_data_keyspace"),
                replication: KeyspaceReplication::Simple(1),
                durable_writes: Some(false),
            }
        )))
    );
}

#[test]
fn test_create_keyspace_with_durable_writes_true() {
    let cql = CREATE_KEYSPACE_WITH_DURABLE_WRITES_TRUE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Keyspace(
            CreateKeyspaceStatement {
                if_not_exists: false,
                keyspace_name: find_token(cql, "big_data_keyspace"),
                replication: KeyspaceReplication::Simple(1),
                durable_writes: Some(true),
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
fn test_parsing_create_table_with_all_native_data_types() {
    let cql = CREATE_TABLE_WITH_ALL_NATIVE_DATA_TYPES;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: [
                    ("uuid_column", CqlNativeType::Uuid, None),
                    ("int_column", CqlNativeType::Int, None),
                    ("ascii_column", CqlNativeType::Ascii, None),
                    ("bigint_column", CqlNativeType::BigInt, None),
                    ("blob_column", CqlNativeType::Blob, None),
                    ("boolean_column", CqlNativeType::Boolean, None),
                    ("date_column", CqlNativeType::Date, None),
                    ("decimal_column", CqlNativeType::Decimal, None),
                    ("double_column", CqlNativeType::Double, None),
                    ("duration_column", CqlNativeType::Duration, None),
                    ("float_column", CqlNativeType::Float, None),
                    ("inet_column", CqlNativeType::INet, None),
                    ("smallint_column", CqlNativeType::SmallInt, None),
                    (
                        "text_column",
                        CqlNativeType::Text,
                        Some(ColumnDefinitionAttribute::PrimaryKey)
                    ),
                    ("time_column", CqlNativeType::Time, None),
                    ("timestamp_column", CqlNativeType::Timestamp, None),
                    ("timeuuid_column", CqlNativeType::TimeUuid, None),
                    ("tinyint_column", CqlNativeType::TinyInt, None),
                    ("varchar_column", CqlNativeType::VarChar, None),
                    ("varint_column", CqlNativeType::VarInt, None),
                ]
                .into_iter()
                .map(|(cn, dt, attribute)| ColumnDefinition::Column {
                    column_name: find_token(cql, cn),
                    data_type: CqlDataType::ValueType(CqlValueType::NativeType(dt)),
                    attribute,
                })
                .collect(),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_counter_data_type() {
    let cql = CREATE_TABLE_WITH_COUNTER_DATA_TYPE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "text_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Text
                        )),
                        attribute: Some(ColumnDefinitionAttribute::PrimaryKey),
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "counter_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Counter
                        )),
                        attribute: None,
                    },
                ),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_if_not_exists() {
    let cql = CREATE_TABLE_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: true,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(ColumnDefinition::Column {
                    column_name: find_token(cql, "uuid_column"),
                    data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                        CqlNativeType::Uuid
                    )),
                    attribute: Some(ColumnDefinitionAttribute::PrimaryKey),
                },),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_explicit_keyspace() {
    let cql = CREATE_TABLE_WITH_EXPLICIT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: Some(find_token(cql, "big_data_keyspace")),
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(ColumnDefinition::Column {
                    column_name: find_token(cql, "uuid_column"),
                    data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                        CqlNativeType::Uuid
                    )),
                    attribute: Some(ColumnDefinitionAttribute::PrimaryKey),
                },),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_compound_primary_key() {
    let cql = CREATE_TABLE_WITH_COMPOUND_PRIMARY_KEY;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "text_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Text
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "uuid_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Uuid
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::PrimaryKey(PrimaryKeyDefinition::Compound {
                        partition: rfind_token(cql, "text_column"),
                        clustering: vec!(rfind_token(cql, "uuid_column")),
                    }),
                ),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_composite_partition_primary_key() {
    let cql = CREATE_TABLE_WITH_COMPOSITE_PARTITION_PRIMARY_KEY;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "text_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Text
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "uuid_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Uuid
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "timestamp_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Timestamp
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::PrimaryKey(PrimaryKeyDefinition::CompositePartition {
                        partition: vec!(
                            rfind_token(cql, "text_column"),
                            rfind_token(cql, "uuid_column")
                        ),
                        clustering: vec!(rfind_token(cql, "timestamp_column")),
                    }),
                ),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_composite_partition_primary_key_missing_clustering_keys() {
    let cql = CREATE_TABLE_WITH_COMPOSITE_PARTITION_PRIMARY_KEY_MISSING_CLUSTERING_KEYS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "text_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Text
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "uuid_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Uuid
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "timestamp_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Timestamp
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::PrimaryKey(PrimaryKeyDefinition::CompositePartition {
                        partition: vec!(
                            rfind_token(cql, "text_column"),
                            rfind_token(cql, "uuid_column")
                        ),
                        clustering: vec!(),
                    }),
                ),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_comment() {
    let cql = CREATE_TABLE_WITH_COMMENT;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(ColumnDefinition::Column {
                    column_name: find_token(cql, "uuid_column"),
                    data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                        CqlNativeType::Uuid
                    )),
                    attribute: Some(ColumnDefinitionAttribute::PrimaryKey),
                },),
                attributes: Some(vec!(TableDefinitionAttribute::Comment(
                    find_string_literal(cql, "'big data!'")
                ))),
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_compact_storage() {
    let cql = CREATE_TABLE_WITH_COMPACT_STORAGE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(ColumnDefinition::Column {
                    column_name: find_token(cql, "uuid_column"),
                    data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                        CqlNativeType::Uuid
                    )),
                    attribute: Some(ColumnDefinitionAttribute::PrimaryKey),
                },),
                attributes: Some(vec!(TableDefinitionAttribute::CompactStorage)),
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_compaction() {
    let cql = CREATE_TABLE_WITH_COMPACTION;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(ColumnDefinition::Column {
                    column_name: find_token(cql, "uuid_column"),
                    data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                        CqlNativeType::Uuid
                    )),
                    attribute: Some(ColumnDefinitionAttribute::PrimaryKey),
                },),
                attributes: Some(vec!(TableDefinitionAttribute::Compaction(HashMap::from([
                    (
                        find_string_literal(cql, "'class'"),
                        find_token(cql, "'LeveledCompactionStrategy'")
                    ),
                ])))),
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_clustering_order() {
    for (cql, clustering_order) in [
        (CREATE_TABLE_WITH_ASC_CLUSTERING_ORDER, ClusteringOrder::Asc),
        (
            CREATE_TABLE_WITH_DESC_CLUSTERING_ORDER,
            ClusteringOrder::Desc,
        ),
    ] {
        assert_eq!(
            parse_cql(cql.to_string()).unwrap(),
            vec!(CqlStatement::Create(CreateStatement::Table(
                CreateTableStatement {
                    if_not_exists: false,
                    keyspace_name: None,
                    table_name: find_token(cql, "big_data_table"),
                    column_definitions: vec!(
                        ColumnDefinition::Column {
                            column_name: find_token(cql, "text_column"),
                            data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                                CqlNativeType::Text
                            )),
                            attribute: None,
                        },
                        ColumnDefinition::Column {
                            column_name: find_token(cql, "uuid_column"),
                            data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                                CqlNativeType::Uuid
                            )),
                            attribute: None,
                        },
                        ColumnDefinition::Column {
                            column_name: find_token(cql, "time_column"),
                            data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                                CqlNativeType::Timestamp
                            )),
                            attribute: None,
                        },
                        ColumnDefinition::PrimaryKey(PrimaryKeyDefinition::Compound {
                            partition: rfind_token(cql, "text_column"),
                            clustering: vec!(find_nth_token(cql, 1, "time_column")),
                        }),
                    ),
                    attributes: Some(vec!(TableDefinitionAttribute::ClusteringOrderBy(vec!(
                        ClusteringOrderDefinition {
                            column_name: rfind_token(cql, "time_column"),
                            order: Some(clustering_order),
                        }
                    )))),
                    table_alias: None,
                }
            )))
        );
    }
}

#[test]
fn test_parsing_create_table_with_multiple_clustering_orders() {
    let cql = CREATE_TABLE_WITH_MULTIPLE_CLUSTERING_ORDERS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "text_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Text
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "uuid_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Uuid
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "time_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Timestamp
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::PrimaryKey(PrimaryKeyDefinition::Compound {
                        partition: rfind_token(cql, "text_column"),
                        clustering: vec!(
                            find_nth_token(cql, 1, "time_column"),
                            find_nth_token(cql, 1, "uuid_column")
                        ),
                    }),
                ),
                attributes: Some(vec!(TableDefinitionAttribute::ClusteringOrderBy(vec!(
                    ClusteringOrderDefinition {
                        column_name: rfind_token(cql, "time_column"),
                        order: Some(ClusteringOrder::Desc),
                    },
                    ClusteringOrderDefinition {
                        column_name: rfind_token(cql, "uuid_column"),
                        order: Some(ClusteringOrder::Asc),
                    },
                )))),
                table_alias: None,
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
