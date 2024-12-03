use crate::cql::ast::*;
use crate::cql::lex::TokenRange;
use crate::cql::parser::parse_cql;
use crate::cql::test_cql::*;
use std::collections::HashMap;
use std::sync::Arc;

fn find_token(cql: &str, s: &str) -> TokenView {
    let b = cql.find(s).expect("find str in cql to create token view");
    let e = b + s.len() - 1;
    let range = TokenRange::new(b, e);
    TokenView {
        cql: Arc::new(String::from(cql)),
        range,
    }
}

fn rfind_token(cql: &str, s: &str) -> TokenView {
    let b = cql.rfind(s).expect("rfind str in cql to create token view");
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
                fields: HashMap::from([(
                    find_token(cql, "int_attr"),
                    rfind_token(cql, "int"),
                )]),
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
                fields: HashMap::from([(
                    find_token(cql, "int_attr"),
                    rfind_token(cql, "int"),
                ), (
                    find_token(cql, "text_attr"),
                    rfind_token(cql, "text"),
                )]),
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
                fields: HashMap::from([(
                    find_token(cql, "int_attr"),
                    rfind_token(cql, "int"),
                )]),
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
                fields: HashMap::from([(
                    find_token(cql, "int_attr"),
                    rfind_token(cql, "int"),
                ), (
                    find_token(cql, "text_attr"),
                    rfind_token(cql, "text"),
                )]),
            }
        )))
    );
}

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
                signature: Some(vec!(find_token(cql, "int"))),
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
                signature: Some(vec!(find_token(cql, "int"), find_token(cql, "text"))),
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
                signature: Some(vec!(find_token(cql, "int"), find_token(cql, "text"))),
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
fn test_parsing_drop_keyspace_if_exists() {
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

#[test]
fn test_parsing_drop_materialized_view_with_default_keyspace() {
    assert_eq!(
        parse_cql(DROP_MATERIALIZED_VIEW_DEFAULT_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::MaterializedView(
            DropMaterializedViewStatement {
                view_name: find_token(DROP_MATERIALIZED_VIEW_DEFAULT_KEYSPACE, "big_data_view"),
                if_exists: false,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_materialized_view_with_default_keyspace_if_exists() {
    assert_eq!(
        parse_cql(DROP_MATERIALIZED_VIEW_DEFAULT_KEYSPACE_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::MaterializedView(
            DropMaterializedViewStatement {
                view_name: find_token(
                    DROP_MATERIALIZED_VIEW_DEFAULT_KEYSPACE_IF_EXISTS,
                    "big_data_view"
                ),
                if_exists: true,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_materialized_view_with_explicit_keyspace() {
    assert_eq!(
        parse_cql(DROP_MATERIALIZED_VIEW_EXPLICIT_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::MaterializedView(
            DropMaterializedViewStatement {
                view_name: find_token(DROP_MATERIALIZED_VIEW_EXPLICIT_KEYSPACE, "big_data_view"),
                if_exists: false,
                keyspace_name: Some(find_token(
                    DROP_MATERIALIZED_VIEW_EXPLICIT_KEYSPACE,
                    "big_data_keyspace"
                ))
            }
        )))
    );
}

#[test]
fn test_parsing_drop_materialized_view_with_explicit_keyspace_if_exists() {
    assert_eq!(
        parse_cql(DROP_MATERIALIZED_VIEW_EXPLICIT_KEYSPACE_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::MaterializedView(
            DropMaterializedViewStatement {
                view_name: find_token(
                    DROP_MATERIALIZED_VIEW_EXPLICIT_KEYSPACE_IF_EXISTS,
                    "big_data_view"
                ),
                if_exists: true,
                keyspace_name: Some(find_token(
                    DROP_MATERIALIZED_VIEW_EXPLICIT_KEYSPACE_IF_EXISTS,
                    "big_data_keyspace"
                ))
            }
        )))
    );
}

#[test]
fn test_parsing_drop_role() {
    assert_eq!(
        parse_cql(DROP_ROLE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Role(DropRoleStatement {
            role_name: find_token(DROP_ROLE, "big_data_role"),
            if_exists: false,
        })))
    );
}

#[test]
fn test_parsing_drop_role_if_exists() {
    assert_eq!(
        parse_cql(DROP_ROLE_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Role(DropRoleStatement {
            role_name: find_token(DROP_ROLE_IF_EXISTS, "big_data_role"),
            if_exists: true,
        })))
    );
}

#[test]
fn test_parsing_drop_table_with_default_keyspace() {
    assert_eq!(
        parse_cql(DROP_TABLE_DEFAULT_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Table(
            DropTableStatement {
                alias: None,
                table_name: find_token(DROP_TABLE_DEFAULT_KEYSPACE, "big_data_table"),
                if_exists: false,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_table_with_default_keyspace_if_exists() {
    assert_eq!(
        parse_cql(DROP_TABLE_DEFAULT_KEYSPACE_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Table(
            DropTableStatement {
                alias: None,
                table_name: find_token(DROP_TABLE_DEFAULT_KEYSPACE_IF_EXISTS, "big_data_table"),
                if_exists: true,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_table_with_explicit_keyspace() {
    assert_eq!(
        parse_cql(DROP_TABLE_EXPLICIT_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Table(
            DropTableStatement {
                alias: None,
                table_name: find_token(DROP_TABLE_EXPLICIT_KEYSPACE, "big_data_table"),
                if_exists: false,
                keyspace_name: Some(find_token(
                    DROP_TABLE_EXPLICIT_KEYSPACE,
                    "big_data_keyspace"
                ))
            }
        )))
    );
}

#[test]
fn test_parsing_drop_table_with_explicit_keyspace_if_exists() {
    assert_eq!(
        parse_cql(DROP_TABLE_EXPLICIT_KEYSPACE_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Table(
            DropTableStatement {
                alias: None,
                table_name: find_token(DROP_TABLE_EXPLICIT_KEYSPACE_IF_EXISTS, "big_data_table"),
                if_exists: true,
                keyspace_name: Some(find_token(
                    DROP_TABLE_EXPLICIT_KEYSPACE_IF_EXISTS,
                    "big_data_keyspace"
                ))
            }
        )))
    );
}

#[test]
fn test_parsing_drop_trigger_default_keyspace() {
    let cql = DROP_TRIGGER_DEFAULT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Trigger(
            DropTriggerStatement {
                table_name: find_token(cql, "big_data_table"),
                trigger_name: find_token(cql, "big_data_trigger"),
                if_exists: false,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_trigger_default_keyspace_if_exists() {
    let cql = DROP_TRIGGER_DEFAULT_KEYSPACE_IF_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Trigger(
            DropTriggerStatement {
                table_name: find_token(cql, "big_data_table"),
                trigger_name: find_token(cql, "big_data_trigger"),
                if_exists: true,
                keyspace_name: None,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_trigger_explicit_keyspace() {
    let cql = DROP_TRIGGER_EXPLICIT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Trigger(
            DropTriggerStatement {
                table_name: find_token(cql, "big_data_table"),
                trigger_name: find_token(cql, "big_data_trigger"),
                if_exists: false,
                keyspace_name: Some(find_token(cql, "big_data_keyspace"))
            }
        )))
    );
}

#[test]
fn test_parsing_drop_trigger_explicit_keyspace_if_exists() {
    let cql = DROP_TRIGGER_EXPLICIT_KEYSPACE_IF_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Trigger(
            DropTriggerStatement {
                table_name: find_token(cql, "big_data_table"),
                trigger_name: find_token(cql, "big_data_trigger"),
                if_exists: true,
                keyspace_name: Some(find_token(cql, "big_data_keyspace"))
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

#[test]
fn test_parsing_drop_user() {
    assert_eq!(
        parse_cql(DROP_USER.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::User(DropUserStatement {
            user_name: find_token(DROP_USER, "big_data_user"),
            if_exists: false,
        })))
    );
}

#[test]
fn test_parsing_drop_user_if_exists() {
    assert_eq!(
        parse_cql(DROP_USER_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::User(DropUserStatement {
            user_name: find_token(DROP_USER_IF_EXISTS, "big_data_user"),
            if_exists: true,
        })))
    );
}