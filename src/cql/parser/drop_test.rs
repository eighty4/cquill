use crate::cql::ast::{
    CqlStatement, DropAggregateStatement, DropFunctionStatement, DropIndexStatement,
    DropKeyspaceStatement, DropMaterializedViewStatement, DropRoleStatement, DropStatement,
    DropTableStatement, DropTriggerStatement, DropTypeStatement, DropUserStatement,
};
use crate::cql::parse_cql;
use crate::cql::parser::token::testing::find_token;
use crate::cql::test_cql::*;

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
