use std::sync::Arc;

use pretty_assertions::assert_eq;

use crate::{
    ast::*,
    parse_cql,
    parser::testing::{find_string_literal, find_token, rfind_token},
    test_cql::*,
};

#[test]
fn test_update_single_column() {
    let cql = Arc::new(String::from(UPDATE_SINGLE_COLUMN));
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Update(UpdateStatement {
            table_name: find_token(cql.as_str(), "big_data_table"),
            assignments: vec!(Assignment {
                selection: AssignmentSelection::Column {
                    column_name: find_token(cql.as_str(), "int_column"),
                },
                expr_term: ExpressionTerm::Number(TokenView {
                    cql: cql.clone(),
                    range: TokenRange::new(39, 39),
                }),
            }),
            where_clause: WhereClause {
                relations: vec!(WhereClauseRelation {
                    column_name: find_token(cql.as_str(), "text_column"),
                    expr_term: ExpressionTerm::String(find_string_literal(&cql, "'big data!'")),
                }),
            },
            if_behavior: None,
        })),
    );
}

#[test]
fn test_update_multiple_columns() {
    let cql = Arc::new(String::from(UPDATE_MULTIPLE_COLUMNS));
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Update(UpdateStatement {
            table_name: find_token(cql.as_str(), "big_data_table"),
            assignments: vec!(
                Assignment {
                    selection: AssignmentSelection::Column {
                        column_name: find_token(cql.as_str(), "int_column"),
                    },
                    expr_term: ExpressionTerm::Number(TokenView {
                        cql: cql.clone(),
                        range: TokenRange::new(39, 39),
                    }),
                },
                Assignment {
                    selection: AssignmentSelection::Column {
                        column_name: find_token(cql.as_str(), "float_column"),
                    },
                    expr_term: ExpressionTerm::Number(TokenView {
                        cql: cql.clone(),
                        range: TokenRange::new(57, 59),
                    }),
                }
            ),
            where_clause: WhereClause {
                relations: vec!(WhereClauseRelation {
                    column_name: find_token(cql.as_str(), "text_column"),
                    expr_term: ExpressionTerm::String(find_string_literal(&cql, "'big data!'")),
                }),
            },
            if_behavior: None,
        })),
    );
}

#[test]
fn test_update_if_exists() {
    let cql = Arc::new(String::from(UPDATE_IF_EXISTS));
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Update(UpdateStatement {
            table_name: find_token(cql.as_str(), "big_data_table"),
            assignments: vec!(Assignment {
                selection: AssignmentSelection::Column {
                    column_name: find_token(cql.as_str(), "int_column"),
                },
                expr_term: ExpressionTerm::Number(TokenView {
                    cql: cql.clone(),
                    range: TokenRange::new(39, 39),
                }),
            }),
            where_clause: WhereClause {
                relations: vec!(WhereClauseRelation {
                    column_name: find_token(cql.as_str(), "text_column"),
                    expr_term: ExpressionTerm::String(find_string_literal(&cql, "'big data!'")),
                }),
            },
            if_behavior: Some(UpdateIfBehavior::Exists),
        })),
    );
}

#[test]
fn test_update_if_single_condition() {
    let cql = Arc::new(String::from(UPDATE_IF_CONDITION));
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Update(UpdateStatement {
            table_name: find_token(cql.as_str(), "big_data_table"),
            assignments: vec!(Assignment {
                selection: AssignmentSelection::Column {
                    column_name: find_token(cql.as_str(), "int_column"),
                },
                expr_term: ExpressionTerm::Number(TokenView {
                    cql: cql.clone(),
                    range: TokenRange::new(39, 39),
                }),
            }),
            where_clause: WhereClause {
                relations: vec!(WhereClauseRelation {
                    column_name: find_token(cql.as_str(), "text_column"),
                    expr_term: ExpressionTerm::String(find_string_literal(&cql, "'big data!'")),
                }),
            },
            if_behavior: Some(UpdateIfBehavior::Conditional(vec![UpdateIfCondition {
                selection: AssignmentSelection::Column {
                    column_name: rfind_token(cql.as_str(), "int_column"),
                },
                expr_term: ExpressionTerm::Number(rfind_token(cql.as_str(), "6")),
            },])),
        })),
    );
}
