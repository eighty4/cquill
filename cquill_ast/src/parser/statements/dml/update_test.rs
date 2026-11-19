use std::sync::Arc;

use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_update_single_column() {
    let cql = Arc::new(String::from(UPDATE_SINGLE_COLUMN));
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Update(UpdateStatement {
            table_name: find_token(cql.as_str(), "big_data_table"),
            assignments: vec!(UpdateAssignment {
                selection: UpdateAssignmentSelection::Column {
                    column_name: find_token(cql.as_str(), "int_column"),
                },
                expr_term: UpdateExpressionTerm::Number(TokenView {
                    cql: cql.clone(),
                    range: TokenRange::new(39, 39),
                }),
            }),
            where_clause: UpdateWhereClause {
                relations: vec!(UpdateWhereClauseRelation {
                    column_name: find_token(cql.as_str(), "text_column"),
                    expr_term: UpdateExpressionTerm::String(find_string_literal(
                        &cql,
                        "'big data!'"
                    )),
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
                UpdateAssignment {
                    selection: UpdateAssignmentSelection::Column {
                        column_name: find_token(cql.as_str(), "int_column"),
                    },
                    expr_term: UpdateExpressionTerm::Number(TokenView {
                        cql: cql.clone(),
                        range: TokenRange::new(39, 39),
                    }),
                },
                UpdateAssignment {
                    selection: UpdateAssignmentSelection::Column {
                        column_name: find_token(cql.as_str(), "float_column"),
                    },
                    expr_term: UpdateExpressionTerm::Number(TokenView {
                        cql: cql.clone(),
                        range: TokenRange::new(57, 59),
                    }),
                }
            ),
            where_clause: UpdateWhereClause {
                relations: vec!(UpdateWhereClauseRelation {
                    column_name: find_token(cql.as_str(), "text_column"),
                    expr_term: UpdateExpressionTerm::String(find_string_literal(
                        &cql,
                        "'big data!'"
                    )),
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
            assignments: vec!(UpdateAssignment {
                selection: UpdateAssignmentSelection::Column {
                    column_name: find_token(cql.as_str(), "int_column"),
                },
                expr_term: UpdateExpressionTerm::Number(TokenView {
                    cql: cql.clone(),
                    range: TokenRange::new(39, 39),
                }),
            }),
            where_clause: UpdateWhereClause {
                relations: vec!(UpdateWhereClauseRelation {
                    column_name: find_token(cql.as_str(), "text_column"),
                    expr_term: UpdateExpressionTerm::String(find_string_literal(
                        &cql,
                        "'big data!'"
                    )),
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
            assignments: vec!(UpdateAssignment {
                selection: UpdateAssignmentSelection::Column {
                    column_name: find_token(cql.as_str(), "int_column"),
                },
                expr_term: UpdateExpressionTerm::Number(TokenView {
                    cql: cql.clone(),
                    range: TokenRange::new(39, 39),
                }),
            }),
            where_clause: UpdateWhereClause {
                relations: vec!(UpdateWhereClauseRelation {
                    column_name: find_token(cql.as_str(), "text_column"),
                    expr_term: UpdateExpressionTerm::String(find_string_literal(
                        &cql,
                        "'big data!'"
                    )),
                }),
            },
            if_behavior: Some(UpdateIfBehavior::Conditional(vec![UpdateIfCondition {
                selection: UpdateAssignmentSelection::Column {
                    column_name: rfind_token(cql.as_str(), "int_column"),
                },
                expr_term: UpdateExpressionTerm::Number(rfind_token(cql.as_str(), "6")),
            },])),
        })),
    );
}
