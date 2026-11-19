use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;

use std::{iter::Peekable, slice::Iter, sync::Arc};

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/dml.html#update-statement
pub fn parse_update_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<UpdateStatement> {
    let table_name = pop_identifier(cql, iter)?;
    pop_next_match(iter, SetKeyword)?;
    let assignments = parse_assignments(cql, iter)?;
    pop_next_match(iter, WhereKeyword)?;
    let where_clause = parse_where_clause(cql, iter)?;
    let if_behavior = parse_if_behavior(cql, iter)?;
    Ok(UpdateStatement {
        table_name,
        assignments,
        where_clause,
        if_behavior,
    })
}

fn parse_assignments(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<Vec<UpdateAssignment>> {
    let mut assignments: Vec<UpdateAssignment> = Vec::new();
    loop {
        let selection = parse_assignment_selection(cql, iter)?;
        pop_next_match(iter, Equal)?;
        let expr_term = parse_expression_term(cql, iter)?;
        assignments.push(UpdateAssignment {
            selection,
            expr_term,
        });
        if pop_next_if(iter, Comma).is_none() {
            break;
        }
    }
    Ok(assignments)
}

// todo unit test column field and column access
fn parse_assignment_selection(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<UpdateAssignmentSelection> {
    dbg!(iter.peek());
    let column_name = TokenView::new(cql, pop_next_match(iter, Identifier)?);
    match iter.peek() {
        Some(Token {
            name: LeftSquareBracket,
            ..
        }) => {
            pop_next(iter)?;
            let expr_term = parse_expression_term(cql, iter)?;
            pop_next_match(iter, RightSquareBracket)?;
            Ok(UpdateAssignmentSelection::ColumnAccess {
                column_name,
                expr_term,
            })
        }
        Some(Token { name: Dot, .. }) => {
            pop_next(iter)?;
            Ok(UpdateAssignmentSelection::ColumnField {
                column_name,
                field_name: TokenView::new(cql, pop_next_match(iter, Identifier)?),
            })
        }
        _ => Ok(UpdateAssignmentSelection::Column { column_name }),
    }
}

// todo operator
fn parse_where_clause(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<UpdateWhereClause> {
    let mut relations: Vec<UpdateWhereClauseRelation> = Vec::new();
    loop {
        let identifier = pop_next_match(iter, Identifier)?;
        let column_name = TokenView {
            cql: cql.clone(),
            range: identifier.range.clone(),
        };
        parse_comparison_operator(iter)?;
        let expr_term = parse_expression_term(cql, iter)?;
        relations.push(UpdateWhereClauseRelation {
            column_name,
            // operator,
            expr_term,
        });
        if pop_next_if(iter, Comma).is_none() {
            break;
        }
    }
    Ok(UpdateWhereClause { relations })
}

// todo fn calls
fn parse_expression_term(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<UpdateExpressionTerm> {
    match iter.peek() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            StringLiteral(_) => Ok(UpdateExpressionTerm::String(pop_string_literal(cql, iter)?)),
            NumberLiteral => Ok(UpdateExpressionTerm::Number(pop_number_literal(cql, iter)?)),
            _ => todo!("parse error on {:?}", token.name),
        },
    }
}

fn parse_if_behavior(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<Option<UpdateIfBehavior>> {
    match pop_next_if(iter, IfKeyword) {
        None => Ok(None),
        Some(_) => match iter.peek() {
            None => todo!("parse error"),
            Some(token) => match token.name {
                ExistsKeyword => {
                    pop_next(iter)?;
                    Ok(Some(UpdateIfBehavior::Exists))
                }
                Identifier => {
                    let mut conditions: Vec<UpdateIfCondition> = Vec::new();
                    loop {
                        let selection = parse_assignment_selection(cql, iter)?;
                        parse_comparison_operator(iter)?;
                        let expr_term = parse_expression_term(cql, iter)?;
                        conditions.push(UpdateIfCondition {
                            selection,
                            expr_term,
                        });
                        if pop_next_if(iter, Comma).is_none() {
                            break;
                        }
                    }
                    Ok(Some(UpdateIfBehavior::Conditional(conditions)))
                }
                _ => todo!("parse error"),
            },
        },
    }
}

fn parse_comparison_operator(iter: &mut Peekable<Iter<Token>>) -> ParseResult<()> {
    let token = pop_next(iter)?;
    match token.name {
        Equal | GreaterThan | LessThan | GreaterThanEqual | LessThanEqual => Ok(()),
        _ => todo!("parse error"),
    }
}
