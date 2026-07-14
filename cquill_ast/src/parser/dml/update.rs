use crate::ParseError;
use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;
use crate::parser::terms::parse_bind_marker;

use std::{iter::Peekable, slice::Iter, sync::Arc};

// todo keyspace qualifier on table name
pub fn parse_update_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<UpdateStatement> {
    let table_name = pop_identifier(cql, iter)?;
    let using_params = parse_using_params(cql, iter)?;
    pop_next_match(iter, SetKeyword)?;
    let assignments = parse_assignments(cql, iter)?;
    pop_next_match(iter, WhereKeyword)?;
    let where_clause = parse_where_clause(cql, iter)?;
    let if_behavior = parse_if_behavior(cql, iter)?;
    Ok(UpdateStatement {
        table_name,
        using_params,
        assignments,
        where_clause,
        if_behavior,
    })
}

fn parse_using_params(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<Option<Vec<UpdateParameter>>> {
    if pop_next_if(iter, UsingKeyword).is_none() {
        Ok(None)
    } else {
        let mut update_params: Vec<UpdateParameter> = Vec::new();
        loop {
            update_params.push(match iter.next() {
                Some(Token { name: TimestampKeyword, .. }) => {
                    UpdateParameter::Timestamp(match iter.peek().map(|token| &token.name) {
                        Some(AndKeyword | SetKeyword) => TimestampValue::Unspecified,
                        // todo validate number is not a float
                        Some(NumberLiteral) => TimestampValue::Integer(TokenView::new(cql, iter.next().unwrap())),
                        Some(StringLiteral(_)) => TimestampValue::String(TokenView::new(cql, iter.next().unwrap())),
                        Some(Colon | QuestionMark) => TimestampValue::BindMarker(parse_bind_marker(cql, iter)?),
                        _ => return Err(ParseError::default()),
                    })
                },
                Some(Token { name: TtlKeyword, .. }) => {
                    UpdateParameter::TimeToLive(match iter.peek().map(|token| &token.name) {
                        Some(NullKeyword) => {
                            iter.next().unwrap();
                            TimeToLiveValue::Null
                        },
                        // todo validate number is not a float
                        Some(NumberLiteral) => TimeToLiveValue::Integer(TokenView::new(cql, iter.next().unwrap())),
                        Some(Colon | QuestionMark) => TimeToLiveValue::BindMarker(parse_bind_marker(cql, iter)?),
                        _ => return Err(ParseError::default()),
                    })
                },
                _ => {
                    return Err(ParseError::default());
                }
            });
            if pop_next_if(iter, AndKeyword).is_none() {
                break;
            }
        }
        Ok(Some(update_params))
    }
}

fn parse_assignments(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<Vec<Assignment>> {
    let mut assignments: Vec<Assignment> = Vec::new();
    loop {
        let selection = parse_assignment_selection(cql, iter)?;
        pop_next_match(iter, Equal)?;
        let expr_term = parse_expression_term(cql, iter)?;
        assignments.push(Assignment {
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
) -> ParseResult<AssignmentSelection> {
    let column_name = TokenView::new(cql, pop_next_match(iter, Identifier)?);
    match iter.peek() {
        Some(Token {
            name: LeftSquareBracket,
            ..
        }) => {
            pop_next(iter)?;
            let expr_term = parse_expression_term(cql, iter)?;
            pop_next_match(iter, RightSquareBracket)?;
            Ok(AssignmentSelection::ColumnAccess {
                column_name,
                expr_term,
            })
        }
        Some(Token { name: Dot, .. }) => {
            pop_next(iter)?;
            Ok(AssignmentSelection::ColumnField {
                column_name,
                field_name: TokenView::new(cql, pop_next_match(iter, Identifier)?),
            })
        }
        _ => Ok(AssignmentSelection::Column { column_name }),
    }
}

// todo operator
fn parse_where_clause(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<WhereClause> {
    let mut relations: Vec<WhereClauseRelation> = Vec::new();
    loop {
        let identifier = pop_next_match(iter, Identifier)?;
        let column_name = TokenView {
            cql: cql.clone(),
            range: identifier.range.clone(),
        };
        parse_comparison_operator(iter)?;
        let expr_term = parse_expression_term(cql, iter)?;
        relations.push(WhereClauseRelation {
            column_name,
            // operator,
            expr_term,
        });
        if pop_next_if(iter, Comma).is_none() {
            break;
        }
    }
    Ok(WhereClause { relations })
}

// todo fn calls
fn parse_expression_term(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<ExpressionTerm> {
    match iter.peek() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            StringLiteral(_) => Ok(ExpressionTerm::String(pop_string_literal(cql, iter)?)),
            NumberLiteral => Ok(ExpressionTerm::Number(pop_number_literal(cql, iter)?)),
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
                        if pop_next_if(iter, AndKeyword).is_none() {
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
