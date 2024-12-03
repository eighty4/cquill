use crate::cql::ast::*;
use crate::cql::lex::*;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

pub type ParseResult<T> = Result<T, anyhow::Error>;

pub fn parse_cql(cql: String) -> ParseResult<Vec<CqlStatement>> {
    let tokens = Tokenizer::new(cql.as_str()).tokenize().unwrap();
    let cql = Arc::new(cql);
    let mut iter = tokens.iter().peekable();
    let mut result = Vec::new();
    while iter.peek().is_some() {
        result.push(parse_statement(&cql, &mut iter)?);
        while iter.next_if(|t| t.name == TokenName::Semicolon).is_some() {}
    }
    if result.is_empty() {
        todo!("parse error")
    } else {
        Ok(result)
    }
}

fn parse_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CqlStatement> {
    match iter.next() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            TokenName::CreateKeyword => parse_create_statement(cql, iter).map(CqlStatement::Create),
            TokenName::DropKeyword => parse_drop_statement(cql, iter).map(CqlStatement::Drop),
            _ => todo!("parse error {:?}", token.name),
        },
    }
}

fn parse_create_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateStatement> {
    match iter.next() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            TokenName::IndexKeyword => Ok(CreateStatement::Index(parse_create_index_statement(
                cql, iter,
            )?)),
            TokenName::KeyspaceKeyword => Ok(CreateStatement::Keyspace(
                parse_create_keyspace_statement(cql, iter)?,
            )),
            TokenName::RoleKeyword => Ok(CreateStatement::Role(parse_create_role_statement(
                cql, iter,
            )?)),
            TokenName::TableKeyword => Ok(CreateStatement::Table(parse_create_table_statement(
                cql, iter,
            )?)),
            _ => todo!("parse error"),
        },
    }
}

fn parse_create_index_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateIndexStatement> {
    todo!("parse result")
}

fn parse_create_keyspace_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateKeyspaceStatement> {
    todo!("parse result")
}

fn parse_create_role_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateRoleStatement> {
    todo!("parse result")
}

fn parse_create_table_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateTableStatement> {
    let (keyspace_name, table_name) = parse_keyspace_object_identifier(cql, iter)?;
    Ok(CreateTableStatement {
        keyspace_name,
        table_name,
        column_definitions: parse_create_table_column_definitions(cql, iter)?,
        table_alias: None,
        attributes: Vec::new(),
        if_not_exists: false,
    })
}

fn parse_create_table_column_definitions(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<ColumnDefinitions> {
    todo!()
    // ColumnDefinitions {
    //     definitions: Vec::new(),
    //     primary_key: None,
    //     view: create_view(),
    // }
}

fn parse_drop_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropStatement> {
    match iter.next() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            TokenName::AggregateKeyword => {
                parse_drop_aggregate_statement(cql, iter).map(DropStatement::Aggregate)
            }
            TokenName::FunctionKeyword => {
                parse_drop_function_statement(cql, iter).map(DropStatement::Function)
            }
            TokenName::IndexKeyword => {
                parse_drop_index_statement(cql, iter).map(DropStatement::Index)
            }
            TokenName::KeyspaceKeyword => {
                parse_drop_keyspace_statement(cql, iter).map(DropStatement::Keyspace)
            }
            TokenName::MaterializedKeyword => match iter.next() {
                None => todo!("parse error"),
                Some(next) => match next.name {
                    TokenName::ViewKeyword => parse_drop_materialized_view_statement(cql, iter)
                        .map(DropStatement::MaterializedView),
                    _ => todo!("parse error"),
                },
            },
            TokenName::TableKeyword => {
                parse_drop_table_statement(cql, iter).map(DropStatement::Table)
            }
            TokenName::TriggerKeyword => {
                parse_drop_trigger_statement(cql, iter).map(DropStatement::Trigger)
            }
            TokenName::TypeKeyword => parse_drop_type_statement(cql, iter).map(DropStatement::Type),
            _ => todo!("parse error"),
        },
    }
}

// todo aggregate signatures
fn parse_drop_aggregate_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropAggregateStatement> {
    let if_exists = peek_match_advance(iter, &[TokenName::IfKeyword, TokenName::ExistsKeyword])?;
    let (keyspace_name, aggregate_name) = parse_keyspace_object_identifier(cql, iter)?;
    Ok(DropAggregateStatement {
        aggregate_name,
        if_exists,
        keyspace_name,
    })
}

// todo function signatures
fn parse_drop_function_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropFunctionStatement> {
    let if_exists = peek_match_advance(iter, &[TokenName::IfKeyword, TokenName::ExistsKeyword])?;
    let (keyspace_name, function_name) = parse_keyspace_object_identifier(cql, iter)?;
    Ok(DropFunctionStatement {
        function_name,
        if_exists,
        keyspace_name,
    })
}

fn parse_drop_index_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropIndexStatement> {
    let if_exists = peek_match_advance(iter, &[TokenName::IfKeyword, TokenName::ExistsKeyword])?;
    let (keyspace_name, index_name) = parse_keyspace_object_identifier(cql, iter)?;
    Ok(DropIndexStatement {
        index_name,
        if_exists,
        keyspace_name,
    })
}

fn parse_drop_keyspace_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropKeyspaceStatement> {
    let (keyspace_name, if_exists) = match iter.next() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            TokenName::IfKeyword => match iter.next() {
                None => todo!("parse error"),
                Some(exists_token) => match exists_token.name {
                    TokenName::ExistsKeyword => match iter.next() {
                        None => todo!("parse error"),
                        Some(keyword_token) => (create_view(cql, keyword_token), true),
                    },
                    _ => todo!("parse error"),
                },
            },
            TokenName::Identifier => (create_view(cql, token), false),
            _ => todo!("parse error"),
        },
    };
    Ok(DropKeyspaceStatement {
        keyspace_name,
        if_exists,
    })
}

fn parse_drop_materialized_view_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropMaterializedViewStatement> {
    todo!("parse error")
}

fn parse_drop_table_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropTableStatement> {
    todo!("parse error")
}

fn parse_drop_trigger_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropTriggerStatement> {
    todo!("parse error")
}

fn parse_drop_type_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropTypeStatement> {
    todo!("parse error")
}

fn parse_keyspace_object_identifier(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> Result<(Option<TokenView>, TokenView), anyhow::Error> {
    let object_or_keyspace = create_view(cql, next(iter, TokenName::Identifier)?);
    match iter.peek().map(|t| &t.name) {
        Some(TokenName::Dot) => {
            _ = iter.next();
            Ok((
                Some(object_or_keyspace),
                create_view(cql, next(iter, TokenName::Identifier)?),
            ))
        }
        _ => Ok((None, object_or_keyspace)),
    }
}

fn create_view(cql: &Arc<String>, token: &Token) -> TokenView {
    TokenView {
        cql: cql.clone(),
        range: token.range.clone(),
    }
}

fn next<'a>(
    iter: &'a mut Peekable<Iter<Token>>,
    next: TokenName,
) -> Result<&'a Token, anyhow::Error> {
    match iter.next() {
        None => todo!("panic error"),
        Some(token) => {
            if token.name == next {
                Ok(token)
            } else {
                todo!("panic error")
            }
        }
    }
}

fn peek<'a>(iter: &'a mut Peekable<Iter<Token>>) -> Result<&'a TokenName, anyhow::Error> {
    iter.peek()
        .map(|t| &t.name)
        .ok_or_else(|| todo!("parse error"))
}

/// Advances iter for each of input tokens so long as peeking next matches input.
/// If iter is advanced with `next()` and a subsequent `peek()` is None or does not match input,
/// this function will return an error.
/// Returns true and advances iterator `nexts.len()` number of times if all tokens match.
/// Returns false if first token peeked does not match `nexts.first()`.
/// Returns error if first token peeked matches and subsequent peeks return None or do not match.
fn peek_match_advance(
    iter: &mut Peekable<Iter<Token>>,
    nexts: &[TokenName],
) -> Result<bool, anyhow::Error> {
    let mut advanced = false;
    for maybe_next in nexts {
        match iter.peek() {
            None => todo!("parse error"),
            Some(next) => {
                if next.name == *maybe_next {
                    _ = iter.next();
                    advanced = true;
                } else if advanced {
                    todo!("parse error")
                } else {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}
