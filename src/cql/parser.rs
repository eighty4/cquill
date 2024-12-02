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
    loop {
        if let Ok(_) = peek(&mut iter) {
            result.push(parse_statement(&cql, &mut iter)?);
        } else {
            break;
        }
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
            _ => todo!("parse error"),
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
            TokenName::KeyspaceKeyword => Ok(CreateStatement::Keyspace(parse_create_keyspace_statement(
                cql, iter,
            )?)),
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
    let (keyspace_name, table_name) = parse_keyspace_table_identifier(cql, iter)?;
    Ok(CreateTableStatement {
        keyspace_name,
        table_name,
        column_definitions: parse_create_table_column_definitions(cql, iter)?,
        table_alias: None,
        attributes: Vec::new(),
        if_not_exists: false,
    })
}

fn parse_keyspace_table_identifier(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> Result<(Option<TokenView>, TokenView), anyhow::Error> {
    match iter.next() {
        None => todo!("parse error"),
        Some(token) => {
            if let TokenName::Identifier = token.name {
                if let TokenName::Dot = peek(iter)? {
                    match iter.next() {
                        None => todo!("parse error"),
                        Some(table_token) => {
                            return Ok((
                                Some(create_view(&cql, table_token)),
                                create_view(&cql, table_token),
                            ));
                        }
                    }
                }
            }
            Ok((None, create_view(&cql, token)))
        }
    }
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

fn parse_drop_aggregate_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropAggregateStatement> {
    todo!("parse error")
}

fn parse_drop_function_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropFunctionStatement> {
    todo!("parse error")
}

fn parse_drop_index_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropIndexStatement> {
    todo!("parse error")
}

fn parse_drop_keyspace_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropKeyspaceStatement> {
    todo!("parse error")
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

fn create_view(cql: &Arc<String>, token: &Token) -> TokenView {
    TokenView {
        cql: cql.clone(),
        range: token.range.clone(),
    }
}

fn peek<'a>(iter: &'a mut Peekable<Iter<Token>>) -> Result<&'a TokenName, anyhow::Error> {
    iter.peek()
        .map(|t| &t.name)
        .ok_or_else(|| anyhow::anyhow!(""))
}
