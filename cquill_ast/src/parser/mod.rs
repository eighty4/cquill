mod iter;
mod statements;

#[cfg(test)]
mod testing;

use crate::ast::*;
use crate::lex::*;
use crate::parser::iter::pop_next;
use crate::parser::iter::pop_next_if;
use crate::parser::iter::pop_next_match;
use crate::parser::statements::*;
use TokenName::*;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid cql encountered")]
    InvalidCql,
}

pub type ParseResult<T> = Result<T, ParseError>;

pub fn parse_cql(cql: String) -> ParseResult<Vec<CqlStatement>> {
    let tokens = Tokenizer::new(cql.as_str()).tokenize().unwrap();
    let cql = Arc::new(cql);
    let mut iter = tokens.iter().peekable();
    let mut result = Vec::new();
    while iter.peek().is_some() {
        result.push(parse_statement(&cql, &mut iter)?);
        while pop_next_if(&mut iter, Semicolon).is_some() {}
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
            BeginKeyword => parse_batch_statement(cql, iter).map(CqlStatement::Batch),
            CreateKeyword => parse_create_statement(cql, iter).map(CqlStatement::Create),
            DeleteKeyword => parse_delete_statement(cql, iter).map(CqlStatement::Delete),
            DropKeyword => parse_drop_statement(cql, iter).map(CqlStatement::Drop),
            InsertKeyword => parse_insert_statement(cql, iter).map(CqlStatement::Insert),
            SelectKeyword => parse_select_statement(cql, iter).map(CqlStatement::Select),
            UpdateKeyword => parse_update_statement(cql, iter).map(CqlStatement::Update),
            UseKeyword => parse_use_keyspace_statement(cql, iter).map(CqlStatement::Use),
            AlterKeyword | GrantKeyword | ListKeyword | RevokeKeyword => todo!(),
            _ => todo!("parse error {:?}", token.name),
        },
    }
}

pub fn parse_create_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateStatement> {
    match iter.next() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            AggregateKeyword => {
                parse_create_aggregate_statement(cql, iter, false).map(CreateStatement::Aggregate)
            }
            FunctionKeyword => {
                parse_create_function_statement(cql, iter, false).map(CreateStatement::Function)
            }
            OrKeyword => {
                pop_next_match(iter, ReplaceKeyword)?;
                match pop_next(iter)?.name {
                    AggregateKeyword => parse_create_aggregate_statement(cql, iter, true)
                        .map(CreateStatement::Aggregate),
                    FunctionKeyword => parse_create_function_statement(cql, iter, true)
                        .map(CreateStatement::Function),
                    _ => todo!("parse error"),
                }
            }
            CustomKeyword => {
                parse_create_custom_index_statement(cql, iter).map(CreateStatement::CustomIndex)
            }
            IndexKeyword => parse_create_index_statement(cql, iter).map(CreateStatement::Index),
            KeyspaceKeyword => {
                parse_create_keyspace_statement(cql, iter).map(CreateStatement::Keyspace)
            }
            MaterializedKeyword => {
                pop_next_match(iter, ViewKeyword)?;
                parse_create_materialized_view_statement(cql, iter)
                    .map(CreateStatement::MaterializedView)
            }
            RoleKeyword => parse_create_role_statement(cql, iter).map(CreateStatement::Role),
            TableKeyword => parse_create_table_statement(cql, iter).map(CreateStatement::Table),
            TriggerKeyword => {
                parse_create_trigger_statement(cql, iter).map(CreateStatement::Trigger)
            }
            TypeKeyword => parse_create_type_statement(cql, iter).map(CreateStatement::Type),
            UserKeyword => parse_create_user_statement(cql, iter).map(CreateStatement::User),
            _ => todo!("parse error"),
        },
    }
}

pub fn parse_drop_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropStatement> {
    match iter.next() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            AggregateKeyword => {
                parse_drop_aggregate_statement(cql, iter).map(DropStatement::Aggregate)
            }
            FunctionKeyword => {
                parse_drop_function_statement(cql, iter).map(DropStatement::Function)
            }
            IndexKeyword => parse_drop_index_statement(cql, iter).map(DropStatement::Index),
            KeyspaceKeyword => {
                parse_drop_keyspace_statement(cql, iter).map(DropStatement::Keyspace)
            }
            MaterializedKeyword => {
                pop_next_match(iter, ViewKeyword)?;
                parse_drop_materialized_view_statement(cql, iter)
                    .map(DropStatement::MaterializedView)
            }
            RoleKeyword => parse_drop_role_statement(cql, iter).map(DropStatement::Role),
            TableKeyword => parse_drop_table_statement(cql, iter).map(DropStatement::Table),
            TriggerKeyword => parse_drop_trigger_statement(cql, iter).map(DropStatement::Trigger),
            TypeKeyword => parse_drop_type_statement(cql, iter).map(DropStatement::Type),
            UserKeyword => parse_drop_user_statement(cql, iter).map(DropStatement::User),
            _ => todo!("parse error"),
        },
    }
}
