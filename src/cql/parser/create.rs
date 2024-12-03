use crate::cql::ast::*;
use crate::cql::lex::Token;
use crate::cql::lex::TokenName::*;
use crate::cql::parser::iter::{advance_peek_match, pop_next};
use crate::cql::parser::token::{create_view, parse_object_identifiers};
use crate::cql::parser::ParseResult;
use std::collections::HashMap;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

pub fn parse_create_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateStatement> {
    match iter.next() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            AggregateKeyword => {
                parse_create_aggregate_statement(cql, iter).map(CreateStatement::Aggregate)
            }
            FunctionKeyword => {
                parse_create_function_statement(cql, iter).map(CreateStatement::Function)
            }
            IndexKeyword => parse_create_index_statement(cql, iter).map(CreateStatement::Index),
            KeyspaceKeyword => {
                parse_create_keyspace_statement(cql, iter).map(CreateStatement::Keyspace)
            }
            MaterializedKeyword => {
                _ = pop_next(iter, ViewKeyword)?;
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

fn parse_create_aggregate_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateAggregateStatement> {
    unimplemented!()
}

fn parse_create_function_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateFunctionStatement> {
    unimplemented!()
}

fn parse_create_index_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateIndexStatement> {
    unimplemented!()
}

fn parse_create_keyspace_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateKeyspaceStatement> {
    unimplemented!()
}

fn parse_create_materialized_view_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateMaterializedViewStatement> {
    unimplemented!()
}

fn parse_create_role_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateRoleStatement> {
    unimplemented!()
}

fn parse_create_table_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateTableStatement> {
    let (keyspace_name, table_name) = parse_object_identifiers(cql, iter)?;
    Ok(CreateTableStatement {
        keyspace_name,
        table_name,
        column_definitions: parse_create_table_column_definitions(cql, iter)?,
        table_alias: None,
        attributes: Vec::new(),
        if_not_exists: false,
    })
}

fn parse_create_trigger_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateTriggerStatement> {
    unimplemented!()
}

// todo fields with collections, collections with generics and udts
fn parse_create_type_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateTypeStatement> {
    let if_not_exists = advance_peek_match(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let (keyspace_name, type_name) = parse_object_identifiers(cql, iter)?;
    _ = pop_next(iter, LeftParenthesis)?;
    let mut fields = HashMap::new();
    loop {
        let field_name = create_view(cql, pop_next(iter, Identifier)?);
        let field_type = match iter.next() {
            None => todo!("panic error"),
            Some(popped) => {
                if popped.name.is_cql_data_type() {
                    create_view(cql, popped)
                } else {
                    todo!("panic error")
                }
            }
        };
        fields.insert(field_name, field_type);
        if iter.next_if(|t| t.name == Comma).is_none() {
            break;
        }
    }
    _ = pop_next(iter, RightParenthesis)?;
    Ok(CreateTypeStatement {
        keyspace_name,
        type_name,
        if_not_exists,
        fields,
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

fn parse_create_user_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateUserStatement> {
    unimplemented!()
}
