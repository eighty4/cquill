use crate::cql::ast::*;
use crate::cql::lex::Token;
use crate::cql::lex::TokenName::*;
use crate::cql::parser::iter::{
    pop_cql_data_type, pop_identifier, pop_keyspace_object_name, pop_next_if, pop_next_match,
    pop_sequence,
};
use crate::cql::parser::ParseResult;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

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

fn parse_drop_aggregate_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropAggregateStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, aggregate_name) = pop_keyspace_object_name(cql, iter)?;
    let signature = pop_function_type_signature(cql, iter)?;
    Ok(DropAggregateStatement {
        aggregate_name,
        if_exists,
        keyspace_name,
        signature,
    })
}

fn parse_drop_function_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropFunctionStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, function_name) = pop_keyspace_object_name(cql, iter)?;
    let signature = pop_function_type_signature(cql, iter)?;
    Ok(DropFunctionStatement {
        function_name,
        if_exists,
        keyspace_name,
        signature,
    })
}

fn parse_drop_index_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropIndexStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, index_name) = pop_keyspace_object_name(cql, iter)?;
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
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let keyspace_name = pop_identifier(cql, iter)?;
    Ok(DropKeyspaceStatement {
        keyspace_name,
        if_exists,
    })
}

fn parse_drop_materialized_view_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropMaterializedViewStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, view_name) = pop_keyspace_object_name(cql, iter)?;
    Ok(DropMaterializedViewStatement {
        view_name,
        if_exists,
        keyspace_name,
    })
}

fn parse_drop_role_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropRoleStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let role_name = pop_identifier(cql, iter)?;
    Ok(DropRoleStatement {
        role_name,
        if_exists,
    })
}

fn parse_drop_table_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropTableStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, table_name) = pop_keyspace_object_name(cql, iter)?;
    Ok(DropTableStatement {
        alias: None,
        table_name,
        if_exists,
        keyspace_name,
    })
}

fn parse_drop_trigger_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropTriggerStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let trigger_name = pop_identifier(cql, iter)?;
    pop_next_match(iter, OnKeyword)?;
    let (keyspace_name, table_name) = pop_keyspace_object_name(cql, iter)?;
    Ok(DropTriggerStatement {
        trigger_name,
        table_name,
        if_exists,
        keyspace_name,
    })
}

fn parse_drop_type_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropTypeStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, type_name) = pop_keyspace_object_name(cql, iter)?;
    Ok(DropTypeStatement {
        type_name,
        if_exists,
        keyspace_name,
    })
}

fn parse_drop_user_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropUserStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let user_name = pop_identifier(cql, iter)?;
    Ok(DropUserStatement {
        user_name,
        if_exists,
    })
}

/// For function type signatures of `DROP AGGREGATE` and `DROP FUNCTION` statements.
/// Pops a parentheses-enclosed and comma-seperated list of CqlDataType such as
/// `(text, int, frozen<someUDT>)`.
fn pop_function_type_signature(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<Option<Vec<CqlDataType>>> {
    Ok(match pop_next_if(iter, LeftParenthesis) {
        None => None,
        Some(_) => {
            let mut result = Vec::new();
            loop {
                result.push(pop_cql_data_type(cql, iter)?);
                if pop_next_if(iter, Comma).is_none() {
                    break;
                }
            }
            pop_next_match(iter, RightParenthesis)?;
            if result.is_empty() {
                todo!("parse error");
            }
            Some(result)
        }
    })
}
