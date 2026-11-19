use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/triggers.html#create-trigger-statement
pub fn parse_create_trigger_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateTriggerStatement> {
    let if_not_exists = pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let trigger_name = pop_identifier(cql, iter)?;
    pop_next_match(iter, OnKeyword)?;
    let (keyspace_name, table_name) = pop_keyspace_object_name(cql, iter)?;
    pop_next_match(iter, UsingKeyword)?;
    let index_classpath = pop_string_literal(cql, iter)?;
    Ok(CreateTriggerStatement {
        if_not_exists,
        trigger_name,
        table_name,
        keyspace_name,
        index_classpath,
    })
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/triggers.html#drop-trigger-statement
pub fn parse_drop_trigger_statement(
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
