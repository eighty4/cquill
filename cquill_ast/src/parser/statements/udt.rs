use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/types.html#creating-a-udt
// todo fields with collections, collections with generics and UDTs
pub fn parse_create_type_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateTypeStatement> {
    let if_not_exists = pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let (keyspace_name, type_name) = pop_keyspace_object_name(cql, iter)?;
    let fields = pop_named_data_types_map(cql, iter)?;
    Ok(CreateTypeStatement {
        keyspace_name,
        type_name,
        if_not_exists,
        fields,
    })
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/types.html#altering-a-udt
#[allow(unused)]
pub fn parse_alter_type_statement() {
    todo!();
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/types.html#dropping-a-udt
pub fn parse_drop_type_statement(
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
