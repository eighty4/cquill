use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/mvs.html#create-materialized-view-statement
pub fn parse_create_materialized_view_statement(
    _cql: &Arc<String>,
    _iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateMaterializedViewStatement> {
    unimplemented!();
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/mvs.html#alter-materialized-view-statement
#[allow(unused)]
pub fn parse_alter_materialized_view_statement() {
    todo!();
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/mvs.html#drop-materialized-view-statement
pub fn parse_drop_materialized_view_statement(
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
