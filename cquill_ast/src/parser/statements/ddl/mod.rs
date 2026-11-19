mod create_keyspace;
mod create_table;

pub use create_keyspace::*;
pub use create_table::*;

use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;

use std::{iter::Peekable, slice::Iter, sync::Arc};

#[cfg(test)]
mod create_keyspace_test;

#[cfg(test)]
mod create_table_test;

#[cfg(test)]
mod drop_keyspace_test;

#[cfg(test)]
mod drop_table_test;

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#drop-keyspace-statement
pub fn parse_drop_keyspace_statement(
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

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#drop-table-statement
pub fn parse_drop_table_statement(
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

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#use-statement
pub fn parse_use_keyspace_statement(
    _cql: &Arc<String>,
    _iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<UseKeyspaceStatement> {
    todo!();
}
