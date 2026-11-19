use crate::ast::*;
use crate::lex::Token;
use crate::parser::ParseResult;

use std::{iter::Peekable, slice::Iter, sync::Arc};

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/dml.html#insert-statement
pub fn parse_insert_statement(
    _cql: &Arc<String>,
    _iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<InsertStatement> {
    todo!();
}
