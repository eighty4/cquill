use std::{iter::Peekable, slice::Iter, sync::Arc};

use crate::{ParseResult, ast::BatchStatement, lex::Token};

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/dml.html#batch_statement
pub fn parse_batch_statement(
    _cql: &Arc<String>,
    _iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<BatchStatement> {
    todo!();
}
