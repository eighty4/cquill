use std::{iter::Peekable, slice::Iter, sync::Arc};

use crate::{ParseResult, ast::DeleteStatement, lex::Token};

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/dml.html#delete_statement
pub fn parse_delete_statement(
    _cql: &Arc<String>,
    _iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DeleteStatement> {
    todo!();
}
