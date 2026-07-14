use std::{iter::Peekable, slice::Iter, sync::Arc};

use crate::lex::TokenName::*;
use crate::parser::iter::pop_keyspace_object_name;
use crate::{ParseResult, ast::TruncateTableStatement, lex::Token};

pub fn parse_truncate_table_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<TruncateTableStatement> {
    match iter.peek() {
        Some(Token { name: Identifier, .. }) => {},
        Some(Token { name: TableKeyword, .. }) => _ = iter.next(),
        _ => panic!(),
    };
    match pop_keyspace_object_name(cql, iter) {
        Ok(_) => {},
        Err(_) => panic!(),
    };
    todo!();
}
