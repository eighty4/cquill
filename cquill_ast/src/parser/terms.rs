use std::{iter::Peekable, slice::Iter, sync::Arc};

use crate::{ParseError, ParseResult, ast::BindMarker, lex::{Token, TokenName::*}, parser::iter::pop_identifier};

pub fn parse_bind_marker(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<BindMarker> {
    match iter.next() {
        Some(Token { name: Colon, ..}) => Ok(BindMarker::Named(pop_identifier(cql, iter)?)),
        Some(Token { name: QuestionMark, ..}) => Ok(BindMarker::Anonymous),
        _ => Err(ParseError::InvalidCql("expected a bind marker".into())),
    }
}
