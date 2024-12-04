mod create;
mod drop;
mod iter;

#[cfg(test)]
mod create_test;

#[cfg(test)]
mod drop_test;

#[cfg(test)]
mod testing;

use crate::cql::ast::*;
use crate::cql::lex::*;
use crate::cql::parser::create::parse_create_statement;
use crate::cql::parser::drop::parse_drop_statement;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;
use TokenName::*;
use crate::cql::parser::iter::pop_next_if;

pub type ParseResult<T> = Result<T, anyhow::Error>;

pub fn parse_cql(cql: String) -> ParseResult<Vec<CqlStatement>> {
    let tokens = Tokenizer::new(cql.as_str()).tokenize().unwrap();
    let cql = Arc::new(cql);
    let mut iter = tokens.iter().peekable();
    let mut result = Vec::new();
    while iter.peek().is_some() {
        result.push(parse_statement(&cql, &mut iter)?);
        while pop_next_if(&mut iter, Semicolon).is_some() {}
    }
    if result.is_empty() {
        todo!("parse error")
    } else {
        Ok(result)
    }
}

fn parse_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CqlStatement> {
    match iter.next() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            CreateKeyword => parse_create_statement(cql, iter).map(CqlStatement::Create),
            DropKeyword => parse_drop_statement(cql, iter).map(CqlStatement::Drop),
            _ => todo!("parse error {:?}", token.name),
        },
    }
}
