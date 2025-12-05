mod create;
mod drop;
mod iter;
mod update;

#[cfg(test)]
mod create_test;

#[cfg(test)]
mod drop_test;

#[cfg(test)]
mod testing;

#[cfg(test)]
mod update_test;

use crate::ast::*;
use crate::lex::*;
use crate::parser::create::parse_create_statement;
use crate::parser::drop::parse_drop_statement;
use crate::parser::iter::pop_next_if;
use crate::parser::update::parse_update_statement;
use TokenName::*;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid cql encountered")]
    InvalidCql,
}

pub type ParseResult<T> = Result<T, ParseError>;

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
            UpdateKeyword => parse_update_statement(cql, iter).map(CqlStatement::Update),
            _ => todo!("parse error {:?}", token.name),
        },
    }
}
