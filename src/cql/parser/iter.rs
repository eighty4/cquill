use crate::cql::ast::StringView;
use crate::cql::lex::TokenName::{FalseKeyword, StringLiteral, TrueKeyword};
use crate::cql::lex::{Token, TokenName};
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

/// Advances iter for each of input tokens so long as peeking next matches input.
/// If iter is advanced with `next()` and a subsequent `peek()` is None or does not match input,
/// this function will return an error.
///
/// Returns true and advances iterator `nexts.len()` number of times if all tokens match.
/// Returns false if first token peeked does not match `nexts.first()`.
/// Returns error if first token peeked matches and subsequent peeks return None or do not match.
pub fn advance_peek_match(
    iter: &mut Peekable<Iter<Token>>,
    nexts: &[TokenName],
) -> Result<bool, anyhow::Error> {
    let mut advanced = false;
    for maybe_next in nexts {
        match iter.peek() {
            None => todo!("parse error"),
            Some(next) => {
                if next.name == *maybe_next {
                    _ = iter.next();
                    advanced = true;
                } else if advanced {
                    todo!("parse error")
                } else {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

/// Returns true/false whether peeked token matches and Err if peek returns None.
pub fn peek_next_match(
    iter: &mut Peekable<Iter<Token>>,
    next: TokenName,
) -> Result<bool, anyhow::Error> {
    match iter.peek() {
        None => todo!("panic error"),
        Some(peeked) => Ok(peeked.name == next),
    }
}

/// Returns next Token or Err if next returns None.
pub fn pop_next<'a>(iter: &'a mut Peekable<Iter<Token>>) -> Result<&'a Token, anyhow::Error> {
    iter.next().ok_or_else(|| todo!("panic error"))
}

/// Returns next Token if it matches TokenName or Err if next returns None.
pub fn pop_next_if<'a>(iter: &'a mut Peekable<Iter<Token>>, next: TokenName) -> Option<&'a Token> {
    iter.next_if(|t| t.name == next)
}

/// Returns Token if it matches TokenName or Err if next returns None or Token does not match.
pub fn pop_next_match<'a>(
    iter: &'a mut Peekable<Iter<Token>>,
    next: TokenName,
) -> Result<&'a Token, anyhow::Error> {
    match iter.next() {
        None => todo!("panic error"),
        Some(popped) => {
            if popped.name == next {
                Ok(popped)
            } else {
                todo!("panic error {:?}", popped.name)
            }
        }
    }
}

/// Pops and returns bool or Err if next returns None or does not return TrueKeyword or
/// FalseKeyword.
pub fn pop_boolean_literal(iter: &mut Peekable<Iter<Token>>) -> Result<bool, anyhow::Error> {
    match iter.next() {
        None => todo!("panic error"),
        Some(popped) => match &popped.name {
            TrueKeyword => Ok(true),
            FalseKeyword => Ok(false),
            _ => todo!("panic error"),
        },
    }
}

/// Pops and returns StringView or Err if next returns None or does not return StringLiteral.
pub fn pop_string_literal(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> Result<StringView, anyhow::Error> {
    match iter.next() {
        None => todo!("panic error"),
        Some(popped) => match &popped.name {
            StringLiteral(style) => Ok(StringView {
                cql: cql.clone(),
                range: popped.range.clone(),
                style: style.clone(),
            }),
            _ => todo!("panic error"),
        },
    }
}
