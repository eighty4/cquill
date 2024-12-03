use crate::cql::ast::StringView;
use crate::cql::lex::TokenName::StringLiteral;
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
                todo!("panic error")
            }
        }
    }
}

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
