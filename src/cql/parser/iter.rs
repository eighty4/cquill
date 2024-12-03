use std::iter::Peekable;
use std::slice::Iter;
use crate::cql::lex::{Token, TokenName};

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

pub fn pop_next<'a>(
    iter: &'a mut Peekable<Iter<Token>>,
    next: TokenName,
) -> Result<&'a Token, anyhow::Error> {
    match iter.next() {
        None => todo!("panic error"),
        Some(token) => {
            if token.name == next {
                Ok(token)
            } else {
                todo!("panic error")
            }
        }
    }
}
