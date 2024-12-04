use crate::cql::ast::CqlUserDefinedType::Unfrozen;
use crate::cql::ast::{
    CqlDataType, CqlDataType::*, CqlNativeType::*, CqlValueType::*, StringView, TokenView,
};
use crate::cql::lex::TokenName::*;
use crate::cql::lex::{Token, TokenName};
use crate::cql::ParseResult;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

/// Returns true/false whether peeked token matches and Err if peek returns None.
pub fn peek_next_match(
    iter: &mut Peekable<Iter<Token>>,
    next: TokenName,
) -> Result<bool, anyhow::Error> {
    match iter.peek() {
        None => todo!("parse error"),
        Some(peeked) => Ok(peeked.name == next),
    }
}

/// Returns next Token or Err if next returns None.
pub fn pop_next<'a>(iter: &'a mut Peekable<Iter<Token>>) -> Result<&'a Token, anyhow::Error> {
    iter.next().ok_or_else(|| todo!("parse error"))
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
        None => todo!("parse error"),
        Some(popped) => {
            if popped.name == next {
                Ok(popped)
            } else {
                todo!("parse error expected={:?} actual={:?}", next, popped.name)
            }
        }
    }
}

/// Advances iter for each of input tokens so long as peeking next matches input.
/// If iter is advanced with `next()` and a subsequent `peek()` is None or does not match input,
/// this function will return an error.
///
/// Returns true and advances iterator `nexts.len()` number of times if all tokens match.
/// Returns false if first token peeked does not match `nexts.first()`.
/// Returns error if first token peeked matches and subsequent peeks return None or do not match.
pub fn pop_sequence(
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

pub fn pop_identifier(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<TokenView> {
    let popped = pop_next_match(iter, Identifier)?;
    Ok(TokenView {
        cql: cql.clone(),
        range: popped.range.clone(),
    })
}

/// Pops and returns bool or Err if next returns None or does not return TrueKeyword or
/// FalseKeyword.
pub fn pop_boolean_literal(iter: &mut Peekable<Iter<Token>>) -> Result<bool, anyhow::Error> {
    match iter.next() {
        None => todo!("parse error"),
        Some(popped) => match &popped.name {
            TrueKeyword => Ok(true),
            FalseKeyword => Ok(false),
            _ => todo!("parse error"),
        },
    }
}

pub fn pop_cql_data_type(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> Result<CqlDataType, anyhow::Error> {
    Ok(match iter.next() {
        None => todo!("parse error"),
        Some(popped) => match popped.name {
            AsciiKeyword => ValueType(NativeType(Ascii)),
            BigIntKeyword => ValueType(NativeType(BigInt)),
            BlobKeyword => ValueType(NativeType(Blob)),
            BooleanKeyword => ValueType(NativeType(Boolean)),
            CounterKeyword => ValueType(NativeType(Counter)),
            DateKeyword => ValueType(NativeType(Date)),
            DecimalKeyword => ValueType(NativeType(Decimal)),
            DoubleKeyword => ValueType(NativeType(Double)),
            DurationKeyword => ValueType(NativeType(Duration)),
            FloatKeyword => ValueType(NativeType(Float)),
            InetKeyword => ValueType(NativeType(INet)),
            IntKeyword => ValueType(NativeType(Int)),
            SmallIntKeyword => ValueType(NativeType(SmallInt)),
            TextKeyword => ValueType(NativeType(Text)),
            TimeKeyword => ValueType(NativeType(Time)),
            TimestampKeyword => ValueType(NativeType(Timestamp)),
            TimeUuidKeyword => ValueType(NativeType(TimeUuid)),
            TinyIntKeyword => ValueType(NativeType(TinyInt)),
            UuidKeyword => ValueType(NativeType(Uuid)),
            VarCharKeyword => ValueType(NativeType(VarChar)),
            VarIntKeyword => ValueType(NativeType(VarInt)),
            Identifier => ValueType(UserDefinedType(Unfrozen(popped.to_token_view(cql)))),
            _ => todo!("parse error"),
        },
    })
}

pub fn pop_keyspace_object_name(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> Result<(Option<TokenView>, TokenView), anyhow::Error> {
    let object_or_keyspace = pop_identifier(cql, iter)?;
    Ok(match pop_next_if(iter, Dot) {
        Some(_) => (Some(object_or_keyspace), pop_identifier(cql, iter)?),
        None => (None, object_or_keyspace),
    })
}

/// Pops and returns StringView or Err if next returns None or does not return StringLiteral.
pub fn pop_string_literal(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> Result<StringView, anyhow::Error> {
    match iter.next() {
        None => todo!("parse error"),
        Some(popped) => match &popped.name {
            StringLiteral(style) => Ok(StringView {
                cql: cql.clone(),
                range: popped.range.clone(),
                style: style.clone(),
            }),
            _ => todo!("parse error"),
        },
    }
}
