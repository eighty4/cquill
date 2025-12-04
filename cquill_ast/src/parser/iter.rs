use crate::ast::CqlCollectionType::List;
use crate::ast::{
    CqlDataType, CqlDataType::*, CqlNativeType, CqlNativeType::*, CqlValueType::*, StringView,
    TokenView,
};
use crate::lex::TokenName::*;
use crate::lex::{Token, TokenName};
use crate::ParseResult;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

pub fn advance_until(iter: &mut Peekable<Iter<Token>>, until: TokenName) {
    loop {
        match iter.next() {
            None => break,
            Some(popped) => {
                if popped.name == until {
                    break;
                }
            }
        }
    }
}

/// Returns true/false whether peeked token matches and Err if peek returns None.
pub fn peek_next_match(iter: &mut Peekable<Iter<Token>>, next: TokenName) -> ParseResult<bool> {
    match iter.peek() {
        None => todo!("parse error"),
        Some(peeked) => Ok(peeked.name == next),
    }
}

/// Returns next Token or Err if next returns None.
pub fn pop_next<'a>(iter: &'a mut Peekable<Iter<Token>>) -> ParseResult<&'a Token> {
    iter.next().ok_or_else(|| todo!("parse error"))
}

/// Returns next Some(Token) if it matches TokenName or None if next returns None.
pub fn pop_next_if<'a>(iter: &'a mut Peekable<Iter<Token>>, next: TokenName) -> Option<&'a Token> {
    iter.next_if(|t| t.name == next)
}

/// Returns Token if it matches TokenName or Err if next returns None or Token does not match.
pub fn pop_next_match<'a>(
    iter: &'a mut Peekable<Iter<Token>>,
    next: TokenName,
) -> ParseResult<&'a Token> {
    match iter.next() {
        None => todo!("parse error"),
        Some(popped) => {
            if popped.name == next {
                Ok(popped)
            } else {
                todo!("parse error")
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
pub fn pop_sequence(iter: &mut Peekable<Iter<Token>>, nexts: &[TokenName]) -> ParseResult<bool> {
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
pub fn pop_boolean_literal(iter: &mut Peekable<Iter<Token>>) -> ParseResult<bool> {
    match iter.next() {
        None => todo!("parse error"),
        Some(popped) => match &popped.name {
            TrueKeyword => Ok(true),
            FalseKeyword => Ok(false),
            _ => todo!("parse error"),
        },
    }
}

pub fn pop_number_literal(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<TokenView> {
    match iter.next() {
        None => todo!("parse error"),
        Some(popped) => match &popped.name {
            NumberLiteral => Ok(TokenView::new(cql, popped)),
            _ => todo!("parse error"),
        },
    }
}

// todo collections, custom types and tuples
pub fn pop_cql_data_type(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CqlDataType> {
    Ok(match iter.next() {
        None => todo!("parse error"),
        Some(popped) => match maybe_cql_native_type(popped) {
            Some(native_type) => ValueType(NativeType(native_type)),
            None => match popped.name {
                ListKeyword => {
                    pop_next_match(iter, LessThan)?;
                    let list_of = match maybe_cql_native_type(pop_next(iter)?) {
                        None => todo!("frozen<> and udt"),
                        Some(native_type) => NativeType(native_type),
                    };
                    pop_next_match(iter, GreaterThan)?;
                    CollectionType(List(list_of))
                }
                FrozenKeyword => Frozen({
                    pop_next_match(iter, LessThan)?;
                    let generic_type = Box::new(pop_cql_data_type(cql, iter)?);
                    pop_next_match(iter, GreaterThan)?;
                    generic_type
                }),
                Identifier => ValueType(UserDefinedType(popped.to_token_view(cql))),
                _ => todo!("parse error"),
            },
        },
    })
}

fn maybe_cql_native_type(token: &Token) -> Option<CqlNativeType> {
    Some(match token.name {
        AsciiKeyword => Ascii,
        BigIntKeyword => BigInt,
        BlobKeyword => Blob,
        BooleanKeyword => Boolean,
        CounterKeyword => Counter,
        DateKeyword => Date,
        DecimalKeyword => Decimal,
        DoubleKeyword => Double,
        DurationKeyword => Duration,
        FloatKeyword => Float,
        InetKeyword => INet,
        IntKeyword => Int,
        SmallIntKeyword => SmallInt,
        TextKeyword => Text,
        TimeKeyword => Time,
        TimestampKeyword => Timestamp,
        TimeUuidKeyword => TimeUuid,
        TinyIntKeyword => TinyInt,
        UuidKeyword => Uuid,
        VarCharKeyword => VarChar,
        VarIntKeyword => VarInt,
        _ => return None,
    })
}

pub fn pop_keyspace_object_name(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<(Option<TokenView>, TokenView)> {
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
) -> ParseResult<StringView> {
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

/// For function type signatures of `DROP AGGREGATE` and `DROP FUNCTION` statements.
/// Pops a parentheses-enclosed and comma-seperated list of CqlDataType such as
/// `(text, int, frozen<someUDT>)`.
// todo determine if `DROP AGGREGATE agg()` with empty parentheses is valid
pub fn pop_if_function_signature(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<Option<Vec<CqlDataType>>> {
    Ok(match pop_next_if(iter, LeftParenthesis) {
        None => None,
        Some(_) => {
            let mut result = Vec::new();
            loop {
                result.push(pop_cql_data_type(cql, iter)?);
                if pop_next_if(iter, Comma).is_none() {
                    break;
                }
            }
            pop_next_match(iter, RightParenthesis)?;
            if result.is_empty() {
                todo!("parse error");
            }
            Some(result)
        }
    })
}

/// For aggregate type signature of `CREATE AGGREGATE agg(int)`.
// todo verify `CREATE AGGREGATE` only supports a single argument
pub fn pop_aggregate_signature(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CqlDataType> {
    pop_next_match(iter, LeftParenthesis)?;
    let data_type = pop_cql_data_type(cql, iter)?;
    pop_next_match(iter, RightParenthesis)?;
    Ok(data_type)
}

pub fn pop_comma_separated_identifiers(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<Vec<TokenView>> {
    let mut identifiers = Vec::new();
    loop {
        identifiers.push(pop_identifier(cql, iter)?);
        if pop_next_if(iter, Comma).is_none() {
            break;
        }
    }
    Ok(identifiers)
}
