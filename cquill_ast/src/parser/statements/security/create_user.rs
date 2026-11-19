use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

pub fn parse_create_user_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateUserStatement> {
    let if_not_exists = pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let user_name = pop_identifier(cql, iter)?;
    let password = match iter.peek() {
        None => None,
        Some(peeked) => match peeked.name {
            WithKeyword => {
                _ = iter.next();
                let popped = pop_next(iter)?;
                Some(match popped.name {
                    HashedKeyword => {
                        pop_next_match(iter, PasswordKeyword)?;
                        AuthPassword::Hashed(pop_string_literal(cql, iter)?)
                    }
                    PasswordKeyword => AuthPassword::PlainText(pop_string_literal(cql, iter)?),
                    _ => todo!("parse error"),
                })
            }
            _ => None,
        },
    };
    let user_status = match iter.peek() {
        None => None,
        Some(peeked) => {
            let status = match peeked.name {
                NoSuperUserKeyword => Some(CreateUserStatus::NoSuperuser),
                SuperUserKeyword => Some(CreateUserStatus::Superuser),
                Semicolon => None,
                _ => todo!("parse error"),
            };
            if status.is_some() {
                _ = iter.next();
            }
            status
        }
    };
    Ok(CreateUserStatement {
        user_name,
        if_not_exists,
        password,
        user_status,
    })
}
