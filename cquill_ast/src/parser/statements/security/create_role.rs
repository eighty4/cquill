use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

pub fn parse_create_role_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateRoleStatement> {
    let if_not_exists = pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let role_name = pop_identifier(cql, iter)?;
    let attributes = if peek_next_match(iter, WithKeyword)? {
        _ = iter.next();
        let mut attributes = Vec::new();
        loop {
            attributes.push(match iter.next() {
                None => todo!("parse error"),
                Some(popped) => match popped.name {
                    SuperUserKeyword => {
                        pop_next_match(iter, Equal)?;
                        RoleConfigAttribute::Superuser(pop_boolean_literal(iter)?)
                    }
                    LoginKeyword => {
                        pop_next_match(iter, Equal)?;
                        RoleConfigAttribute::Login(pop_boolean_literal(iter)?)
                    }
                    PasswordKeyword => {
                        pop_next_match(iter, Equal)?;
                        RoleConfigAttribute::Password(AuthPassword::PlainText(pop_string_literal(
                            cql, iter,
                        )?))
                    }
                    HashedKeyword => {
                        pop_next_match(iter, PasswordKeyword)?;
                        pop_next_match(iter, Equal)?;
                        RoleConfigAttribute::Password(AuthPassword::Hashed(pop_string_literal(
                            cql, iter,
                        )?))
                    }
                    OptionsKeyword => {
                        pop_next_match(iter, Equal)?;
                        RoleConfigAttribute::Options(pop_hacky_map_literal(cql, iter)?)
                    }
                    AccessKeyword => {
                        pop_next_match(iter, ToKeyword)?;
                        if peek_next_match(iter, AllKeyword)? {
                            _ = iter.next();
                            pop_next_match(iter, DatacentersKeyword)?;
                            RoleConfigAttribute::Access(Datacenters::All)
                        } else {
                            pop_next_match(iter, DatacentersKeyword)?;
                            pop_next_match(iter, LeftCurvedBracket)?;
                            let mut datacenters = Vec::new();
                            loop {
                                datacenters.push(pop_string_literal(cql, iter)?);
                                match pop_next(iter)?.name {
                                    Comma => continue,
                                    RightCurvedBracket => break,
                                    _ => todo!("parse error"),
                                }
                            }
                            RoleConfigAttribute::Access(Datacenters::Explicit(datacenters))
                        }
                    }
                    _ => todo!("parse error {:?}", popped.name),
                },
            });
            if pop_next_if(iter, AndKeyword).is_none() {
                break;
            }
        }
        Some(attributes)
    } else {
        None
    };
    Ok(CreateRoleStatement {
        if_not_exists,
        role_name,
        attributes,
    })
}
