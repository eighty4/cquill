use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

pub fn parse_drop_user_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropUserStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let user_name = pop_identifier(cql, iter)?;
    Ok(DropUserStatement {
        user_name,
        if_exists,
    })
}
