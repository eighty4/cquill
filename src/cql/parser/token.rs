use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;
use crate::cql::ast::TokenView;
use crate::cql::lex::Token;
use crate::cql::lex::TokenName::{Dot, Identifier};
use crate::cql::parser::iter::pop_next;

pub fn create_view(cql: &Arc<String>, token: &Token) -> TokenView {
    TokenView {
        cql: cql.clone(),
        range: token.range.clone(),
    }
}

pub fn parse_object_identifiers(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> Result<(Option<TokenView>, TokenView), anyhow::Error> {
    let object_or_keyspace = create_view(cql, pop_next(iter, Identifier)?);
    match iter.peek().map(|t| &t.name) {
        Some(Dot) => {
            _ = iter.next();
            Ok((
                Some(object_or_keyspace),
                create_view(cql, pop_next(iter, Identifier)?),
            ))
        }
        _ => Ok((None, object_or_keyspace)),
    }
}
