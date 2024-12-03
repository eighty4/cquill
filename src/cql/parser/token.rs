use crate::cql::ast::TokenView;
use crate::cql::lex::Token;
use crate::cql::lex::TokenName::{Dot, Identifier};
use crate::cql::parser::iter::pop_next_match;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

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
    let object_or_keyspace = create_view(cql, pop_next_match(iter, Identifier)?);
    match iter.peek().map(|t| &t.name) {
        Some(Dot) => {
            _ = iter.next();
            Ok((
                Some(object_or_keyspace),
                create_view(cql, pop_next_match(iter, Identifier)?),
            ))
        }
        _ => Ok((None, object_or_keyspace)),
    }
}

#[cfg(test)]
pub mod testing {
    use crate::cql::ast::{StringStyle, StringView, TokenRange, TokenView};
    use std::sync::Arc;

    pub fn find_string_literal(cql: &str, s: &str) -> StringView {
        let b = cql
            .find(s)
            .expect("find string literal in cql to create string view");
        let e = b + s.len() - 1;
        let range = TokenRange::new(b, e);
        let style = match &cql[b..b + 1] {
            "$" => StringStyle::DollarSign,
            "'" => {
                if &cql[b..b + 3] == "'''" {
                    StringStyle::TripleQuote
                } else {
                    StringStyle::SingleQuote
                }
            }
            _ => panic!(),
        };
        StringView {
            cql: Arc::new(String::from(cql)),
            range,
            style,
        }
    }

    pub fn find_token(cql: &str, s: &str) -> TokenView {
        let b = cql.find(s).expect("find str in cql to create token view");
        let e = b + s.len() - 1;
        let range = TokenRange::new(b, e);
        TokenView {
            cql: Arc::new(String::from(cql)),
            range,
        }
    }

    pub fn rfind_token(cql: &str, s: &str) -> TokenView {
        let b = cql.rfind(s).expect("rfind str in cql to create token view");
        let e = b + s.len() - 1;
        let range = TokenRange::new(b, e);
        TokenView {
            cql: Arc::new(String::from(cql)),
            range,
        }
    }
}
