use crate::ast::{StringStyle, StringView, TokenRange, TokenView};
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

pub fn find_nth_token(cql: &str, nth: usize, s: &str) -> TokenView {
    let b = cql
        .match_indices(s)
        .nth(nth)
        .expect("find nth str in cql to create token view")
        .0;
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
