use std::sync::Arc;

use crate::ast::*;
use crate::lex::*;

#[derive(Debug)]
pub struct Token {
    #[allow(unused)]
    pub line: usize,
    pub name: TokenName,
    pub range: TokenRange,
}

impl Token {
    pub fn new(name: TokenName, line: usize, range: TokenRange) -> Self {
        Self { line, name, range }
    }

    pub fn to_token_view(&self, cql: &Arc<String>) -> TokenView {
        TokenView {
            cql: cql.clone(),
            range: self.range.clone(),
        }
    }
}
