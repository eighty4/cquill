use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum StringStyle {
    DollarSign,
    SingleQuote,
    TripleQuote,
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct StringView {
    pub cql: Arc<String>,
    pub range: TokenRange,
    pub style: StringStyle,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TokenRange(usize, usize);

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct TokenView {
    pub cql: Arc<String>,
    pub range: TokenRange,
}

impl TokenView {
    pub fn value(&self) -> String {
        String::from(&self.cql[self.range.begin()..=self.range.end()])
    }
}

impl Display for TokenView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}

impl TokenRange {
    pub fn new(b: usize, e: usize) -> Self {
        Self(b, e)
    }

    pub fn begin(&self) -> usize {
        self.0
    }

    pub fn end(&self) -> usize {
        self.1
    }

    pub fn splice(&self, cql: &'static str) -> &'static str {
        &cql[self.0..=self.1]
    }

    pub fn next_char(&mut self) {
        self.0 = self.1 + 1;
        self.1 = self.0;
    }

    pub fn extend(&mut self, i: usize) {
        self.1 += i;
    }
}
