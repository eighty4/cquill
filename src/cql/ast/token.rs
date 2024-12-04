use std::fmt::{Display, Formatter};
use std::sync::Arc;

// todo support vector
#[derive(Debug, PartialEq)]
pub enum CqlDataType {
    CollectionType(CqlCollectionType),
    /// A custom type implemented in Java.
    CustomType(StringView),
    /// Composite of native type and user defined types.
    ValueType(CqlValueType),
    Tuple(Box<CqlDataType>, Box<CqlDataType>),
}

#[derive(Debug, PartialEq)]
pub enum CqlValueType {
    NativeType(CqlNativeType),
    UserDefinedType(CqlUserDefinedType),
}

// todo handle Frozen<T> at CqlDataType level
#[derive(Debug, PartialEq)]
pub enum CqlUserDefinedType {
    Frozen(TokenView),
    Unfrozen(TokenView),
}

#[derive(Debug, PartialEq)]
pub enum CqlNativeType {
    Ascii,
    BigInt,
    Blob,
    Boolean,
    Counter,
    Date,
    Decimal,
    Double,
    Duration,
    Float,
    INet,
    Int,
    SmallInt,
    Text,
    Time,
    Timestamp,
    TimeUuid,
    TinyInt,
    Uuid,
    VarChar,
    VarInt,
}

// todo should CqlValueType for collection generics be CqlDataType
// todo are collection generics optional
#[derive(Debug, PartialEq)]
pub enum CqlCollectionType {
    List(CqlValueType),
    Map(CqlValueType, CqlValueType),
    Set(CqlValueType),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum StringStyle {
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
pub struct TokenRange(usize, usize);

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
