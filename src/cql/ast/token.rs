use std::fmt::{Display, Formatter};
use std::sync::Arc;

// todo support vector
// todo collections cannot be nested
//  can a tuple be nested in a collection such as `map<text, tuple<int, text>>`?
// todo frozen can be around a collection such as `frozen<map<text, someUDT>>`
// todo frozen can be around a UDT such as frozen<someUDT>
// todo frozen can be in a collection such as `map<text, frozen<someUDT>>`
#[derive(Debug, Eq, Hash, PartialEq)]
pub enum CqlDataType {
    CollectionType(CqlCollectionType),
    /// A custom type implemented in Java.
    CustomType(StringView),
    Frozen(Box<CqlDataType>),
    /// Composite of native type and user defined types.
    ValueType(CqlValueType),
    Tuple(Box<CqlDataType>, Box<CqlDataType>),
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum CqlValueType {
    NativeType(CqlNativeType),
    UserDefinedType(TokenView),
}

#[derive(Debug, Eq, Hash, PartialEq)]
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
#[derive(Debug, Eq, Hash, PartialEq)]
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

impl StringView {
    pub fn value(&self) -> String {
        let offset = match self.style {
            StringStyle::DollarSign => 2,
            StringStyle::SingleQuote => 1,
            StringStyle::TripleQuote => 3,
        };
        let b = self.range.begin() + offset;
        let e = self.range.end() - offset;
        String::from(&self.cql[b..=e])
    }
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
