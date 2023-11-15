use crate::cql::lex::TokenName::*;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub(crate) enum StringStyle {
    DollarSign,
    SingleQuote,
    TripleQuote,
}

#[derive(Debug, PartialEq)]
pub(crate) enum TokenName {
    LeftCurvedBracket,
    RightCurvedBracket,
    LeftParenthesis,
    RightParenthesis,
    LeftSquareBracket,  // todo impl
    RightSquareBracket, // todo impl
    Comma,
    Semicolon,
    Colon,
    Dot,
    Dollar,
    Star,
    Divide,  // todo impl
    Modulus, // todo impl
    Plus,    // todo impl
    Minus,
    DoubleMinus, // todo impl
    DoubleQuote, // todo impl
    SingleQuote,
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanEqual,
    GreaterThanEqual,

    AccessKeyword, // todo test lex
    AddKeyword,
    AllKeyword, // todo test lex
    AllowKeyword,
    AlterKeyword,
    AggregateKeyword,
    AndKeyword,
    ApplyKeyword,
    AsciiKeyword,
    AsKeyword,
    AscKeyword,
    AuthorizeKeyword, // todo test lex
    BatchKeyword,
    BeginKeyword,
    BigIntKeyword,
    BlobKeyword,
    BooleanKeyword,
    ByKeyword,
    CalledKeyword,
    ClusteringKeyword,
    CompactKeyword,
    ContainsKeyword,
    CounterKeyword,
    CreateKeyword,
    CustomKeyword,
    DateKeyword,
    DatacentersKeyword, // todo test lex
    DecimalKeyword,
    DefaultKeyword,
    DeleteKeyword,
    DescKeyword,
    DescribeKeyword, // todo test lex
    DistinctKeyword,
    DoubleKeyword,
    DropKeyword,
    DurationKeyword,
    ExecuteKeyword, // todo test lex
    ExistsKeyword,
    FalseKeyword,
    FilteringKeyword,
    FinalFuncKeyword,
    FloatKeyword,
    FromKeyword,
    FunctionKeyword,
    FunctionsKeyword, // todo test lex
    GrantKeyword,     // todo test lex
    GroupKeyword,
    HashedKeyword, // todo test lex
    IfKeyword,
    InKeyword,
    IndexKeyword,
    InetKeyword,
    InfinityKeyword, // todo test lex
    InitCondKeyword,
    InputKeyword,
    InsertKeyword,
    IntKeyword,
    IntoKeyword,
    JsonKeyword,
    KeyKeyword,
    KeyspaceKeyword,
    KeyspacesKeyword, // todo test lex
    LanguageKeyword,
    LimitKeyword,
    ListKeyword,  // todo test lex
    LoginKeyword, // todo test lex
    MaterializedKeyword,
    MBeanKeyword,       // todo test lex
    MBeansKeyword,      // todo test lex
    ModifyKeyword,      // todo test lex
    NaNKeyword,         // todo test lex
    NoRecursiveKeyword, // todo test lex
    NotKeyword,
    NoSuperUserKeyword, // todo test lex
    NullKeyword,
    OfKeyword, // todo test lex
    OnKeyword, // todo test lex
    OptionsKeyword,
    OrKeyword,
    OrderKeyword,
    PartitionKeyword,
    PasswordKeyword, // todo test lex
    PerKeyword,
    PermissionKeyword,  // todo test lex
    PermissionsKeyword, // todo test lex
    PrimaryKeyword,
    RenameKeyword,
    ReplaceKeyword,
    ReplicationKeyword,
    ReturnsKeyword,
    RevokeKeyword, // todo test lex
    RoleKeyword,   // todo test lex
    RolesKeyword,  // todo test lex
    SelectKeyword,
    SetKeyword,
    SFuncKeyword,
    SmallIntKeyword,
    StaticKeyword, // todo test lex
    StorageKeyword,
    STypeKeyword,
    SumKeyword,       // todo test lex
    SuperUserKeyword, // todo test lex
    TableKeyword,
    TablesKeyword, // todo test lex
    TextKeyword,
    TimeKeyword,
    TimestampKeyword,
    TimeUuidKeyword,
    TinyIntKeyword,
    ToKeyword,
    TokenKeyword,   // todo test lex
    TriggerKeyword, // todo test lex
    TrueKeyword,
    TruncateKeyword,
    TtlKeyword,
    TypeKeyword,
    UnloggedKeyword,
    UnsetKeyword, // todo test lex
    UpdateKeyword,
    UseKeyword,
    UserKeyword,  // todo test lex
    UsersKeyword, // todo test lex
    UsingKeyword,
    UuidKeyword,
    ValuesKeyword,
    VarCharKeyword,
    VarIntKeyword,
    ViewKeyword,
    WhereKeyword,
    WithKeyword,

    UuidLiteral,
    StringLiteral(StringStyle),
    NumberLiteral,
    BlobLiteral,
    Identifier,
}

impl TokenName {
    pub fn match_keyword(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "access" => AccessKeyword,
            "add" => AddKeyword,
            "all" => AllKeyword,
            "allow" => AllowKeyword,
            "alter" => AlterKeyword,
            "aggregate" => AggregateKeyword,
            "and" => AndKeyword,
            "apply" => ApplyKeyword,
            "ascii" => AsciiKeyword,
            "as" => AsKeyword,
            "asc" => AscKeyword,
            "authorize" => AuthorizeKeyword,
            "batch" => BatchKeyword,
            "begin" => BeginKeyword,
            "bigint" => BigIntKeyword,
            "blob" => BlobKeyword,
            "boolean" => BooleanKeyword,
            "by" => ByKeyword,
            "called" => CalledKeyword,
            "clustering" => ClusteringKeyword,
            "compact" => CompactKeyword,
            "contains" => ContainsKeyword,
            "counter" => CounterKeyword,
            "create" => CreateKeyword,
            "custom" => CustomKeyword,
            "date" => DateKeyword,
            "datacenters" => DatacentersKeyword,
            "decimal" => DecimalKeyword,
            "default" => DefaultKeyword,
            "delete" => DeleteKeyword,
            "desc" => DescKeyword,
            "describe" => DescribeKeyword,
            "distinct" => DistinctKeyword,
            "double" => DoubleKeyword,
            "drop" => DropKeyword,
            "duration" => DurationKeyword,
            "execute" => ExecuteKeyword,
            "exists" => ExistsKeyword,
            "false" => FalseKeyword,
            "filtering" => FilteringKeyword,
            "finalfunc" => FinalFuncKeyword,
            "float" => FloatKeyword,
            "from" => FromKeyword,
            "function" => FunctionKeyword,
            "functions" => FunctionsKeyword,
            "grant" => GrantKeyword,
            "group" => GroupKeyword,
            "hashed" => HashedKeyword,
            "if" => IfKeyword,
            "in" => InKeyword,
            "index" => IndexKeyword,
            "inet" => InetKeyword,
            "initcond" => InitCondKeyword,
            "input" => InputKeyword,
            "insert" => InsertKeyword,
            "int" => IntKeyword,
            "into" => IntoKeyword,
            "json" => JsonKeyword,
            "key" => KeyKeyword,
            "keyspace" => KeyspaceKeyword,
            "keyspaces" => KeyspacesKeyword,
            "language" => LanguageKeyword,
            "limit" => LimitKeyword,
            "list" => ListKeyword,
            "login" => LoginKeyword,
            "materialized" => MaterializedKeyword,
            "mbean" => MBeanKeyword,
            "mbeans" => MBeansKeyword,
            "modify" => ModifyKeyword,
            "norecursive" => NoRecursiveKeyword,
            "not" => NotKeyword,
            "nosuperuser" => NoSuperUserKeyword,
            "null" => NullKeyword,
            "of" => OfKeyword,
            "on" => OnKeyword,
            "options" => OptionsKeyword,
            "or" => OrKeyword,
            "order" => OrderKeyword,
            "partition" => PartitionKeyword,
            "password" => PasswordKeyword,
            "per" => PerKeyword,
            "permission" => PermissionKeyword,
            "permissions" => PermissionsKeyword,
            "primary" => PrimaryKeyword,
            "rename" => RenameKeyword,
            "replace" => ReplaceKeyword,
            "replication" => ReplicationKeyword,
            "returns" => ReturnsKeyword,
            "revoke" => RevokeKeyword,
            "role" => RoleKeyword,
            "roles" => RolesKeyword,
            "select" => SelectKeyword,
            "set" => SetKeyword,
            "sfunc" => SFuncKeyword,
            "smallint" => SmallIntKeyword,
            "static" => StaticKeyword,
            "storage" => StorageKeyword,
            "stype" => STypeKeyword,
            "sum" => SumKeyword,
            "superuser" => SuperUserKeyword,
            "table" => TableKeyword,
            "tables" => TablesKeyword,
            "text" => TextKeyword,
            "time" => TimeKeyword,
            "timestamp" => TimestampKeyword,
            "timeuuid" => TimeUuidKeyword,
            "tinyint" => TinyIntKeyword,
            "to" => ToKeyword,
            "token" => TokenKeyword,
            "trigger" => TriggerKeyword,
            "true" => TrueKeyword,
            "truncate" => TruncateKeyword,
            "ttl" => TtlKeyword,
            "type" => TypeKeyword,
            "unlogged" => UnloggedKeyword,
            "unset" => UnsetKeyword,
            "update" => UpdateKeyword,
            "use" => UseKeyword,
            "user" => UserKeyword,
            "users" => UsersKeyword,
            "using" => UsingKeyword,
            "uuid" => UuidKeyword,
            "values" => ValuesKeyword,
            "varchar" => VarCharKeyword,
            "varint" => VarIntKeyword,
            "view" => ViewKeyword,
            "where" => WhereKeyword,
            "with" => WithKeyword,
            &_ => Identifier,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TokenRange(usize, usize);

impl TokenRange {
    fn new(b: usize, e: usize) -> Self {
        Self(b, e)
    }

    pub fn splice(&self, cql: &'static str) -> &'static str {
        &cql[self.0..=self.1]
    }

    pub fn next_char(&self) -> Self {
        Self::new(self.1 + 1, self.1 + 1)
    }

    pub fn extend(&mut self, i: usize) {
        self.1 += i;
    }
}

pub(crate) struct Token {
    pub line: usize,
    pub name: TokenName,
    pub range: TokenRange,
}

impl Token {
    fn new(name: TokenName, line: usize, range: TokenRange) -> Self {
        Self { line, name, range }
    }
}

pub(crate) struct Tokenizer<'a> {
    cql: &'a str,
    current: TokenRange,
    line: usize,
    tokens: Vec<Token>,
}

impl<'a> Tokenizer<'a> {
    pub fn new(cql: &'a str) -> Self {
        Self {
            cql,
            current: TokenRange::new(0, 0),
            line: 0,
            tokens: Vec::new(),
        }
    }

    pub fn tokenize(mut self) -> Result<Vec<Token>, ()> {
        loop {
            if self.current.0 >= self.cql.len() {
                break;
            }
            let c = self.splice();
            let maybe_name = match c {
                " " | "\r" | "\t" | "\n" => {
                    if c == "\n" {
                        self.line += 1;
                    }
                    self.current = self.current.next_char();
                    continue;
                }
                "{" => Some(LeftCurvedBracket),
                "}" => Some(RightCurvedBracket),
                "(" => Some(LeftParenthesis),
                ")" => Some(RightParenthesis),
                "-" => Some(Minus),
                "." => Some(Dot),
                "=" => Some(Equal),
                "!" => {
                    if self.match_next("=") {
                        self.advance();
                        Some(NotEqual)
                    } else {
                        None
                    }
                }
                ":" => Some(Colon),
                "," => Some(Comma),
                ";" => Some(Semicolon),
                "*" => Some(Star),
                "<" => {
                    if self.match_next("=") {
                        self.advance();
                        Some(LessThanEqual)
                    } else {
                        Some(LessThan)
                    }
                }
                ">" => {
                    if self.match_next("=") {
                        self.advance();
                        Some(GreaterThanEqual)
                    } else {
                        Some(GreaterThan)
                    }
                }
                "$" => self.dollar_sign_string(),
                "'" => Some(self.quote_string()),
                &_ => match self.constant_or_identifier_or_keyword() {
                    Ok(name) => Some(name),
                    Err(_) => None,
                },
            };
            if let Some(name) = maybe_name {
                self.add_token(name);
            }
            self.current = self.current.next_char();
        }
        Ok(self.tokens)
    }

    fn quote_string(&mut self) -> TokenName {
        let is_triple_quote = self.match_next("'") && self.match_next_nth(1, "'");
        if is_triple_quote {
            self.advance();
            self.advance();
            loop {
                if let Some(c) = self.peek() {
                    if c == "\n" {
                        self.line += 1;
                    }
                    self.advance();
                    if c == "'" && self.match_next("'") && self.match_next_nth(1, "'") {
                        self.advance();
                        self.advance();
                        break;
                    }
                }
            }
            StringLiteral(StringStyle::TripleQuote)
        } else {
            let mut escaped_single_quote = false;
            loop {
                if let Some(c) = self.peek() {
                    if c == "\n" {
                        self.line += 1;
                    }
                    self.advance();
                    if c == "'" {
                        if escaped_single_quote {
                            escaped_single_quote = false;
                        } else if self.match_next("'") {
                            escaped_single_quote = true;
                        } else {
                            break;
                        }
                    }
                }
            }
            StringLiteral(StringStyle::SingleQuote)
        }
    }

    fn dollar_sign_string(&mut self) -> Option<TokenName> {
        if self.match_next("$") {
            self.advance();
        } else {
            return None;
        }
        loop {
            if let Some(c) = self.peek() {
                if c == "\n" {
                    self.line += 1;
                }
                self.advance();
                if c == "$" && self.match_next("$") {
                    self.advance();
                    break;
                }
            }
        }
        Some(StringLiteral(StringStyle::DollarSign))
    }

    fn constant_or_identifier_or_keyword(&mut self) -> Result<TokenName, ()> {
        if self.splice() == "0" {
            match self.blob() {
                Ok(maybe_name) => {
                    if let Some(name) = maybe_name {
                        return Ok(name);
                    }
                }
                Err(_) => return Err(()),
            };
        }
        let mut only_hex = true;
        let mut only_digit = true;
        let mut has_dash = false;
        let mut has_decimal = false;
        let mut has_underscore = false;
        loop {
            if let Some(s) = self.peek() {
                let mut advance = true;
                for c in s.chars() {
                    advance =
                        advance && (c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.');
                    if advance {
                        if c == '.' {
                            if !only_digit {
                                advance = false;
                                break;
                            }
                            has_decimal = true;
                        } else if c == '-' {
                            has_dash = true;
                        } else if c == '_' {
                            has_underscore = true;
                        } else if !c.is_ascii_digit() {
                            only_digit = false;
                            if !c.is_ascii_hexdigit() {
                                only_hex = false;
                            }
                        }
                    }
                }
                if advance && self.advance().is_some() {
                    continue;
                }
            }
            break;
        }
        let s = self.splice();
        let maybe_uuid = s.len() == 36 && only_hex && !has_decimal && has_dash && !has_underscore;
        if maybe_uuid && Uuid::try_parse_ascii(s.as_bytes()).is_ok() {
            Ok(UuidLiteral)
        } else if only_digit && !has_dash && !has_underscore {
            Ok(NumberLiteral)
        } else if !has_dash && !has_decimal {
            Ok(TokenName::match_keyword(s))
        } else {
            Err(())
        }
    }

    fn blob(&mut self) -> Result<Option<TokenName>, ()> {
        if !self.peek().map_or(false, |c| c.to_ascii_lowercase() == "x") {
            Ok(None)
        } else {
            self.advance();
            let mut hex = true;
            loop {
                if let Some(s) = self.peek() {
                    let mut advance = false;
                    for c in s.chars() {
                        advance = c.is_ascii_alphanumeric();
                        hex = hex && c.is_ascii_hexdigit();
                    }
                    if advance && self.advance().is_some() {
                        continue;
                    }
                }
                break;
            }
            if hex {
                Ok(Some(BlobLiteral))
            } else {
                Err(())
            }
        }
    }

    fn add_token(&mut self, name: TokenName) {
        let token = Token::new(name, self.line, self.current);
        self.tokens.push(token);
    }

    fn advance(&mut self) -> Option<&'a str> {
        let maybe_c = self.char_at(self.current.1 + 1);
        if let Some(c) = maybe_c {
            self.current.extend(c.len());
        }
        maybe_c
    }

    fn char_at(&self, i: usize) -> Option<&'a str> {
        assert!(i < self.cql.len());
        assert!(self.cql.is_char_boundary(i));
        let mut e = i + 1;
        loop {
            if e > self.cql.len() {
                return None;
            } else if self.cql.is_char_boundary(e) {
                break;
            } else {
                e += 1;
            }
        }
        Some(&self.cql[i..e])
    }

    fn match_next(&self, exp: &'a str) -> bool {
        self.match_next_nth(0, exp)
    }

    fn match_next_nth(&self, nth: usize, exp: &'a str) -> bool {
        self.peek_nth(nth).map_or(false, |s| s == exp)
    }

    fn peek(&self) -> Option<&'a str> {
        self.peek_nth(0)
    }

    fn peek_nth(&self, nth: usize) -> Option<&'a str> {
        let i = self.current.1 + 1 + nth;
        if i >= self.cql.len() {
            None
        } else {
            self.char_at(i)
        }
    }

    fn splice(&self) -> &'a str {
        &self.cql[self.current.0..=self.current.1]
    }
}
