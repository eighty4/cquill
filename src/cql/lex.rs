use crate::cql::lex::TokenName::*;

#[derive(Debug, PartialEq)]
pub(crate) enum TokenName {
    LeftCurvedBracket,
    RightCurvedBracket,
    LeftRoundBracket,
    RightRoundBracket,
    LeftSquareBracket,
    RightSquareBracket,
    Comma,
    Semicolon,
    Colon,
    Dot,
    Star,
    Divide,
    Modulus,
    Plus,
    Minus,
    DoubleMinus,
    DoubleQuote,
    SingleQuote,
    Equal,
    LessThan,
    GreaterThan,
    LessThanEqual,
    GreaterThanEqual,

    AccessKeyword, // todo test lex
    AllKeyword,    // todo test lex
    AllowKeyword,  // todo test lex
    AlterKeyword,
    AggregateKeyword, // todo test lex
    AndKeyword,
    ApplyKeyword, // todo test lex
    AsciiKeyword,
    AsKeyword, // todo test lex
    AscKeyword,
    AuthorizeKeyword, // todo test lex
    BatchKeyword,     // todo test lex
    BeginKeyword,     // todo test lex
    BigIntKeyword,
    BlobKeyword,
    BooleanKeyword,
    ByKeyword,
    CalledKeyword, // todo test lex
    CastKeyword,   // todo test lex
    ClusteringKeyword,
    CompactKeyword,  // todo test lex
    ContainsKeyword, // todo test lex
    CountKeyword,    // todo test lex
    CounterKeyword,
    CreateKeyword,
    CustomKeyword, // todo test lex
    DateKeyword,
    DatacentersKeyword, // todo test lex
    DecimalKeyword,
    DefaultKeyword, // todo test lex
    DeleteKeyword,  // todo test lex
    DescKeyword,
    DescribeKeyword, // todo test lex
    DistinctKeyword, // todo test lex
    DoubleKeyword,
    DropKeyword, // todo test lex
    DurationKeyword,
    EntriesKeyword, // todo test lex
    ExecuteKeyword, // todo test lex
    ExistsKeyword,  // todo test lex
    FalseKeyword,
    FilteringKeyword,
    FinalFuncKeyword, // todo test lex
    FloatKeyword,
    FromKeyword,      // todo test lex
    FullKeyword,      // todo test lex
    FunctionKeyword,  // todo test lex
    FunctionsKeyword, // todo test lex
    GrantKeyword,     // todo test lex
    GroupKeyword,     // todo test lex
    HashedKeyword,    // todo test lex
    IfKeyword,        // todo test lex
    InKeyword,        // todo test lex
    IndexKeyword,     // todo test lex
    InetKeyword,
    InitCondKeyword, // todo test lex
    InputKeyword,    // todo test lex
    InsertKeyword,   // todo test lex
    IntKeyword,
    JsonKeyword, // todo test lex
    KeyKeyword,
    KeysKeyword, // todo test lex
    KeyspaceKeyword,
    KeyspacesKeyword,    // todo test lex
    LanguageKeyword,     // todo test lex
    LimitKeyword,        // todo test lex
    ListKeyword,         // todo test lex
    LoginKeyword,        // todo test lex
    MaterializedKeyword, // todo test lex
    MBeanKeyword,        // todo test lex
    MBeansKeyword,       // todo test lex
    ModifyKeyword,       // todo test lex
    NoRecursiveKeyword,  // todo test lex
    NotKeyword,          // todo test lex
    NoSuperUserKeyword,  // todo test lex
    NullKeyword,         // todo test lex
    OfKeyword,           // todo test lex
    OnKeyword,           // todo test lex
    OptionsKeyword,      // todo test lex
    OrKeyword,           // todo test lex
    OrderKeyword,
    PartitionKeyword,   // todo test lex
    PasswordKeyword,    // todo test lex
    PerKeyword,         // todo test lex
    PermissionKeyword,  // todo test lex
    PermissionsKeyword, // todo test lex
    PrimaryKeyword,
    ReplaceKeyword, // todo test lex
    ReplicationKeyword,
    ReturnsKeyword, // todo test lex
    RevokeKeyword,  // todo test lex
    RoleKeyword,    // todo test lex
    RolesKeyword,   // todo test lex
    SelectKeyword,  // todo test lex
    SetKeyword,     // todo test lex
    SFuncKeyword,   // todo test lex
    SmallIntKeyword,
    StaticKeyword,    // todo test lex
    StorageKeyword,   // todo test lex
    STypeKeyword,     // todo test lex
    SumKeyword,       // todo test lex
    SuperUserKeyword, // todo test lex
    TableKeyword,
    TablesKeyword, // todo test lex
    TextKeyword,
    TimeKeyword,
    TimestampKeyword, // todo test lex
    TimeUuidKeyword,
    TinyIntKeyword,
    ToKeyword,      // todo test lex
    TokenKeyword,   // todo test lex
    TriggerKeyword, // todo test lex
    TrueKeyword,
    TruncateKeyword, // todo test lex
    TtlKeyword,      // todo test lex
    UnloggedKeyword, // todo test lex
    UnsetKeyword,    // todo test lex
    UpdateKeyword,   // todo test lex
    UseKeyword,
    UserKeyword,  // todo test lex
    UsersKeyword, // todo test lex
    UsingKeyword, // todo test lex
    UuidKeyword,
    ValuesKeyword, // todo test lex
    VarCharKeyword,
    VarIntKeyword,
    ViewKeyword,  // todo test lex
    WhereKeyword, // todo test lex
    WithKeyword,
    WriteTimeKeyword, // todo test lex

    StringLiteral,
    NumberLiteral,
    Identifier,
}

impl TokenName {
    pub fn match_keyword(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "access" => AccessKeyword,
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
            "cast" => CastKeyword,
            "clustering" => ClusteringKeyword,
            "compact" => CompactKeyword,
            "contains" => ContainsKeyword,
            "count" => CountKeyword,
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
            "entries" => EntriesKeyword,
            "execute" => ExecuteKeyword,
            "exists" => ExistsKeyword,
            "false" => FalseKeyword,
            "filtering" => FilteringKeyword,
            "finalfunc" => FinalFuncKeyword,
            "float" => FloatKeyword,
            "from" => FromKeyword,
            "full" => FullKeyword,
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
            "json" => JsonKeyword,
            "key" => KeyKeyword,
            "keys" => KeysKeyword,
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
            "writetime" => WriteTimeKeyword,
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
                "(" => Some(LeftRoundBracket),
                ")" => Some(RightRoundBracket),
                "." => Some(Dot),
                "=" => Some(Equal),
                ":" => Some(Colon),
                "," => Some(Comma),
                ";" => Some(Semicolon),
                "'" => Some(self.string()),
                &_ => {
                    if c.chars().all(|c| c.is_ascii_digit()) {
                        Some(self.number())
                    } else if c.chars().all(|c| c.is_ascii_alphabetic()) {
                        Some(self.identifier_or_keyword())
                    } else {
                        // todo add error
                        None
                    }
                }
            };
            if let Some(name) = maybe_name {
                self.add_token(name);
            }
            self.current = self.current.next_char();
        }
        Ok(self.tokens)
    }

    fn string(&mut self) -> TokenName {
        loop {
            if let Some(c) = self.peek() {
                if c == "\n" {
                    self.line += 1;
                }
                self.advance();
                if c != "'" {
                    continue;
                }
            }
            break;
        }
        StringLiteral
    }

    fn number(&mut self) -> TokenName {
        loop {
            if let Some(c) = self.peek() {
                if c.chars().all(|c| c.is_ascii_digit()) && self.advance().is_some() {
                    continue;
                }
            }
            break;
        }
        NumberLiteral
    }

    fn identifier_or_keyword(&mut self) -> TokenName {
        loop {
            if let Some(c) = self.peek() {
                if c.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                    && self.advance().is_some()
                {
                    continue;
                }
            }
            break;
        }
        TokenName::match_keyword(self.splice())
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

    fn peek(&self) -> Option<&'a str> {
        let i = self.current.1 + 1;
        if i >= self.cql.len() {
            None
        } else {
            self.char_at(i)
        }
    }

    fn splice(&self) -> &'a str {
        &self.cql[self.current.0..=self.current.1]
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
}
