use crate::ast::{StringStyle, TokenRange, TokenView};
use crate::lex::TokenName::*;
use anyhow::anyhow;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub enum TokenName {
    LeftCurvedBracket,
    RightCurvedBracket,
    LeftParenthesis,
    RightParenthesis,
    LeftSquareBracket,  // todo test lex
    RightSquareBracket, // todo test lex
    Comma,
    Semicolon,
    Colon,
    Dot,
    Star,
    #[allow(unused)]
    Divide, // todo impl
    #[allow(unused)]
    Modulus, // todo impl
    #[allow(unused)]
    Plus, // todo impl
    Minus,
    #[allow(unused)]
    DoubleMinus, // todo impl
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanEqual,
    GreaterThanEqual,

    AccessKeyword,
    AddKeyword,
    AllKeyword,
    AllowKeyword,
    AlterKeyword,
    AggregateKeyword,
    AndKeyword,
    ApplyKeyword,
    AsciiKeyword,
    AsKeyword,
    AscKeyword,
    AuthorizeKeyword,
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
    DatacentersKeyword,
    DecimalKeyword,
    DefaultKeyword,
    DeleteKeyword,
    DescKeyword,
    DescribeKeyword,
    DistinctKeyword,
    DoubleKeyword,
    DropKeyword,
    DurationKeyword,
    ExecuteKeyword,
    ExistsKeyword,
    FalseKeyword,
    FilteringKeyword,
    FinalFuncKeyword,
    FloatKeyword,
    FromKeyword,
    FrozenKeyword,
    FunctionKeyword,
    FunctionsKeyword,
    GrantKeyword,
    GroupKeyword,
    HashedKeyword,
    IfKeyword,
    InKeyword,
    IndexKeyword,
    InetKeyword,
    #[allow(unused)]
    InfinityKeyword, // todo test lex
    InitCondKeyword,
    InputKeyword,
    InsertKeyword,
    IntKeyword,
    IntoKeyword,
    JsonKeyword,
    KeyKeyword,
    KeyspaceKeyword,
    KeyspacesKeyword,
    LanguageKeyword,
    LimitKeyword,
    ListKeyword,
    LoginKeyword,
    MaterializedKeyword,
    MBeanKeyword,
    MBeansKeyword,
    ModifyKeyword,
    #[allow(unused)]
    NaNKeyword, // todo test lex
    NoRecursiveKeyword,
    NotKeyword,
    NoSuperUserKeyword,
    NullKeyword,
    OfKeyword,
    OnKeyword,
    OptionsKeyword,
    OrKeyword,
    OrderKeyword,
    PartitionKeyword,
    PasswordKeyword,
    PerKeyword,
    PermissionKeyword,
    PermissionsKeyword,
    PrimaryKeyword,
    RenameKeyword,
    ReplaceKeyword,
    ReplicationKeyword,
    ReturnsKeyword,
    RevokeKeyword,
    RoleKeyword,
    RolesKeyword,
    SelectKeyword,
    SetKeyword,
    SFuncKeyword,
    SmallIntKeyword,
    StaticKeyword, // todo test lex
    StorageKeyword,
    STypeKeyword,
    SumKeyword, // todo test lex
    SuperUserKeyword,
    TableKeyword,
    TablesKeyword, // todo test lex
    TextKeyword,
    TimeKeyword,
    TimestampKeyword,
    TimeUuidKeyword,
    TinyIntKeyword,
    ToKeyword,
    TokenKeyword, // todo test lex
    TriggerKeyword,
    TrueKeyword,
    TruncateKeyword,
    TtlKeyword,
    TypeKeyword,
    UnloggedKeyword,
    UnsetKeyword,
    UpdateKeyword,
    UseKeyword,
    UserKeyword,
    UsersKeyword,
    UsingKeyword,
    UuidKeyword,
    ValuesKeyword,
    VarCharKeyword,
    VarIntKeyword,
    ViewKeyword,
    WhereKeyword,
    WithKeyword,

    // index column keywords
    EntriesKeyword, // todo test lex
    FullKeyword,    // todo test lex
    KeysKeyword,    // todo test lex

    UuidLiteral,
    StringLiteral(StringStyle),
    NumberLiteral,
    BlobLiteral,
    Identifier,
}

impl TokenName {
    // todo determine if any gotchas with reserved vs not reserved keywords
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
            "entries" => EntriesKeyword,
            "execute" => ExecuteKeyword,
            "exists" => ExistsKeyword,
            "false" => FalseKeyword,
            "filtering" => FilteringKeyword,
            "finalfunc" => FinalFuncKeyword,
            "float" => FloatKeyword,
            "from" => FromKeyword,
            "frozen" => FrozenKeyword,
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
            "into" => IntoKeyword,
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

#[derive(Debug)]
pub struct Token {
    #[allow(unused)]
    pub line: usize,
    pub name: TokenName,
    pub range: TokenRange,
}

impl Token {
    fn new(name: TokenName, line: usize, range: TokenRange) -> Self {
        Self { line, name, range }
    }

    pub fn to_token_view(&self, cql: &Arc<String>) -> TokenView {
        TokenView {
            cql: cql.clone(),
            range: self.range.clone(),
        }
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

    pub fn tokenize(mut self) -> Result<Vec<Token>, anyhow::Error> {
        loop {
            if self.current.begin() >= self.cql.len() {
                break;
            }
            let c = self.splice();
            let maybe_name = match c {
                " " | "\r" | "\t" | "\n" => {
                    if c == "\n" {
                        self.line += 1;
                    }
                    self.current.next_char();
                    continue;
                }
                "{" => Some(LeftCurvedBracket),
                "}" => Some(RightCurvedBracket),
                "(" => Some(LeftParenthesis),
                ")" => Some(RightParenthesis),
                "[" => Some(LeftSquareBracket),
                "]" => Some(RightSquareBracket),
                "-" => {
                    if self.match_next("-") {
                        self.current.next_char();
                        self.skip_until_line_term();
                        None
                    } else {
                        Some(Minus)
                    }
                }
                "/" => {
                    if self.match_next("/") {
                        self.current.next_char();
                        self.skip_until_line_term();
                    } else if self.match_next("*") {
                        self.current.next_char();
                        loop {
                            match self.peek() {
                                None => break,
                                Some(s) => {
                                    if s == "*" && self.match_next_nth(1, "/") {
                                        self.current.next_char();
                                        break;
                                    } else {
                                        if s == "\n" {
                                            self.line += 1;
                                        }
                                        self.current.next_char();
                                    }
                                }
                            }
                        }
                    }
                    None
                }
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
                "'" => match self.quote_string() {
                    Ok(name) => Some(name),
                    Err(err) => return Err(err),
                },
                &_ => self.constant_or_identifier_or_keyword().ok(),
            };
            if let Some(name) = maybe_name {
                self.add_token(name);
            }
            self.current.next_char();
        }
        Ok(self.tokens)
    }

    fn quote_string(&mut self) -> Result<TokenName, anyhow::Error> {
        let is_triple_quote = self.match_next("'") && self.match_next_nth(1, "'");
        if is_triple_quote {
            self.advance();
            self.advance();
            loop {
                match self.peek() {
                    None => return Err(anyhow!("unclosed triple quote string")),
                    Some(c) => {
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
            }
            Ok(StringLiteral(StringStyle::TripleQuote))
        } else {
            let mut escaped_single_quote = false;
            loop {
                match self.peek() {
                    None => return Err(anyhow!("unclosed single quote string")),
                    Some(c) => {
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
            }
            Ok(StringLiteral(StringStyle::SingleQuote))
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
        let mut line_commented_started = false;
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
                            if self.match_next_nth(1, "-") {
                                line_commented_started = true;
                            } else {
                                has_dash = true;
                            }
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
                if !line_commented_started && advance && self.advance().is_some() {
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
        if !self.peek().is_some_and(|c| c.eq_ignore_ascii_case("x")) {
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
            if hex { Ok(Some(BlobLiteral)) } else { Err(()) }
        }
    }

    fn add_token(&mut self, name: TokenName) {
        let token = Token::new(name, self.line, self.current.clone());
        self.tokens.push(token);
    }

    fn advance(&mut self) -> Option<&'a str> {
        let maybe_c = self.char_at(self.current.end() + 1);
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
        self.peek_nth(nth) == Some(exp)
    }

    fn peek(&self) -> Option<&'a str> {
        self.peek_nth(0)
    }

    fn peek_nth(&self, nth: usize) -> Option<&'a str> {
        let i = self.current.end() + 1 + nth;
        if i >= self.cql.len() {
            None
        } else {
            self.char_at(i)
        }
    }

    fn skip_until_line_term(&mut self) {
        loop {
            match self.peek() {
                None => break,
                Some(s) => {
                    if s == "\n" {
                        break;
                    } else {
                        self.current.next_char();
                    }
                }
            }
        }
    }

    fn splice(&self) -> &'a str {
        &self.cql[self.current.begin()..=self.current.end()]
    }
}
