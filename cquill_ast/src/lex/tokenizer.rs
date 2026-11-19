use anyhow::anyhow;
use uuid::Uuid;

use crate::ast::*;
use crate::lex::{Token, TokenName, TokenName::*};

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
                "'" => Some(self.quote_string()?),
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
