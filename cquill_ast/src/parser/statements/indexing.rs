use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

/// https://cassandra.apache.org/doc/5.0.9/cassandra/reference/cql-commands/create-custom-index.html
pub fn parse_create_custom_index_statement(
    _cql: &Arc<String>,
    _iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateCustomIndexStatement> {
    todo!();
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/reference/cql-commands/create-index.html
pub fn parse_create_index_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateIndexStatement> {
    let if_not_exists = pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let index_name = if peek_next_match(iter, Identifier)? {
        Some(pop_identifier(cql, iter)?)
    } else {
        None
    };
    pop_next_match(iter, OnKeyword)?;
    let (keyspace_name, table_name) = pop_keyspace_object_name(cql, iter)?;
    pop_next_match(iter, LeftParenthesis)?;
    let on_column = match iter.next() {
        None => todo!("parse error"),
        Some(popped) => match popped.name {
            FullKeyword | EntriesKeyword | KeysKeyword | ValuesKeyword => {
                pop_next_match(iter, LeftParenthesis)?;
                let column_name = pop_identifier(cql, iter)?;
                pop_next_match(iter, RightParenthesis)?;
                match popped.name {
                    FullKeyword => CreateIndexColumn::FullCollection(column_name),
                    EntriesKeyword => CreateIndexColumn::MapEntries(column_name),
                    KeysKeyword => CreateIndexColumn::MapKeys(column_name),
                    ValuesKeyword => CreateIndexColumn::MapValues(column_name),
                    _ => unreachable!(),
                }
            }
            Identifier => CreateIndexColumn::Column(popped.to_token_view(cql)),
            _ => todo!("parse error"),
        },
    };
    pop_next_match(iter, RightParenthesis)?;
    Ok(CreateIndexStatement {
        if_not_exists,
        keyspace_name,
        table_name,
        index_name,
        on_column,
    })
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/reference/cql-commands/drop-index.html
pub fn parse_drop_index_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropIndexStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, index_name) = pop_keyspace_object_name(cql, iter)?;
    Ok(DropIndexStatement {
        index_name,
        if_exists,
        keyspace_name,
    })
}
