use crate::cql::ast::*;
use crate::cql::lex::*;
use std::collections::HashMap;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;
use TokenName::*;

pub type ParseResult<T> = Result<T, anyhow::Error>;

pub fn parse_cql(cql: String) -> ParseResult<Vec<CqlStatement>> {
    let tokens = Tokenizer::new(cql.as_str()).tokenize().unwrap();
    let cql = Arc::new(cql);
    let mut iter = tokens.iter().peekable();
    let mut result = Vec::new();
    while iter.peek().is_some() {
        result.push(parse_statement(&cql, &mut iter)?);
        while iter.next_if(|t| t.name == Semicolon).is_some() {}
    }
    if result.is_empty() {
        todo!("parse error")
    } else {
        Ok(result)
    }
}

fn parse_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CqlStatement> {
    match iter.next() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            CreateKeyword => parse_create_statement(cql, iter).map(CqlStatement::Create),
            DropKeyword => parse_drop_statement(cql, iter).map(CqlStatement::Drop),
            _ => todo!("parse error {:?}", token.name),
        },
    }
}

fn parse_create_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateStatement> {
    match iter.next() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            IndexKeyword => Ok(CreateStatement::Index(parse_create_index_statement(
                cql, iter,
            )?)),
            KeyspaceKeyword => Ok(CreateStatement::Keyspace(parse_create_keyspace_statement(
                cql, iter,
            )?)),
            RoleKeyword => Ok(CreateStatement::Role(parse_create_role_statement(
                cql, iter,
            )?)),
            TableKeyword => Ok(CreateStatement::Table(parse_create_table_statement(
                cql, iter,
            )?)),
            TypeKeyword => Ok(CreateStatement::Type(parse_create_type_statement(
                cql, iter,
            )?)),
            _ => todo!("parse error"),
        },
    }
}

fn parse_create_index_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateIndexStatement> {
    todo!("parse result")
}

fn parse_create_keyspace_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateKeyspaceStatement> {
    todo!("parse result")
}

fn parse_create_role_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateRoleStatement> {
    todo!("parse result")
}

fn parse_create_table_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateTableStatement> {
    let (keyspace_name, table_name) = parse_keyspace_object_names(cql, iter)?;
    Ok(CreateTableStatement {
        keyspace_name,
        table_name,
        column_definitions: parse_create_table_column_definitions(cql, iter)?,
        table_alias: None,
        attributes: Vec::new(),
        if_not_exists: false,
    })
}

// todo fields with collections, collections with generics and udts
fn parse_create_type_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateTypeStatement> {
    let if_not_exists = peek_match_advance(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let (keyspace_name, type_name) = parse_keyspace_object_names(cql, iter)?;
    _ = next(iter, LeftParenthesis)?;
    let mut fields = HashMap::new();
    loop {
        let field_name = create_view(cql, next(iter, Identifier)?);
        let field_type = match iter.next() {
            None => todo!("panic error"),
            Some(popped) => {
                if popped.name.is_cql_data_type() {
                    create_view(cql, popped)
                } else {
                    todo!("panic error")
                }
            }
        };
        fields.insert(field_name, field_type);
        if iter.next_if(|t| t.name == Comma).is_none() {
            break;
        }
    }
    _ = next(iter, RightParenthesis)?;
    Ok(CreateTypeStatement {
        keyspace_name,
        type_name,
        if_not_exists,
        fields,
    })
}

fn parse_create_table_column_definitions(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<ColumnDefinitions> {
    todo!()
    // ColumnDefinitions {
    //     definitions: Vec::new(),
    //     primary_key: None,
    //     view: create_view(),
    // }
}

fn parse_drop_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropStatement> {
    match iter.next() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            AggregateKeyword => {
                parse_drop_aggregate_statement(cql, iter).map(DropStatement::Aggregate)
            }
            FunctionKeyword => {
                parse_drop_function_statement(cql, iter).map(DropStatement::Function)
            }
            IndexKeyword => parse_drop_index_statement(cql, iter).map(DropStatement::Index),
            KeyspaceKeyword => {
                parse_drop_keyspace_statement(cql, iter).map(DropStatement::Keyspace)
            }
            MaterializedKeyword => {
                _ = next(iter, ViewKeyword)?;
                parse_drop_materialized_view_statement(cql, iter)
                    .map(DropStatement::MaterializedView)
            }
            RoleKeyword => parse_drop_role_statement(cql, iter).map(DropStatement::Role),
            TableKeyword => parse_drop_table_statement(cql, iter).map(DropStatement::Table),
            TriggerKeyword => parse_drop_trigger_statement(cql, iter).map(DropStatement::Trigger),
            TypeKeyword => parse_drop_type_statement(cql, iter).map(DropStatement::Type),
            UserKeyword => parse_drop_user_statement(cql, iter).map(DropStatement::User),
            _ => todo!("parse error"),
        },
    }
}

fn parse_drop_aggregate_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropAggregateStatement> {
    let if_exists = peek_match_advance(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, aggregate_name) = parse_keyspace_object_names(cql, iter)?;
    let signature = parse_function_type_signature(cql, iter);
    Ok(DropAggregateStatement {
        aggregate_name,
        if_exists,
        keyspace_name,
        signature,
    })
}

fn parse_drop_function_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropFunctionStatement> {
    let if_exists = peek_match_advance(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, function_name) = parse_keyspace_object_names(cql, iter)?;
    let signature = parse_function_type_signature(cql, iter);
    Ok(DropFunctionStatement {
        function_name,
        if_exists,
        keyspace_name,
        signature,
    })
}

fn parse_drop_index_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropIndexStatement> {
    let if_exists = peek_match_advance(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, index_name) = parse_keyspace_object_names(cql, iter)?;
    Ok(DropIndexStatement {
        index_name,
        if_exists,
        keyspace_name,
    })
}

fn parse_drop_keyspace_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropKeyspaceStatement> {
    let if_exists = peek_match_advance(iter, &[IfKeyword, ExistsKeyword])?;
    let keyspace_name = create_view(cql, next(iter, Identifier)?);
    Ok(DropKeyspaceStatement {
        keyspace_name,
        if_exists,
    })
}

fn parse_drop_materialized_view_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropMaterializedViewStatement> {
    let if_exists = peek_match_advance(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, view_name) = parse_keyspace_object_names(cql, iter)?;
    Ok(DropMaterializedViewStatement {
        view_name,
        if_exists,
        keyspace_name,
    })
}

fn parse_drop_role_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropRoleStatement> {
    let if_exists = peek_match_advance(iter, &[IfKeyword, ExistsKeyword])?;
    let role_name = create_view(cql, next(iter, Identifier)?);
    Ok(DropRoleStatement {
        role_name,
        if_exists,
    })
}

fn parse_drop_table_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropTableStatement> {
    let if_exists = peek_match_advance(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, table_name) = parse_keyspace_object_names(cql, iter)?;
    Ok(DropTableStatement {
        alias: None,
        table_name,
        if_exists,
        keyspace_name,
    })
}

fn parse_drop_trigger_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropTriggerStatement> {
    let if_exists = peek_match_advance(iter, &[IfKeyword, ExistsKeyword])?;
    let trigger_name = create_view(cql, next(iter, Identifier)?);
    next(iter, OnKeyword)?;
    let (keyspace_name, table_name) = parse_keyspace_object_names(cql, iter)?;
    Ok(DropTriggerStatement {
        trigger_name,
        table_name,
        if_exists,
        keyspace_name,
    })
}

fn parse_drop_type_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropTypeStatement> {
    let if_exists = peek_match_advance(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, type_name) = parse_keyspace_object_names(cql, iter)?;
    Ok(DropTypeStatement {
        type_name,
        if_exists,
        keyspace_name,
    })
}

fn parse_drop_user_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropUserStatement> {
    let if_exists = peek_match_advance(iter, &[IfKeyword, ExistsKeyword])?;
    let user_name = create_view(cql, next(iter, Identifier)?);
    Ok(DropUserStatement {
        user_name,
        if_exists,
    })
}

fn parse_keyspace_object_names(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> Result<(Option<TokenView>, TokenView), anyhow::Error> {
    let object_or_keyspace = create_view(cql, next(iter, Identifier)?);
    match iter.peek().map(|t| &t.name) {
        Some(Dot) => {
            _ = iter.next();
            Ok((
                Some(object_or_keyspace),
                create_view(cql, next(iter, Identifier)?),
            ))
        }
        _ => Ok((None, object_or_keyspace)),
    }
}

/// For type signatures of `DROP AGGREGATE` and `DROP FUNCTION` statements.
// todo collections
// todo udt
// todo generics
fn parse_function_type_signature(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> Option<Vec<TokenView>> {
    match iter.next_if(|t| t.name == LeftParenthesis) {
        None => None,
        Some(_) => {
            let mut result = Vec::new();
            loop {
                match iter.next() {
                    None => todo!("parse error"),
                    Some(arg) => {
                        if arg.name.is_cql_data_type() {
                            result.push(create_view(cql, arg));
                            match iter.next() {
                                None => todo!("parse error"),
                                Some(peeked) => match peeked.name {
                                    Comma => continue,
                                    RightParenthesis => break,
                                    _ => todo!("parse error"),
                                },
                            }
                        } else {
                            todo!("parse error");
                        }
                    }
                }
            }
            if result.is_empty() {
                todo!("parse error");
            }
            Some(result)
        }
    }
}

fn create_view(cql: &Arc<String>, token: &Token) -> TokenView {
    TokenView {
        cql: cql.clone(),
        range: token.range.clone(),
    }
}

fn next<'a>(
    iter: &'a mut Peekable<Iter<Token>>,
    next: TokenName,
) -> Result<&'a Token, anyhow::Error> {
    match iter.next() {
        None => todo!("panic error"),
        Some(token) => {
            if token.name == next {
                Ok(token)
            } else {
                todo!("panic error")
            }
        }
    }
}

/// Advances iter for each of input tokens so long as peeking next matches input.
/// If iter is advanced with `next()` and a subsequent `peek()` is None or does not match input,
/// this function will return an error.
/// Returns true and advances iterator `nexts.len()` number of times if all tokens match.
/// Returns false if first token peeked does not match `nexts.first()`.
/// Returns error if first token peeked matches and subsequent peeks return None or do not match.
fn peek_match_advance(
    iter: &mut Peekable<Iter<Token>>,
    nexts: &[TokenName],
) -> Result<bool, anyhow::Error> {
    let mut advanced = false;
    for maybe_next in nexts {
        match iter.peek() {
            None => todo!("parse error"),
            Some(next) => {
                if next.name == *maybe_next {
                    _ = iter.next();
                    advanced = true;
                } else if advanced {
                    todo!("parse error")
                } else {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}
