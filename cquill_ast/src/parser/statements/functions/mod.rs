use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

#[cfg(test)]
mod create_aggregate_test;

#[cfg(test)]
mod create_function_test;

#[cfg(test)]
mod drop_aggregate_test;

#[cfg(test)]
mod drop_function_test;

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/functions.html#create-aggregate-statement
// todo can an aggregate stype be frozen?
// todo parse init condition
pub fn parse_create_aggregate_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
    create_or_replace: bool,
) -> ParseResult<CreateAggregateStatement> {
    let if_exists_behavior = CreateIfExistsBehavior::new(
        create_or_replace,
        pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?,
    )?;
    let function_name = pop_identifier(cql, iter)?;
    let function_arg = pop_aggregate_signature(cql, iter)?;
    pop_next_match(iter, SFuncKeyword)?;
    let state_function = pop_identifier(cql, iter)?;
    pop_next_match(iter, STypeKeyword)?;
    let state_type = pop_cql_data_type(cql, iter)?;
    let final_function = if peek_next_match(iter, FinalFuncKeyword)? {
        iter.next();
        Some(pop_identifier(cql, iter)?)
    } else {
        None
    };
    let init_condition = if peek_next_match(iter, InitCondKeyword)? {
        advance_until(iter, Semicolon);
        true
    } else {
        false
    };
    Ok(CreateAggregateStatement {
        if_exists_behavior,
        function_name,
        function_arg,
        state_function,
        state_type,
        final_function,
        init_condition,
    })
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/functions.html#create-function-statement
pub fn parse_create_function_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
    create_or_replace: bool,
) -> ParseResult<CreateFunctionStatement> {
    let if_exists_behavior = CreateIfExistsBehavior::new(
        create_or_replace,
        pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?,
    )?;
    let function_name = pop_identifier(cql, iter)?;
    let function_args = pop_named_data_types_map(cql, iter)?;
    let on_null_input = match pop_next(iter)?.name {
        CalledKeyword => OnNullInput::Called,
        ReturnsKeyword => {
            pop_next_match(iter, NullKeyword)?;
            OnNullInput::ReturnsNull
        }
        _ => todo!("parse error"),
    };
    pop_next_match(iter, OnKeyword)?;
    pop_next_match(iter, NullKeyword)?;
    pop_next_match(iter, InputKeyword)?;
    pop_next_match(iter, ReturnsKeyword)?;
    let returns = pop_cql_data_type(cql, iter)?;
    pop_next_match(iter, LanguageKeyword)?;
    let language = pop_identifier(cql, iter)?;
    pop_next_match(iter, AsKeyword)?;
    let function_body = pop_string_literal(cql, iter)?;
    Ok(CreateFunctionStatement {
        if_exists_behavior,
        function_name,
        function_args,
        on_null_input,
        returns,
        language,
        function_body,
    })
}

impl CreateIfExistsBehavior {
    fn new(create_or_replace: bool, if_not_exists: bool) -> ParseResult<CreateIfExistsBehavior> {
        if create_or_replace && if_not_exists {
            todo!("parse error")
        } else if create_or_replace {
            Ok(CreateIfExistsBehavior::Replace)
        } else if if_not_exists {
            Ok(CreateIfExistsBehavior::DoNotError)
        } else {
            Ok(CreateIfExistsBehavior::Error)
        }
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/functions.html#drop-aggregate-statement
pub fn parse_drop_aggregate_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropAggregateStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, aggregate_name) = pop_keyspace_object_name(cql, iter)?;
    let signature = pop_if_function_signature(cql, iter)?;
    Ok(DropAggregateStatement {
        aggregate_name,
        if_exists,
        keyspace_name,
        signature,
    })
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/functions.html#drop-function-statement
pub fn parse_drop_function_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<DropFunctionStatement> {
    let if_exists = pop_sequence(iter, &[IfKeyword, ExistsKeyword])?;
    let (keyspace_name, function_name) = pop_keyspace_object_name(cql, iter)?;
    let signature = pop_if_function_signature(cql, iter)?;
    Ok(DropFunctionStatement {
        function_name,
        if_exists,
        keyspace_name,
        signature,
    })
}
