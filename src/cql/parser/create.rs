use crate::cql::ast::*;
use crate::cql::lex::Token;
use crate::cql::lex::TokenName::*;
use crate::cql::parser::iter::{
    peek_next_match, pop_boolean_literal, pop_cql_data_type, pop_identifier,
    pop_keyspace_object_name, pop_next, pop_next_if, pop_next_match, pop_sequence,
    pop_string_literal,
};
use crate::cql::parser::ParseResult;
use std::collections::HashMap;
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::Arc;

pub fn parse_create_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateStatement> {
    match iter.next() {
        None => todo!("parse error"),
        Some(token) => match token.name {
            AggregateKeyword => {
                parse_create_aggregate_statement(cql, iter, false).map(CreateStatement::Aggregate)
            }
            FunctionKeyword => {
                parse_create_function_statement(cql, iter, false).map(CreateStatement::Function)
            }
            OrKeyword => {
                pop_next_match(iter, ReplaceKeyword)?;
                match pop_next(iter)?.name {
                    AggregateKeyword => parse_create_aggregate_statement(cql, iter, false)
                        .map(CreateStatement::Aggregate),
                    FunctionKeyword => parse_create_function_statement(cql, iter, false)
                        .map(CreateStatement::Function),
                    _ => todo!("parse error"),
                }
            }
            IndexKeyword => parse_create_index_statement(cql, iter).map(CreateStatement::Index),
            KeyspaceKeyword => {
                parse_create_keyspace_statement(cql, iter).map(CreateStatement::Keyspace)
            }
            MaterializedKeyword => {
                pop_next_match(iter, ViewKeyword)?;
                parse_create_materialized_view_statement(cql, iter)
                    .map(CreateStatement::MaterializedView)
            }
            RoleKeyword => parse_create_role_statement(cql, iter).map(CreateStatement::Role),
            TableKeyword => parse_create_table_statement(cql, iter).map(CreateStatement::Table),
            TriggerKeyword => {
                parse_create_trigger_statement(cql, iter).map(CreateStatement::Trigger)
            }
            TypeKeyword => parse_create_type_statement(cql, iter).map(CreateStatement::Type),
            UserKeyword => parse_create_user_statement(cql, iter).map(CreateStatement::User),
            _ => todo!("parse error"),
        },
    }
}

fn parse_create_aggregate_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
    create_or_replace: bool,
) -> ParseResult<CreateAggregateStatement> {
    let exists_behavior = CreateExistsBehavior::new(
        create_or_replace,
        pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?,
    )?;
    unimplemented!()
}

fn parse_create_function_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
    create_or_replace: bool,
) -> ParseResult<CreateFunctionStatement> {
    let exists_behavior = CreateExistsBehavior::new(
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
    pop_next_match(iter, ReturnsKeyword)?;
    let returns = pop_cql_data_type(iter)?;
    pop_next_match(iter, LanguageKeyword)?;
    let language = pop_identifier(cql, iter)?;
    pop_next_match(iter, AsKeyword)?;
    let function_body = pop_string_literal(cql, iter)?;
    Ok(CreateFunctionStatement {
        exists_behavior,
        function_name,
        function_args,
        on_null_input,
        returns,
        language,
        function_body,
    })
}

impl CreateExistsBehavior {
    fn new(
        create_or_replace: bool,
        if_not_exists: bool,
    ) -> Result<CreateExistsBehavior, anyhow::Error> {
        if create_or_replace && if_not_exists {
            todo!("parse error")
        } else if create_or_replace {
            Ok(CreateExistsBehavior::Replace)
        } else if if_not_exists {
            Ok(CreateExistsBehavior::IfNotExists)
        } else {
            Ok(CreateExistsBehavior::ErrorIfExists)
        }
    }
}

fn parse_create_index_statement(
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

fn parse_create_keyspace_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateKeyspaceStatement> {
    unimplemented!()
}

fn parse_create_materialized_view_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateMaterializedViewStatement> {
    unimplemented!()
}

fn parse_create_role_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateRoleStatement> {
    let if_not_exists = pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let role_name = pop_identifier(cql, iter)?;
    let attributes = if peek_next_match(iter, WithKeyword)? {
        _ = iter.next();
        let mut attributes = Vec::new();
        loop {
            attributes.push(match iter.next() {
                None => todo!("parse error"),
                Some(popped) => match popped.name {
                    SuperUserKeyword => {
                        pop_next_match(iter, Equal)?;
                        RoleConfigAttribute::Superuser(pop_boolean_literal(iter)?)
                    }
                    LoginKeyword => {
                        pop_next_match(iter, Equal)?;
                        RoleConfigAttribute::Login(pop_boolean_literal(iter)?)
                    }
                    PasswordKeyword => {
                        pop_next_match(iter, Equal)?;
                        RoleConfigAttribute::Password(AuthPassword::PlainText(pop_string_literal(
                            cql, iter,
                        )?))
                    }
                    HashedKeyword => {
                        pop_next_match(iter, PasswordKeyword)?;
                        pop_next_match(iter, Equal)?;
                        RoleConfigAttribute::Password(AuthPassword::Hashed(pop_string_literal(
                            cql, iter,
                        )?))
                    }
                    OptionsKeyword => {
                        pop_next_match(iter, Equal)?;
                        pop_next_match(iter, LeftCurvedBracket)?;
                        let mut options = HashMap::new();
                        loop {
                            let key = pop_string_literal(cql, iter)?;
                            pop_next_match(iter, Colon)?;
                            let popped = pop_next(iter)?;
                            match popped.name {
                                StringLiteral(_) | UuidLiteral | NumberLiteral | TrueKeyword
                                | FalseKeyword => {}
                                _ => todo!("parse error"),
                            };
                            options.insert(key, popped.to_token_view(cql));
                            match pop_next(iter)?.name {
                                Comma => continue,
                                RightCurvedBracket => break,
                                _ => todo!("parse error"),
                            }
                        }
                        RoleConfigAttribute::Options(options)
                    }
                    AccessKeyword => {
                        pop_next_match(iter, ToKeyword)?;
                        if peek_next_match(iter, AllKeyword)? {
                            _ = iter.next();
                            pop_next_match(iter, DatacentersKeyword)?;
                            RoleConfigAttribute::Access(Datacenters::All)
                        } else {
                            pop_next_match(iter, DatacentersKeyword)?;
                            pop_next_match(iter, LeftCurvedBracket)?;
                            let mut datacenters = Vec::new();
                            loop {
                                datacenters.push(pop_string_literal(cql, iter)?);
                                match pop_next(iter)?.name {
                                    Comma => continue,
                                    RightCurvedBracket => break,
                                    _ => todo!("parse error"),
                                }
                            }
                            RoleConfigAttribute::Access(Datacenters::Explicit(datacenters))
                        }
                    }
                    _ => todo!("parse error {:?}", popped.name),
                },
            });
            if pop_next_if(iter, AndKeyword).is_none() {
                break;
            }
        }
        Some(attributes)
    } else {
        None
    };
    Ok(CreateRoleStatement {
        if_not_exists,
        role_name,
        attributes,
    })
}

fn parse_create_table_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateTableStatement> {
    let (keyspace_name, table_name) = pop_keyspace_object_name(cql, iter)?;
    Ok(CreateTableStatement {
        keyspace_name,
        table_name,
        column_definitions: parse_create_table_column_definitions(cql, iter)?,
        table_alias: None,
        attributes: Vec::new(),
        if_not_exists: false,
    })
}

fn parse_create_trigger_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateTriggerStatement> {
    let if_not_exists = pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let trigger_name = pop_identifier(cql, iter)?;
    pop_next_match(iter, OnKeyword)?;
    let (keyspace_name, table_name) = pop_keyspace_object_name(cql, iter)?;
    pop_next_match(iter, UsingKeyword)?;
    let index_classpath = pop_string_literal(cql, iter)?;
    Ok(CreateTriggerStatement {
        if_not_exists,
        trigger_name,
        table_name,
        keyspace_name,
        index_classpath,
    })
}

// todo fields with collections, collections with generics and UDTs
fn parse_create_type_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateTypeStatement> {
    let if_not_exists = pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let (keyspace_name, type_name) = pop_keyspace_object_name(cql, iter)?;
    let fields = pop_named_data_types_map(cql, iter)?;
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

fn parse_create_user_statement(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<CreateUserStatement> {
    let if_not_exists = pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let user_name = pop_identifier(cql, iter)?;
    let password = match iter.peek() {
        None => None,
        Some(peeked) => match peeked.name {
            WithKeyword => {
                _ = iter.next();
                let popped = pop_next(iter)?;
                Some(match popped.name {
                    HashedKeyword => {
                        pop_next_match(iter, PasswordKeyword)?;
                        AuthPassword::Hashed(pop_string_literal(cql, iter)?)
                    }
                    PasswordKeyword => AuthPassword::PlainText(pop_string_literal(cql, iter)?),
                    _ => todo!("parse error"),
                })
            }
            _ => None,
        },
    };
    let user_status = match iter.peek() {
        None => None,
        Some(peeked) => {
            let status = match peeked.name {
                NoSuperUserKeyword => Some(CreateUserStatus::NoSuperuser),
                SuperUserKeyword => Some(CreateUserStatus::Superuser),
                Semicolon => None,
                _ => todo!("parse error"),
            };
            if status.is_some() {
                _ = iter.next();
            }
            status
        }
    };
    Ok(CreateUserStatement {
        user_name,
        if_not_exists,
        password,
        user_status,
    })
}

/// Parses `(datum1 int, datum2 text)` constructs used by UDTs and function arguments.
// todo is `()` valid to create a function or UDT with an empty args signature?
fn pop_named_data_types_map(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<HashMap<TokenView, CqlDataType>> {
    pop_next_match(iter, LeftParenthesis)?;
    let mut fields = HashMap::new();
    loop {
        let field_name = pop_identifier(cql, iter)?;
        let field_type = pop_cql_data_type(iter)?;
        fields.insert(field_name, field_type);
        if pop_next_if(iter, Comma).is_none() {
            break;
        }
    }
    pop_next_match(iter, RightParenthesis)?;
    Ok(fields)
}
