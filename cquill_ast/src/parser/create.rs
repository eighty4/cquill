use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::iter::{
    advance_until, peek_next_match, pop_aggregate_signature, pop_boolean_literal,
    pop_comma_separated_identifiers, pop_cql_data_type, pop_identifier, pop_keyspace_object_name,
    pop_next, pop_next_if, pop_next_match, pop_sequence, pop_string_literal,
};
use crate::parser::ParseResult;
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
                    AggregateKeyword => parse_create_aggregate_statement(cql, iter, true)
                        .map(CreateStatement::Aggregate),
                    FunctionKeyword => parse_create_function_statement(cql, iter, true)
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

// todo can an aggregate stype be frozen?
// todo parse init condition
fn parse_create_aggregate_statement(
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

fn parse_create_function_statement(
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
    let if_not_exists = pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let keyspace_name = pop_identifier(cql, iter)?;
    pop_next_match(iter, WithKeyword)?;
    let mut replication = None;
    let mut durable_writes = None;
    loop {
        println!("looooooop");
        let popped = pop_next(iter)?;
        match popped.name {
            ReplicationKeyword => {
                pop_next_match(iter, Equal)?;
                let mut replication_config = pop_map_config(cql, iter)?;
                replication = Some(match replication_config.remove("class") {
                    None => todo!("parse error"),
                    Some(replication_class) => match replication_class {
                        MapConfigLiteral::String(replication_class) => {
                            match replication_class.as_str() {
                                "SimpleStrategy" => {
                                    match replication_config.get("replication_factor") {
                                        Some(MapConfigLiteral::Integer(factor)) => {
                                            KeyspaceReplication::Simple(*factor)
                                        }
                                        _ => todo!("parse error"),
                                    }
                                }
                                "NetworkTopologyStrategy" => {
                                    let mut factors = HashMap::new();
                                    for (dc, factor) in replication_config {
                                        factors.insert(
                                            dc,
                                            match factor {
                                                MapConfigLiteral::Integer(factor) => factor,
                                                _ => todo!("parse error"),
                                            },
                                        );
                                    }
                                    KeyspaceReplication::NetworkTopology(factors)
                                }
                                _ => todo!("parse error"),
                            }
                        }
                        _ => todo!("parse error"),
                    },
                })
            }
            Identifier => {
                // todo &str without to_token_view allocations
                //  maybe `Token::to_str(&self, cql: &Arc<String>)`
                if popped.to_token_view(cql).value() == *"durable_writes" {
                    pop_next_match(iter, Equal)?;
                    durable_writes = Some(pop_boolean_literal(iter)?);
                } else {
                    todo!("parse error");
                }
            }
            _ => todo!("parse error"),
        }
        if pop_next_if(iter, AndKeyword).is_none() {
            break;
        }
    }
    let replication = match replication {
        None => todo!("parse error"),
        Some(replication) => replication,
    };
    Ok(CreateKeyspaceStatement {
        if_not_exists,
        keyspace_name,
        durable_writes,
        replication,
    })
}

fn pop_map_config(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<HashMap<String, MapConfigLiteral>> {
    pop_next_match(iter, LeftCurvedBracket)?;
    let mut map_config = HashMap::new();
    loop {
        let key = pop_string_literal(cql, iter)?.value();
        pop_next_match(iter, Colon)?;
        let value = {
            let popped = pop_next(iter)?;
            match &popped.name {
                StringLiteral(style) => MapConfigLiteral::String(
                    StringView {
                        cql: cql.clone(),
                        range: popped.range.clone(),
                        style: style.clone(),
                    }
                    .value(),
                ),
                NumberLiteral => MapConfigLiteral::Integer(
                    popped.to_token_view(cql).value().parse().expect("integer"),
                ),
                TrueKeyword => MapConfigLiteral::Boolean(true),
                FalseKeyword => MapConfigLiteral::Boolean(false),
                _ => todo!("parse error"),
            }
        };
        map_config.insert(key, value);
        if pop_next_if(iter, Comma).is_none() {
            break;
        }
    }
    pop_next_match(iter, RightCurvedBracket)?;
    Ok(map_config)
}

enum MapConfigLiteral {
    #[allow(unused)]
    Boolean(bool),
    Integer(i8),
    String(String),
}

fn parse_create_materialized_view_statement(
    _cql: &Arc<String>,
    _iter: &mut Peekable<Iter<Token>>,
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
                        RoleConfigAttribute::Options(pop_hacky_map_literal(cql, iter)?)
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
    let if_not_exists = pop_sequence(iter, &[IfKeyword, NotKeyword, ExistsKeyword])?;
    let (keyspace_name, table_name) = pop_keyspace_object_name(cql, iter)?;
    let column_definitions = pop_table_column_definitions(cql, iter)?;
    let attributes = if pop_next_if(iter, WithKeyword).is_some() {
        let mut attributes = Vec::new();
        loop {
            match iter.next() {
                None => todo!("parse error"),
                Some(popped) => match popped.name {
                    CompactKeyword => {
                        pop_next_match(iter, StorageKeyword)?;
                        attributes.push(TableDefinitionAttribute::CompactStorage);
                    }
                    ClusteringKeyword => {
                        pop_next_match(iter, OrderKeyword)?;
                        pop_next_match(iter, ByKeyword)?;
                        pop_next_match(iter, LeftParenthesis)?;
                        let mut clustering_orders = Vec::new();
                        loop {
                            clustering_orders.push({
                                let column_name = pop_identifier(cql, iter)?;
                                let order = if pop_next_if(iter, AscKeyword).is_some() {
                                    Some(ClusteringOrder::Asc)
                                } else if pop_next_if(iter, DescKeyword).is_some() {
                                    Some(ClusteringOrder::Desc)
                                } else {
                                    todo!("parse error");
                                };
                                ClusteringOrderDefinition { column_name, order }
                            });
                            if pop_next_if(iter, Comma).is_none() {
                                break;
                            }
                        }
                        attributes.push(TableDefinitionAttribute::ClusteringOrderBy(
                            clustering_orders,
                        ));
                        pop_next_match(iter, RightParenthesis)?;
                    }
                    Identifier => match popped.to_token_view(cql).value().to_lowercase().as_str() {
                        "comment" => {
                            pop_next_match(iter, Equal)?;
                            attributes.push(TableDefinitionAttribute::Comment(pop_string_literal(
                                cql, iter,
                            )?))
                        }
                        "compaction" => {
                            pop_next_match(iter, Equal)?;
                            attributes.push(TableDefinitionAttribute::Compaction(
                                pop_hacky_map_literal(cql, iter)?,
                            ))
                        }
                        _ => todo!("parse error"),
                    },
                    _ => todo!("parse error"),
                },
            }
            if pop_next_if(iter, Comma).is_none() {
                break;
            }
        }
        Some(attributes)
    } else {
        None
    };
    let table_alias = None;
    Ok(CreateTableStatement {
        if_not_exists,
        keyspace_name,
        table_name,
        column_definitions,
        table_alias,
        attributes,
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
) -> ParseResult<Vec<(TokenView, CqlDataType)>> {
    pop_next_match(iter, LeftParenthesis)?;
    let mut fields = Vec::new();
    loop {
        let field_name = pop_identifier(cql, iter)?;
        let field_type = pop_cql_data_type(cql, iter)?;
        fields.push((field_name, field_type));
        if pop_next_if(iter, Comma).is_none() {
            break;
        }
    }
    pop_next_match(iter, RightParenthesis)?;
    Ok(fields)
}

fn pop_table_column_definitions(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<Vec<ColumnDefinition>> {
    pop_next_match(iter, LeftParenthesis)?;
    let mut definitions = Vec::new();
    loop {
        if pop_next_if(iter, PrimaryKeyword).is_some() {
            pop_next_match(iter, KeyKeyword)?;
            pop_next_match(iter, LeftParenthesis)?;
            definitions.push(ColumnDefinition::PrimaryKey(
                if pop_next_if(iter, LeftParenthesis).is_some() {
                    let partition = pop_comma_separated_identifiers(cql, iter)?;
                    pop_next_match(iter, RightParenthesis)?;
                    let clustering = if peek_next_match(iter, RightParenthesis)? {
                        Vec::new()
                    } else {
                        pop_next_match(iter, Comma)?;
                        pop_comma_separated_identifiers(cql, iter)?
                    };
                    PrimaryKeyDefinition::CompositePartition {
                        partition,
                        clustering,
                    }
                } else {
                    let partition = pop_identifier(cql, iter)?;
                    if pop_next_if(iter, Comma).is_some() {
                        PrimaryKeyDefinition::Compound {
                            partition,
                            clustering: pop_comma_separated_identifiers(cql, iter)?,
                        }
                    } else {
                        PrimaryKeyDefinition::Single(partition)
                    }
                },
            ));
            pop_next_match(iter, RightParenthesis)?;
        } else {
            let column_name = pop_identifier(cql, iter)?;
            let data_type = pop_cql_data_type(cql, iter)?;
            let attribute = match pop_next_if(iter, StaticKeyword) {
                Some(_) => Some(ColumnDefinitionAttribute::Static),
                None => match pop_next_if(iter, PrimaryKeyword) {
                    Some(_) => {
                        pop_next_match(iter, KeyKeyword)?;
                        Some(ColumnDefinitionAttribute::PrimaryKey)
                    }
                    None => None,
                },
            };
            definitions.push(ColumnDefinition::Column {
                column_name,
                data_type,
                attribute,
            });
        }
        if pop_next_if(iter, Comma).is_none() {
            break;
        }
    }
    pop_next_match(iter, RightParenthesis)?;
    Ok(definitions)
}

fn pop_hacky_map_literal(
    cql: &Arc<String>,
    iter: &mut Peekable<Iter<Token>>,
) -> ParseResult<HashMap<StringView, TokenView>> {
    pop_next_match(iter, LeftCurvedBracket)?;
    let mut map = HashMap::new();
    loop {
        let key = pop_string_literal(cql, iter)?;
        pop_next_match(iter, Colon)?;
        let popped = pop_next(iter)?;
        match popped.name {
            StringLiteral(_) | UuidLiteral | NumberLiteral | TrueKeyword | FalseKeyword => {}
            _ => todo!("parse error"),
        };
        map.insert(key, popped.to_token_view(cql));
        match pop_next(iter)?.name {
            Comma => continue,
            RightCurvedBracket => break,
            _ => todo!("parse error"),
        }
    }
    Ok(map)
}
