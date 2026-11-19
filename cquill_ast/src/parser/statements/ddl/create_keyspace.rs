use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;

use std::collections::HashMap;
use std::{iter::Peekable, slice::Iter, sync::Arc};

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#create-keyspace-statement
pub fn parse_create_keyspace_statement(
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
