use crate::ast::*;
use crate::lex::Token;
use crate::lex::TokenName::*;
use crate::parser::ParseResult;
use crate::parser::iter::*;

use std::{iter::Peekable, slice::Iter, sync::Arc};

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#create-table-statement
pub fn parse_create_table_statement(
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
