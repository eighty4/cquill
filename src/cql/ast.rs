use crate::cql::lex::TokenRange;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub struct TokenView {
    pub cql: Arc<String>,
    pub range: TokenRange,
}

impl TokenView {
    pub fn value(&self) -> String {
        String::from(&self.cql[self.range.begin()..=self.range.end()])
    }
}

impl Display for TokenView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}

/// Declares alias for TABLE keyword
#[derive(Debug, PartialEq)]
pub enum TableAlias {
    ColumnFamily,
}

#[derive(Debug, PartialEq)]
pub enum CqlStatement {
    Create(CreateStatement),
    Drop(DropStatement),
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, PartialEq)]
// todo CREATE MATERIALIZED VIEW
pub enum CreateStatement {
    Index(CreateIndexStatement),
    Keyspace(CreateKeyspaceStatement),
    Role(CreateRoleStatement),
    Table(CreateTableStatement),
}

#[derive(Debug, PartialEq)]
pub struct CreateIndexStatement {
    index_name: TokenView,
    if_not_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct CreateKeyspaceStatement {
    keyspace_name: TokenView,
    if_not_exists: bool,
    replication: KeyspaceReplication,
    durable_writes: Option<bool>,
}

#[derive(Debug, PartialEq)]
pub struct KeyspaceReplication {
    map_view: TokenView,
    strategy: KeyspaceReplicationStrategy,
}

#[derive(Debug, PartialEq)]
pub enum KeyspaceReplicationStrategy {
    NetworkTopology(HashMap<String, i8>),
    Simple(i8),
}

#[derive(Debug, PartialEq)]
// todo WITH statements
pub struct CreateRoleStatement {
    if_not_exists: bool,
    role_name: TokenView,
}

#[derive(Debug, PartialEq)]
// todo table_options
pub struct CreateTableStatement {
    pub keyspace_name: Option<TokenView>,
    pub table_name: TokenView,
    pub column_definitions: ColumnDefinitions,
    pub if_not_exists: bool,
    pub table_alias: Option<TableAlias>,
    pub attributes: Vec<CreateTableAttribute>,
}

#[derive(Debug, PartialEq)]
pub struct CreateTableAttribute {
    view: TokenView,
    kind: CreateTableAttributeKind,
}

#[derive(Debug, PartialEq)]
pub enum CreateTableAttributeKind {
    TableOptions(TableOptions),
    ClusteringOrderBy(ClusteringOrderByColumn),
    Id(String),
    CompactStorage,
}

// todo impl for
//  create table
//  alter table
//  create materialized view
//  alter materialized view
#[derive(Debug, PartialEq)]
pub struct TableOptions {}

#[derive(Debug, PartialEq)]
pub struct ClusteringOrderByColumn {
    column_name: TokenView,
    order: Option<ClusteringOrder>,
}

#[derive(Debug, PartialEq)]
pub enum ClusteringOrder {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq)]
pub struct ColumnDefinitions {
    pub view: TokenView,
    pub definitions: Vec<ColumnDefinition>,
    pub primary_key: Option<Vec<TokenView>>,
}

#[derive(Debug, PartialEq)]
pub struct ColumnDefinition {
    pub view: TokenView,
    pub column_name: TokenView,
    pub type_definition: TokenView,
    pub attribute: Option<ColumnDefinitionAttribute>,
}

#[derive(Debug, PartialEq)]
pub struct ColumnDefinitionAttribute {
    pub view: TokenView,
    pub kind: ColumnDefinitionAttributeKind,
}

#[derive(Debug, PartialEq)]
pub enum ColumnDefinitionAttributeKind {
    Static,
    PrimaryKey,
}

pub struct TablePrimaryKeyDefinition {
    pub view: TokenView,
    pub column_names: Vec<TokenView>,
}

#[derive(Debug, PartialEq)]
pub enum DropStatement {
    Aggregate(DropAggregateStatement),
    Function(DropFunctionStatement),
    Index(DropIndexStatement),
    Keyspace(DropKeyspaceStatement),
    MaterializedView(DropMaterializedViewStatement),
    Table(DropTableStatement),
    Trigger(DropTriggerStatement),
    Type(DropTypeStatement),
}

#[derive(Debug, PartialEq)]
pub struct DropAggregateStatement {
    pub aggregate_name: TokenView,
    pub if_exists: bool,
    pub keyspace_name: Option<TokenView>,
}

#[derive(Debug, PartialEq)]
pub struct DropFunctionStatement {
    pub function_name: TokenView,
    pub if_exists: bool,
    pub keyspace_name: Option<TokenView>,
}

#[derive(Debug, PartialEq)]
pub struct DropIndexStatement {
    pub index_name: TokenView,
    pub if_exists: bool,
    pub keyspace_name: Option<TokenView>,
}

#[derive(Debug, PartialEq)]
pub struct DropKeyspaceStatement {
    pub keyspace_name: TokenView,
    pub if_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct DropMaterializedViewStatement {
    pub view_name: TokenView,
    pub if_exists: bool,
    pub keyspace_name: Option<TokenView>,
}

#[derive(Debug, PartialEq)]
pub struct DropTableStatement {
    pub table_name: TokenView,
    pub alias: Option<TableAlias>,
    pub if_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct DropTriggerStatement {
    pub trigger_name: TokenView,
    pub if_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct DropTypeStatement {
    pub type_name: TokenView,
    pub if_exists: bool,
}
