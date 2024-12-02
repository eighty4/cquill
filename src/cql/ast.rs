use crate::cql::lex::TokenRange;
use std::collections::HashMap;
use std::sync::Arc;

pub struct TokenView {
    pub cql: Arc<String>,
    pub range: TokenRange,
}

/// Declares alias for TABLE keyword
pub enum TableAlias {
    ColumnFamily,
}

pub enum CqlStatement {
    Create(CreateStatement),
    Drop(DropStatement),
    Select,
    Insert,
    Update,
    Delete,
}

// todo CREATE MATERIALIZED VIEW
pub enum CreateStatement {
    Index(CreateIndexStatement),
    Keyspace(CreateKeyspaceStatement),
    Role(CreateRoleStatement),
    Table(CreateTableStatement),
}

pub struct CreateIndexStatement {
    index_name: TokenView,
    if_not_exists: bool
}

pub struct CreateKeyspaceStatement {
    keyspace_name: TokenView,
    if_not_exists: bool,
    replication: KeyspaceReplication,
    durable_writes: Option<bool>,
}

pub struct KeyspaceReplication {
    map_view: TokenView,
    strategy: KeyspaceReplicationStrategy,
}

pub enum KeyspaceReplicationStrategy {
    NetworkTopology(HashMap<String, i8>),
    Simple(i8),
}

// todo WITH statements
pub struct CreateRoleStatement {
    if_not_exists: bool,
    role_name: TokenView,
}

// todo table_options
pub struct CreateTableStatement {
    pub keyspace_name: Option<TokenView>,
    pub table_name: TokenView,
    pub column_definitions: ColumnDefinitions,
    pub if_not_exists: bool,
    pub table_alias: Option<TableAlias>,
    pub attributes: Vec<CreateTableAttribute>,
}

pub struct CreateTableAttribute {
    view: TokenView,
    kind: CreateTableAttributeKind,
}

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
pub struct TableOptions {
    
}

pub struct ClusteringOrderByColumn {
    column_name: TokenView,
    order: Option<ClusteringOrder>,
}

pub enum ClusteringOrder {
    Asc,
    Desc,
}

pub struct ColumnDefinitions {
    pub view: TokenView,
    pub definitions: Vec<ColumnDefinition>,
    pub primary_key: Option<Vec<TokenView>>,
}

pub struct ColumnDefinition {
    pub view: TokenView,
    pub column_name: TokenView,
    pub type_definition: TokenView,
    pub attribute: Option<ColumnDefinitionAttribute> 
}

pub struct ColumnDefinitionAttribute {
    pub view: TokenView,
    pub kind: ColumnDefinitionAttributeKind,
}

pub enum ColumnDefinitionAttributeKind {
    Static,
    PrimaryKey
}

pub struct TablePrimaryKeyDefinition {
    pub view: TokenView,
    pub column_names: Vec<TokenView>
}

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

pub struct DropAggregateStatement {
    pub aggregate_name: TokenView,
    pub if_exists: bool,
}

pub struct DropFunctionStatement {
    pub function_name: TokenView,
    pub if_exists: bool,
}

pub struct DropIndexStatement {
    pub index_name: TokenView,
    pub if_exists: bool,
}

pub struct DropKeyspaceStatement {
    pub keyspace_name: TokenView,
    pub if_exists: bool,
}

pub struct DropMaterializedViewStatement {
    pub view_name: TokenView,
    pub if_exists: bool,
}

pub struct DropTableStatement {
    pub table_name: TokenView,
    pub alias: Option<TableAlias>,
    pub if_exists: bool,
}

pub struct DropTriggerStatement {
    pub trigger_name: TokenView,
    pub if_exists: bool,
}

pub struct DropTypeStatement {
    pub type_name: TokenView,
    pub if_exists: bool,
}
