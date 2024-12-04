use crate::cql::ast::table::TableAlias;
use crate::cql::ast::{StringView, TokenView};
use std::collections::HashMap;

// todo create custom index
#[derive(Debug, PartialEq)]
pub enum CreateStatement {
    // todo
    Aggregate(CreateAggregateStatement),
    // todo
    Function(CreateFunctionStatement),
    Index(CreateIndexStatement),
    // todo
    Keyspace(CreateKeyspaceStatement),
    // todo
    MaterializedView(CreateMaterializedViewStatement),
    // todo
    Role(CreateRoleStatement),
    // todo
    Table(CreateTableStatement),
    // todo
    Trigger(CreateTriggerStatement),
    Type(CreateTypeStatement),
    User(CreateUserStatement),
}

#[derive(Debug, PartialEq)]
pub struct CreateAggregateStatement {}

#[derive(Debug, PartialEq)]
pub struct CreateFunctionStatement {}

#[derive(Debug, PartialEq)]
pub struct CreateIndexStatement {
    pub index_name: Option<TokenView>,
    pub table_name: TokenView,
    pub keyspace_name: Option<TokenView>,
    pub if_not_exists: bool,
    pub on_column: CreateIndexColumn,
}

#[derive(Debug, PartialEq)]
pub enum CreateIndexColumn {
    /// For an index on a scalar data type, set or list.
    Column(TokenView),
    /// For a full index on a frozen collection with `FULL(collection_col)`.
    FullCollection(TokenView),
    /// For an index on map entries with `ENTRIES(map_col)`.
    MapEntries(TokenView),
    /// Alias for `ENTRIES` with `VALUES(map_col)`.
    MapValues(TokenView),
    /// For an index on map keys with `KEYS(map_col)`.
    MapKeys(TokenView),
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
pub struct CreateMaterializedViewStatement {
    if_not_exists: bool,
    role_name: TokenView,
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
pub struct CreateTriggerStatement {}

#[derive(Debug, PartialEq)]
pub struct CreateTypeStatement {
    pub type_name: TokenView,
    pub if_not_exists: bool,
    pub keyspace_name: Option<TokenView>,
    pub fields: HashMap<TokenView, TokenView>,
}

#[derive(Debug, PartialEq)]
pub struct CreateUserStatement {
    pub user_name: TokenView,
    pub if_not_exists: bool,
    pub password: Option<CreateUserPassword>,
    pub user_status: Option<CreateUserStatus>,
}

#[derive(Debug, PartialEq)]
pub enum CreateUserPassword {
    Hashed(StringView),
    PlainText(StringView),
}

#[derive(Debug, PartialEq)]
pub enum CreateUserStatus {
    NoSuperuser,
    Superuser,
}
