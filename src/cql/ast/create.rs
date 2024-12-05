use crate::cql::ast::table::TableAlias;
use crate::cql::ast::{CqlDataType, StringView, TokenView};
use std::collections::HashMap;

// todo create custom index
#[derive(Debug, PartialEq)]
pub enum CreateStatement {
    Aggregate(CreateAggregateStatement),
    Function(CreateFunctionStatement),
    Index(CreateIndexStatement),
    Keyspace(CreateKeyspaceStatement),
    // todo
    MaterializedView(CreateMaterializedViewStatement),
    Role(CreateRoleStatement),
    // todo
    Table(CreateTableStatement),
    // todo
    Trigger(CreateTriggerStatement),
    Type(CreateTypeStatement),
    User(CreateUserStatement),
}

// todo represent init_condition with an AST node
#[derive(Debug, PartialEq)]
pub struct CreateAggregateStatement {
    pub if_exists_behavior: CreateIfExistsBehavior,
    pub function_name: TokenView,
    pub function_arg: CqlDataType,
    pub state_function: TokenView,
    pub state_type: CqlDataType,
    pub final_function: Option<TokenView>,
    pub init_condition: bool,
}

#[derive(Debug, PartialEq)]
pub struct CreateFunctionStatement {
    pub if_exists_behavior: CreateIfExistsBehavior,
    pub function_name: TokenView,
    pub function_args: Vec<(TokenView, CqlDataType)>,
    pub on_null_input: OnNullInput,
    pub returns: CqlDataType,
    pub language: TokenView,
    pub function_body: StringView,
}

#[derive(Debug, PartialEq)]
pub enum CreateIfExistsBehavior {
    /// Specifies `if not exists`
    DoNotError,
    /// Does not specify `or replace` or `if not exists`
    Error,
    /// Specifies `or replace`
    Replace,
}

#[derive(Debug, PartialEq)]
pub enum OnNullInput {
    Called,
    ReturnsNull,
}

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
    pub if_not_exists: bool,
    pub keyspace_name: TokenView,
    pub replication: KeyspaceReplication,
    pub durable_writes: Option<bool>,
}

#[derive(Debug, PartialEq)]
pub enum KeyspaceReplication {
    NetworkTopology(HashMap<String, i8>),
    Simple(i8),
}

#[derive(Debug, PartialEq)]
pub struct CreateMaterializedViewStatement {
    if_not_exists: bool,
    role_name: TokenView,
}

#[derive(Debug, PartialEq)]
pub struct CreateRoleStatement {
    pub if_not_exists: bool,
    pub role_name: TokenView,
    pub attributes: Option<Vec<RoleConfigAttribute>>,
}

#[derive(Debug, PartialEq)]
pub enum RoleConfigAttribute {
    Superuser(bool),
    Login(bool),
    Password(AuthPassword),
    // todo proper map literal type
    Options(HashMap<StringView, TokenView>),
    Access(Datacenters),
}

#[derive(Debug, PartialEq)]
pub enum AuthPassword {
    Hashed(StringView),
    PlainText(StringView),
}

#[derive(Debug, PartialEq)]
pub enum Datacenters {
    All,
    Explicit(Vec<StringView>),
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
pub struct CreateTriggerStatement {
    pub if_not_exists: bool,
    pub trigger_name: TokenView,
    pub table_name: TokenView,
    pub keyspace_name: Option<TokenView>,
    pub index_classpath: StringView,
}

#[derive(Debug, PartialEq)]
pub struct CreateTypeStatement {
    pub type_name: TokenView,
    pub if_not_exists: bool,
    pub keyspace_name: Option<TokenView>,
    pub fields: Vec<(TokenView, CqlDataType)>,
}

#[derive(Debug, PartialEq)]
pub struct CreateUserStatement {
    pub user_name: TokenView,
    pub if_not_exists: bool,
    pub password: Option<AuthPassword>,
    pub user_status: Option<CreateUserStatus>,
}

#[derive(Debug, PartialEq)]
pub enum CreateUserStatus {
    NoSuperuser,
    Superuser,
}
