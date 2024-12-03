use crate::cql::ast::table::TableAlias;
use crate::cql::ast::TokenView;

#[derive(Debug, PartialEq)]
pub enum DropStatement {
    Aggregate(DropAggregateStatement),
    Function(DropFunctionStatement),
    Index(DropIndexStatement),
    Keyspace(DropKeyspaceStatement),
    MaterializedView(DropMaterializedViewStatement),
    Role(DropRoleStatement),
    Table(DropTableStatement),
    Trigger(DropTriggerStatement),
    Type(DropTypeStatement),
    User(DropUserStatement),
}

#[derive(Debug, PartialEq)]
pub struct DropAggregateStatement {
    pub aggregate_name: TokenView,
    pub if_exists: bool,
    pub keyspace_name: Option<TokenView>,
    pub signature: Option<Vec<TokenView>>,
}

#[derive(Debug, PartialEq)]
pub struct DropFunctionStatement {
    pub function_name: TokenView,
    pub if_exists: bool,
    pub keyspace_name: Option<TokenView>,
    pub signature: Option<Vec<TokenView>>,
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
pub struct DropRoleStatement {
    pub role_name: TokenView,
    pub if_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct DropTableStatement {
    pub table_name: TokenView,
    pub alias: Option<TableAlias>,
    pub if_exists: bool,
    pub keyspace_name: Option<TokenView>,
}

#[derive(Debug, PartialEq)]
pub struct DropTriggerStatement {
    pub table_name: TokenView,
    pub trigger_name: TokenView,
    pub if_exists: bool,
    pub keyspace_name: Option<TokenView>,
}

#[derive(Debug, PartialEq)]
pub struct DropTypeStatement {
    pub type_name: TokenView,
    pub if_exists: bool,
    pub keyspace_name: Option<TokenView>,
}

#[derive(Debug, PartialEq)]
pub struct DropUserStatement {
    pub user_name: TokenView,
    pub if_exists: bool,
}
