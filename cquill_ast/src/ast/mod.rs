mod ddl;
mod dml;
mod functions;
mod indexing;
mod materialized_views;
mod security;
mod table;
mod token;
mod triggers;
mod udt;

#[cfg(test)]
mod token_test;

pub use ddl::*;
pub use dml::*;
pub use functions::*;
pub use indexing::*;
pub use materialized_views::*;
pub use security::*;
pub use table::*;
pub use token::*;
pub use triggers::*;
pub use udt::*;

#[derive(Debug, PartialEq)]
pub enum CqlStatement {
    Alter(AlterStatement),
    Batch(BatchStatement),
    Create(CreateStatement),
    Delete(DeleteStatement),
    Drop(DropStatement),
    Grant(GrantStatement),
    Insert(InsertStatement),
    List(ListStatement),
    Revoke(RevokeStatement),
    Select(SelectStatement),
    Update(UpdateStatement),
    Use(UseKeyspaceStatement),
}

#[derive(Debug, PartialEq)]
pub enum AlterStatement {
    AlterKeyspace,
    AlterMaterializedView,
    AlterRole,
    AlterTable,
    AlterType,
    AlterUser,
}

#[derive(Debug, PartialEq)]
pub enum CreateStatement {
    Aggregate(CreateAggregateStatement),
    CustomIndex(CreateCustomIndexStatement),
    Function(CreateFunctionStatement),
    Index(CreateIndexStatement),
    Keyspace(CreateKeyspaceStatement),
    MaterializedView(CreateMaterializedViewStatement),
    Role(CreateRoleStatement),
    Table(CreateTableStatement),
    Trigger(CreateTriggerStatement),
    Type(CreateTypeStatement),
    User(CreateUserStatement),
}

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
pub enum GrantStatement {
    Permission,
    Role,
}

#[derive(Debug, PartialEq)]
pub enum ListStatement {
    Permissions,
    Roles,
    Users,
}

#[derive(Debug, PartialEq)]
pub enum RevokeStatement {
    Permission,
    Role,
}
