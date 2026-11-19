// pub use alter_materialized_view::*;
pub use create_materialized_view::*;
pub use drop_materialized_view::*;

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/mvs.html#create-materialized-view-statement
mod create_materialized_view {
    use crate::ast::TokenView;

    #[derive(Debug, PartialEq)]
    pub struct CreateMaterializedViewStatement {
        if_not_exists: bool,
        role_name: TokenView,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/mvs.html#alter-materialized-view-statement
mod alter_materialized_view {}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/mvs.html#drop-materialized-view-statement
mod drop_materialized_view {
    use crate::ast::TokenView;

    #[derive(Debug, PartialEq)]
    pub struct DropMaterializedViewStatement {
        pub view_name: TokenView,
        pub if_exists: bool,
        pub keyspace_name: Option<TokenView>,
    }
}
