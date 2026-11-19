pub use create_trigger::*;
pub use drop_trigger::*;

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/triggers.html#create-trigger-statement
mod create_trigger {
    use crate::ast::{StringView, TokenView};

    #[derive(Debug, PartialEq)]
    pub struct CreateTriggerStatement {
        pub if_not_exists: bool,
        pub trigger_name: TokenView,
        pub table_name: TokenView,
        pub keyspace_name: Option<TokenView>,
        pub index_classpath: StringView,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/triggers.html#drop-trigger-statement
mod drop_trigger {
    use crate::ast::TokenView;

    #[derive(Debug, PartialEq)]
    pub struct DropTriggerStatement {
        pub table_name: TokenView,
        pub trigger_name: TokenView,
        pub if_exists: bool,
        pub keyspace_name: Option<TokenView>,
    }
}
