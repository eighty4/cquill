pub use create_type::*;
// pub use alter_type::*;
pub use drop_type::*;

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/types.html#creating-a-udt
mod create_type {
    use crate::ast::{CqlDataType, TokenView};

    #[derive(Debug, PartialEq)]
    pub struct CreateTypeStatement {
        pub type_name: TokenView,
        pub if_not_exists: bool,
        pub keyspace_name: Option<TokenView>,
        pub fields: Vec<(TokenView, CqlDataType)>,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/types.html#altering-a-udt
mod alter_type {}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/types.html#dropping-a-udt
mod drop_type {
    use crate::ast::TokenView;

    #[derive(Debug, PartialEq)]
    pub struct DropTypeStatement {
        pub type_name: TokenView,
        pub if_exists: bool,
        pub keyspace_name: Option<TokenView>,
    }
}
