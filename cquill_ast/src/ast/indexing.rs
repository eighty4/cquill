pub use create_custom_index::*;
pub use create_index::*;
pub use drop_index::*;

/// https://cassandra.apache.org/doc/5.0.9/cassandra/reference/cql-commands/create-custom-index.html
mod create_custom_index {
    #[derive(Debug, PartialEq)]
    pub struct CreateCustomIndexStatement {}
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/reference/cql-commands/create-index.html
mod create_index {
    use crate::ast::TokenView;

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
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/reference/cql-commands/drop-index.html
mod drop_index {
    use crate::ast::TokenView;

    #[derive(Debug, PartialEq)]
    pub struct DropIndexStatement {
        pub index_name: TokenView,
        pub if_exists: bool,
        pub keyspace_name: Option<TokenView>,
    }
}
