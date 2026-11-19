// pub use alter_keyspace::*;
pub use use_keyspace::*;
// pub use alter_table::*;
pub use create_keyspace::*;
pub use create_table::*;
pub use drop_keyspace::*;
pub use drop_table::*;
// pub use truncate_table::*;

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#create-keyspace-statement
mod create_keyspace {
    use crate::ast::TokenView;
    use std::collections::HashMap;

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
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#use-statement
mod use_keyspace {
    #[derive(Debug, PartialEq)]
    pub struct UseKeyspaceStatement {}
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#alter-keyspace-statement
mod alter_keyspace {}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#drop-keyspace-statement
mod drop_keyspace {
    use crate::ast::TokenView;

    #[derive(Debug, PartialEq)]
    pub struct DropKeyspaceStatement {
        pub keyspace_name: TokenView,
        pub if_exists: bool,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#create-table-statement
mod create_table {
    use std::collections::HashMap;

    use crate::ast::{CqlDataType, StringView, TableAlias, TokenView};

    #[derive(Debug, PartialEq)]
    // todo table_options
    pub struct CreateTableStatement {
        pub keyspace_name: Option<TokenView>,
        pub table_name: TokenView,
        pub column_definitions: Vec<ColumnDefinition>,
        pub if_not_exists: bool,
        pub table_alias: Option<TableAlias>,
        pub attributes: Option<Vec<TableDefinitionAttribute>>,
    }

    #[derive(Debug, PartialEq)]
    pub enum TableDefinitionAttribute {
        ClusteringOrderBy(Vec<ClusteringOrderDefinition>),
        Comment(StringView),
        // todo proper map literal type
        Compaction(HashMap<StringView, TokenView>),
        CompactStorage,
        Id(String),
        TableOptions(TableOptions),
    }

    #[derive(Debug, PartialEq)]
    pub struct ClusteringOrderDefinition {
        pub column_name: TokenView,
        pub order: Option<ClusteringOrder>,
    }

    // todo impl for
    //  create table
    //  alter table
    //  create materialized view
    //  alter materialized view
    #[derive(Debug, PartialEq)]
    pub struct TableOptions {}

    #[derive(Debug, PartialEq)]
    pub enum ClusteringOrder {
        Asc,
        Desc,
    }

    /// Represents columns within a table definition.
    #[derive(Debug, PartialEq)]
    pub enum ColumnDefinition {
        /// Represents a column.
        Column {
            column_name: TokenView,
            data_type: CqlDataType,
            attribute: Option<ColumnDefinitionAttribute>,
        },
        /// Represents a standalone `PRIMARY KEY (column_name)` definition.
        // todo composite/clustering key
        PrimaryKey(PrimaryKeyDefinition),
    }

    #[derive(Debug, PartialEq)]
    pub enum PrimaryKeyDefinition {
        /// `PRIMARY KEY (partition_k)`
        Single(TokenView),
        /// `PRIMARY KEY (partition_k, clustering_k1, clustering_k2, ...)`
        Compound {
            partition: TokenView,
            clustering: Vec<TokenView>,
        },
        /// `PRIMARY KEY ((partition_k1, partition_k2), clustering_k1, clustering_k2, ...)`
        CompositePartition {
            partition: Vec<TokenView>,
            clustering: Vec<TokenView>,
        },
    }

    #[derive(Debug, PartialEq)]
    pub enum ColumnDefinitionAttribute {
        Static,
        PrimaryKey,
    }

    pub struct TablePrimaryKeyDefinition {
        pub view: TokenView,
        pub column_names: Vec<TokenView>,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#alter-table-statement
mod alter_table {}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#drop-table-statement
mod drop_table {
    use crate::ast::{TableAlias, TokenView};

    #[derive(Debug, PartialEq)]
    pub struct DropTableStatement {
        pub table_name: TokenView,
        pub alias: Option<TableAlias>,
        pub if_exists: bool,
        pub keyspace_name: Option<TokenView>,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/ddl.html#truncate-statement
mod truncate_table {}
